//! Durable jobs and scheduler state.
//!
//! Voodoo Store decides *what* is ready to execute and persists delivery,
//! retries, deadlines, and history. It never executes application code.

use thiserror::Error;

use crate::{EngineError, Store};

const JOB_PREFIX: &[u8] = b"\xffvds:job:data:";
const HISTORY_PREFIX: &[u8] = b"\xffvds:job:history:";
const SCHEDULE_PREFIX: &[u8] = b"\xffvds:schedule:data:";
const VERSION: u8 = 1;

pub type JobId = [u8; 16];
pub type ScheduleId = [u8; 16];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DurableJobState {
    Ready = 0,
    Leased = 1,
    Completed = 2,
    Dead = 3,
    Cancelled = 4,
}

impl TryFrom<u8> for DurableJobState {
    type Error = JobError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Ready),
            1 => Ok(Self::Leased),
            2 => Ok(Self::Completed),
            3 => Ok(Self::Dead),
            4 => Ok(Self::Cancelled),
            _ => Err(JobError::CorruptRecord),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobSpec {
    pub handler: Vec<u8>,
    pub payload: Vec<u8>,
    pub available_at_ms: i64,
    pub deadline_ms: Option<i64>,
    pub priority: i32,
    pub max_attempts: u32,
    pub retry_backoff_ms: u64,
    pub idempotency_key: Option<Vec<u8>>,
}

impl JobSpec {
    pub fn new(handler: impl Into<Vec<u8>>, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            handler: handler.into(),
            payload: payload.into(),
            available_at_ms: 0,
            deadline_ms: None,
            priority: 0,
            max_attempts: 3,
            retry_backoff_ms: 1_000,
            idempotency_key: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableJob {
    pub id: JobId,
    pub state: DurableJobState,
    pub handler: Vec<u8>,
    pub payload: Vec<u8>,
    pub available_at_ms: i64,
    pub deadline_ms: Option<i64>,
    pub priority: i32,
    pub attempts: u32,
    pub max_attempts: u32,
    pub retry_backoff_ms: u64,
    pub lease_until_ms: i64,
    pub lease_generation: u32,
    pub idempotency_key: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobHistoryKind {
    Submitted = 0,
    Claimed = 1,
    Completed = 2,
    RetryScheduled = 3,
    Dead = 4,
    Cancelled = 5,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobHistoryEntry {
    pub sequence: u64,
    pub at_ms: i64,
    pub kind: JobHistoryKind,
    pub detail: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleMode {
    Once,
    Interval { every_ms: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableSchedule {
    pub id: ScheduleId,
    pub job: JobSpec,
    pub mode: ScheduleMode,
    pub next_run_ms: i64,
    pub enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerTickReport {
    pub scanned: usize,
    pub fired: usize,
}

impl Store {
    pub fn submit_job(&mut self, spec: JobSpec, now_ms: i64) -> Result<JobId, JobError> {
        validate_job_spec(&spec)?;
        if let Some(key) = spec.idempotency_key.as_deref() {
            if let Some(existing) = self.find_job_by_idempotency(key)? {
                return Ok(existing);
            }
        }

        let job = build_job(spec)?;
        let id = job.id;
        let mut tx = self.begin()?;
        tx.put_internal(job_key(&id), encode_job(&job)?)?;
        append_history_tx(
            &mut tx,
            &id,
            0,
            JobHistoryEntry {
                sequence: 0,
                at_ms: now_ms,
                kind: JobHistoryKind::Submitted,
                detail: Vec::new(),
            },
        )?;
        tx.commit()?;
        Ok(id)
    }

    pub fn get_job(&self, id: &JobId) -> Result<Option<DurableJob>, JobError> {
        self.get(job_key(id)).map(decode_job).transpose()
    }

    pub fn claim_job(
        &mut self,
        now_ms: i64,
        lease_duration_ms: u64,
    ) -> Result<Option<DurableJob>, JobError> {
        let lease_duration =
            i64::try_from(lease_duration_ms).map_err(|_| JobError::TimeOverflow)?;
        let lease_until_ms = now_ms
            .checked_add(lease_duration)
            .ok_or(JobError::TimeOverflow)?;
        let mut candidate: Option<DurableJob> = None;
        let mut expired = Vec::new();

        for (_, encoded) in self.scan_prefix(JOB_PREFIX) {
            let job = decode_job(&encoded)?;
            let reclaimable = job.state == DurableJobState::Ready
                || (job.state == DurableJobState::Leased && job.lease_until_ms <= now_ms);
            if !reclaimable {
                continue;
            }
            if job.deadline_ms.is_some_and(|deadline| deadline <= now_ms) {
                expired.push(job);
                continue;
            }
            if job.state == DurableJobState::Ready && job.available_at_ms > now_ms {
                continue;
            }
            let replace = candidate.as_ref().is_none_or(|current| {
                job.priority > current.priority
                    || (job.priority == current.priority
                        && job.available_at_ms < current.available_at_ms)
            });
            if replace {
                candidate = Some(job);
            }
        }

        for mut job in expired {
            job.state = DurableJobState::Dead;
            job.lease_until_ms = 0;
            self.persist_job_with_history(
                &job,
                now_ms,
                JobHistoryKind::Dead,
                b"deadline exceeded",
            )?;
        }

        let Some(mut job) = candidate else {
            return Ok(None);
        };
        job.attempts = job
            .attempts
            .checked_add(1)
            .ok_or(JobError::AttemptExhausted)?;
        if job.attempts > job.max_attempts {
            job.state = DurableJobState::Dead;
            job.lease_until_ms = 0;
            self.persist_job_with_history(
                &job,
                now_ms,
                JobHistoryKind::Dead,
                b"max attempts exceeded",
            )?;
            return Ok(None);
        }
        job.state = DurableJobState::Leased;
        job.lease_until_ms = lease_until_ms;
        job.lease_generation = job.attempts;
        self.persist_job_with_history(&job, now_ms, JobHistoryKind::Claimed, &[])?;
        Ok(Some(job))
    }

    pub fn complete_job(
        &mut self,
        id: &JobId,
        lease_generation: u32,
        now_ms: i64,
    ) -> Result<(), JobError> {
        let mut job = self.get_job(id)?.ok_or(JobError::NotFound)?;
        validate_job_lease(&job, lease_generation)?;
        job.state = DurableJobState::Completed;
        job.lease_until_ms = 0;
        self.persist_job_with_history(&job, now_ms, JobHistoryKind::Completed, &[])
    }

    pub fn fail_job(
        &mut self,
        id: &JobId,
        lease_generation: u32,
        now_ms: i64,
        detail: impl AsRef<[u8]>,
    ) -> Result<DurableJobState, JobError> {
        let mut job = self.get_job(id)?.ok_or(JobError::NotFound)?;
        validate_job_lease(&job, lease_generation)?;
        job.lease_until_ms = 0;
        let kind = if job.attempts >= job.max_attempts {
            job.state = DurableJobState::Dead;
            JobHistoryKind::Dead
        } else {
            let multiplier = u64::from(job.attempts.max(1));
            let delay = job
                .retry_backoff_ms
                .checked_mul(multiplier)
                .ok_or(JobError::TimeOverflow)?;
            let delay = i64::try_from(delay).map_err(|_| JobError::TimeOverflow)?;
            job.available_at_ms = now_ms.checked_add(delay).ok_or(JobError::TimeOverflow)?;
            job.state = DurableJobState::Ready;
            JobHistoryKind::RetryScheduled
        };
        self.persist_job_with_history(&job, now_ms, kind, detail.as_ref())?;
        Ok(job.state)
    }

    pub fn cancel_job(&mut self, id: &JobId, now_ms: i64) -> Result<bool, JobError> {
        let Some(mut job) = self.get_job(id)? else {
            return Ok(false);
        };
        if matches!(
            job.state,
            DurableJobState::Completed | DurableJobState::Dead | DurableJobState::Cancelled
        ) {
            return Ok(false);
        }
        job.state = DurableJobState::Cancelled;
        job.lease_until_ms = 0;
        self.persist_job_with_history(&job, now_ms, JobHistoryKind::Cancelled, &[])?;
        Ok(true)
    }

    pub fn job_history(&self, id: &JobId) -> Result<Vec<JobHistoryEntry>, JobError> {
        let prefix = history_prefix(id);
        let mut entries = Vec::new();
        for (_, encoded) in self.scan_prefix(prefix) {
            entries.push(decode_history(&encoded)?);
        }
        entries.sort_unstable_by_key(|entry| entry.sequence);
        Ok(entries)
    }

    pub fn create_schedule(
        &mut self,
        job: JobSpec,
        mode: ScheduleMode,
        first_run_ms: i64,
    ) -> Result<ScheduleId, JobError> {
        validate_job_spec(&job)?;
        if matches!(mode, ScheduleMode::Interval { every_ms: 0 }) {
            return Err(JobError::InvalidSchedule);
        }
        let id = random_id()?;
        let schedule = DurableSchedule {
            id,
            job,
            mode,
            next_run_ms: first_run_ms,
            enabled: true,
        };
        self.put_internal(schedule_key(&id), encode_schedule(&schedule)?)?;
        Ok(id)
    }

    pub fn get_schedule(&self, id: &ScheduleId) -> Result<Option<DurableSchedule>, JobError> {
        self.get(schedule_key(id)).map(decode_schedule).transpose()
    }

    pub fn set_schedule_enabled(
        &mut self,
        id: &ScheduleId,
        enabled: bool,
    ) -> Result<bool, JobError> {
        let Some(mut schedule) = self.get_schedule(id)? else {
            return Ok(false);
        };
        schedule.enabled = enabled;
        self.put_internal(schedule_key(id), encode_schedule(&schedule)?)?;
        Ok(true)
    }

    pub fn tick_schedules(
        &mut self,
        now_ms: i64,
        limit: usize,
    ) -> Result<SchedulerTickReport, JobError> {
        let schedules = self.scan_prefix(SCHEDULE_PREFIX);
        let scanned = schedules.len();
        let mut fired = 0usize;
        for (_, encoded) in schedules {
            if fired == limit {
                break;
            }
            let mut schedule = decode_schedule(&encoded)?;
            if !schedule.enabled || schedule.next_run_ms > now_ms {
                continue;
            }

            let scheduled_at = schedule.next_run_ms;
            let mut spec = schedule.job.clone();
            spec.available_at_ms = scheduled_at;
            if let Some(key) = spec.idempotency_key.as_mut() {
                key.extend_from_slice(&scheduled_at.to_be_bytes());
            }
            let job = build_job(spec)?;

            match schedule.mode {
                ScheduleMode::Once => schedule.enabled = false,
                ScheduleMode::Interval { every_ms } => {
                    let every = i64::try_from(every_ms).map_err(|_| JobError::TimeOverflow)?;
                    let mut next = schedule.next_run_ms;
                    while next <= now_ms {
                        next = next.checked_add(every).ok_or(JobError::TimeOverflow)?;
                    }
                    schedule.next_run_ms = next;
                }
            }

            let mut tx = self.begin()?;
            tx.put_internal(job_key(&job.id), encode_job(&job)?)?;
            append_history_tx(
                &mut tx,
                &job.id,
                0,
                JobHistoryEntry {
                    sequence: 0,
                    at_ms: now_ms,
                    kind: JobHistoryKind::Submitted,
                    detail: b"scheduled".to_vec(),
                },
            )?;
            tx.put_internal(schedule_key(&schedule.id), encode_schedule(&schedule)?)?;
            tx.commit()?;
            fired += 1;
        }
        Ok(SchedulerTickReport { scanned, fired })
    }

    fn persist_job_with_history(
        &mut self,
        job: &DurableJob,
        at_ms: i64,
        kind: JobHistoryKind,
        detail: &[u8],
    ) -> Result<(), JobError> {
        let sequence = self.next_history_sequence(&job.id)?;
        let entry = JobHistoryEntry {
            sequence,
            at_ms,
            kind,
            detail: detail.to_vec(),
        };
        let mut tx = self.begin()?;
        tx.put_internal(job_key(&job.id), encode_job(job)?)?;
        append_history_tx(&mut tx, &job.id, sequence, entry)?;
        tx.commit()?;
        Ok(())
    }

    fn next_history_sequence(&self, id: &JobId) -> Result<u64, JobError> {
        let entries = self.scan_prefix(history_prefix(id));
        match entries.last() {
            None => Ok(0),
            Some((key, _)) => decode_history_sequence(id, key)?
                .checked_add(1)
                .ok_or(JobError::HistoryExhausted),
        }
    }

    fn find_job_by_idempotency(&self, key: &[u8]) -> Result<Option<JobId>, JobError> {
        for (_, encoded) in self.scan_prefix(JOB_PREFIX) {
            let job = decode_job(&encoded)?;
            if job.idempotency_key.as_deref() == Some(key) {
                return Ok(Some(job.id));
            }
        }
        Ok(None)
    }
}

pub(crate) fn enqueue_job_tx(
    tx: &mut crate::Transaction<'_>,
    spec: JobSpec,
    now_ms: i64,
    detail: &[u8],
) -> Result<JobId, JobError> {
    validate_job_spec(&spec)?;
    if let Some(key) = spec.idempotency_key.as_deref() {
        for (_, encoded) in tx.scan_prefix_internal(JOB_PREFIX) {
            let job = decode_job(&encoded)?;
            if job.idempotency_key.as_deref() == Some(key) {
                return Ok(job.id);
            }
        }
    }

    let job = build_job(spec)?;
    let id = job.id;
    tx.put_internal(job_key(&id), encode_job(&job)?)?;
    append_history_tx(
        tx,
        &id,
        0,
        JobHistoryEntry {
            sequence: 0,
            at_ms: now_ms,
            kind: JobHistoryKind::Submitted,
            detail: detail.to_vec(),
        },
    )?;
    Ok(id)
}

fn validate_job_spec(spec: &JobSpec) -> Result<(), JobError> {
    if spec.handler.is_empty() {
        return Err(JobError::EmptyHandler);
    }
    if spec.max_attempts == 0 {
        return Err(JobError::InvalidMaxAttempts);
    }
    Ok(())
}

fn validate_job_lease(job: &DurableJob, generation: u32) -> Result<(), JobError> {
    if job.state != DurableJobState::Leased {
        return Err(JobError::NotLeased);
    }
    if job.lease_generation != generation {
        return Err(JobError::LeaseMismatch {
            expected: job.lease_generation,
            provided: generation,
        });
    }
    Ok(())
}

fn build_job(spec: JobSpec) -> Result<DurableJob, JobError> {
    validate_job_spec(&spec)?;
    Ok(DurableJob {
        id: random_id()?,
        state: DurableJobState::Ready,
        handler: spec.handler,
        payload: spec.payload,
        available_at_ms: spec.available_at_ms,
        deadline_ms: spec.deadline_ms,
        priority: spec.priority,
        attempts: 0,
        max_attempts: spec.max_attempts,
        retry_backoff_ms: spec.retry_backoff_ms,
        lease_until_ms: 0,
        lease_generation: 0,
        idempotency_key: spec.idempotency_key,
    })
}

fn random_id() -> Result<[u8; 16], JobError> {
    let mut id = [0u8; 16];
    getrandom::fill(&mut id).map_err(|_| JobError::EntropyUnavailable)?;
    Ok(id)
}

fn job_key(id: &JobId) -> Vec<u8> {
    let mut key = Vec::with_capacity(JOB_PREFIX.len() + 16);
    key.extend_from_slice(JOB_PREFIX);
    key.extend_from_slice(id);
    key
}

fn history_prefix(id: &JobId) -> Vec<u8> {
    let mut key = Vec::with_capacity(HISTORY_PREFIX.len() + 16);
    key.extend_from_slice(HISTORY_PREFIX);
    key.extend_from_slice(id);
    key
}

fn history_key(id: &JobId, sequence: u64) -> Vec<u8> {
    let mut key = history_prefix(id);
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

fn decode_history_sequence(id: &JobId, key: &[u8]) -> Result<u64, JobError> {
    let prefix = history_prefix(id);
    if !key.starts_with(&prefix) || key.len() != prefix.len() + 8 {
        return Err(JobError::CorruptRecord);
    }
    Ok(u64::from_be_bytes(
        key[prefix.len()..]
            .try_into()
            .map_err(|_| JobError::CorruptRecord)?,
    ))
}

fn schedule_key(id: &ScheduleId) -> Vec<u8> {
    let mut key = Vec::with_capacity(SCHEDULE_PREFIX.len() + 16);
    key.extend_from_slice(SCHEDULE_PREFIX);
    key.extend_from_slice(id);
    key
}

fn append_len(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), JobError> {
    let len = u32::try_from(bytes.len()).map_err(|_| JobError::FieldTooLarge)?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn read_len<'a>(bytes: &'a [u8], cursor: &mut usize) -> Result<&'a [u8], JobError> {
    if *cursor + 4 > bytes.len() {
        return Err(JobError::CorruptRecord);
    }
    let len = u32::from_le_bytes(
        bytes[*cursor..*cursor + 4]
            .try_into()
            .map_err(|_| JobError::CorruptRecord)?,
    ) as usize;
    *cursor += 4;
    let end = cursor.checked_add(len).ok_or(JobError::CorruptRecord)?;
    if end > bytes.len() {
        return Err(JobError::CorruptRecord);
    }
    let value = &bytes[*cursor..end];
    *cursor = end;
    Ok(value)
}

fn encode_job(job: &DurableJob) -> Result<Vec<u8>, JobError> {
    let mut out = Vec::new();
    out.push(VERSION);
    out.push(job.state as u8);
    out.extend_from_slice(&job.id);
    out.extend_from_slice(&job.available_at_ms.to_le_bytes());
    out.extend_from_slice(&job.deadline_ms.unwrap_or(i64::MIN).to_le_bytes());
    out.extend_from_slice(&job.priority.to_le_bytes());
    out.extend_from_slice(&job.attempts.to_le_bytes());
    out.extend_from_slice(&job.max_attempts.to_le_bytes());
    out.extend_from_slice(&job.retry_backoff_ms.to_le_bytes());
    out.extend_from_slice(&job.lease_until_ms.to_le_bytes());
    out.extend_from_slice(&job.lease_generation.to_le_bytes());
    append_len(&mut out, &job.handler)?;
    append_len(&mut out, &job.payload)?;
    match &job.idempotency_key {
        Some(key) => {
            out.push(1);
            append_len(&mut out, key)?;
        }
        None => out.push(0),
    }
    Ok(out)
}

fn decode_job(bytes: &[u8]) -> Result<DurableJob, JobError> {
    const FIXED: usize = 1 + 1 + 16 + 8 + 8 + 4 + 4 + 4 + 8 + 8 + 4;
    if bytes.len() < FIXED || bytes[0] != VERSION {
        return Err(JobError::CorruptRecord);
    }
    let state = DurableJobState::try_from(bytes[1])?;
    let id: JobId = bytes[2..18]
        .try_into()
        .map_err(|_| JobError::CorruptRecord)?;
    let available_at_ms = i64::from_le_bytes(bytes[18..26].try_into().unwrap());
    let deadline_raw = i64::from_le_bytes(bytes[26..34].try_into().unwrap());
    let priority = i32::from_le_bytes(bytes[34..38].try_into().unwrap());
    let attempts = u32::from_le_bytes(bytes[38..42].try_into().unwrap());
    let max_attempts = u32::from_le_bytes(bytes[42..46].try_into().unwrap());
    let retry_backoff_ms = u64::from_le_bytes(bytes[46..54].try_into().unwrap());
    let lease_until_ms = i64::from_le_bytes(bytes[54..62].try_into().unwrap());
    let lease_generation = u32::from_le_bytes(bytes[62..66].try_into().unwrap());
    let mut cursor = FIXED;
    let handler = read_len(bytes, &mut cursor)?.to_vec();
    let payload = read_len(bytes, &mut cursor)?.to_vec();
    if cursor >= bytes.len() {
        return Err(JobError::CorruptRecord);
    }
    let idempotency_key = match bytes[cursor] {
        0 => {
            cursor += 1;
            None
        }
        1 => {
            cursor += 1;
            Some(read_len(bytes, &mut cursor)?.to_vec())
        }
        _ => return Err(JobError::CorruptRecord),
    };
    if cursor != bytes.len() {
        return Err(JobError::CorruptRecord);
    }
    Ok(DurableJob {
        id,
        state,
        handler,
        payload,
        available_at_ms,
        deadline_ms: (deadline_raw != i64::MIN).then_some(deadline_raw),
        priority,
        attempts,
        max_attempts,
        retry_backoff_ms,
        lease_until_ms,
        lease_generation,
        idempotency_key,
    })
}

fn encode_history(entry: &JobHistoryEntry) -> Result<Vec<u8>, JobError> {
    let mut out = Vec::new();
    out.push(VERSION);
    out.extend_from_slice(&entry.sequence.to_le_bytes());
    out.extend_from_slice(&entry.at_ms.to_le_bytes());
    out.push(entry.kind as u8);
    append_len(&mut out, &entry.detail)?;
    Ok(out)
}

fn decode_history(bytes: &[u8]) -> Result<JobHistoryEntry, JobError> {
    if bytes.len() < 18 || bytes[0] != VERSION {
        return Err(JobError::CorruptRecord);
    }
    let sequence = u64::from_le_bytes(bytes[1..9].try_into().unwrap());
    let at_ms = i64::from_le_bytes(bytes[9..17].try_into().unwrap());
    let kind = match bytes[17] {
        0 => JobHistoryKind::Submitted,
        1 => JobHistoryKind::Claimed,
        2 => JobHistoryKind::Completed,
        3 => JobHistoryKind::RetryScheduled,
        4 => JobHistoryKind::Dead,
        5 => JobHistoryKind::Cancelled,
        _ => return Err(JobError::CorruptRecord),
    };
    let mut cursor = 18;
    let detail = read_len(bytes, &mut cursor)?.to_vec();
    if cursor != bytes.len() {
        return Err(JobError::CorruptRecord);
    }
    Ok(JobHistoryEntry {
        sequence,
        at_ms,
        kind,
        detail,
    })
}

fn append_history_tx(
    tx: &mut crate::Transaction<'_>,
    id: &JobId,
    sequence: u64,
    entry: JobHistoryEntry,
) -> Result<(), JobError> {
    tx.put_internal(history_key(id, sequence), encode_history(&entry)?)?;
    Ok(())
}

fn encode_schedule(schedule: &DurableSchedule) -> Result<Vec<u8>, JobError> {
    let mut out = Vec::new();
    out.push(VERSION);
    out.extend_from_slice(&schedule.id);
    out.push(u8::from(schedule.enabled));
    out.extend_from_slice(&schedule.next_run_ms.to_le_bytes());
    match schedule.mode {
        ScheduleMode::Once => out.push(0),
        ScheduleMode::Interval { every_ms } => {
            out.push(1);
            out.extend_from_slice(&every_ms.to_le_bytes());
        }
    }
    let template = DurableJob {
        id: [0; 16],
        state: DurableJobState::Ready,
        handler: schedule.job.handler.clone(),
        payload: schedule.job.payload.clone(),
        available_at_ms: schedule.job.available_at_ms,
        deadline_ms: schedule.job.deadline_ms,
        priority: schedule.job.priority,
        attempts: 0,
        max_attempts: schedule.job.max_attempts,
        retry_backoff_ms: schedule.job.retry_backoff_ms,
        lease_until_ms: 0,
        lease_generation: 0,
        idempotency_key: schedule.job.idempotency_key.clone(),
    };
    append_len(&mut out, &encode_job(&template)?)?;
    Ok(out)
}

fn decode_schedule(bytes: &[u8]) -> Result<DurableSchedule, JobError> {
    if bytes.len() < 27 || bytes[0] != VERSION {
        return Err(JobError::CorruptRecord);
    }
    let id: ScheduleId = bytes[1..17]
        .try_into()
        .map_err(|_| JobError::CorruptRecord)?;
    let enabled = match bytes[17] {
        0 => false,
        1 => true,
        _ => return Err(JobError::CorruptRecord),
    };
    let next_run_ms = i64::from_le_bytes(bytes[18..26].try_into().unwrap());
    let mode_tag = bytes[26];
    let mut cursor = 27;
    let mode = match mode_tag {
        0 => ScheduleMode::Once,
        1 => {
            if cursor + 8 > bytes.len() {
                return Err(JobError::CorruptRecord);
            }
            let every_ms = u64::from_le_bytes(bytes[cursor..cursor + 8].try_into().unwrap());
            cursor += 8;
            if every_ms == 0 {
                return Err(JobError::CorruptRecord);
            }
            ScheduleMode::Interval { every_ms }
        }
        _ => return Err(JobError::CorruptRecord),
    };
    let encoded_job = read_len(bytes, &mut cursor)?;
    if cursor != bytes.len() {
        return Err(JobError::CorruptRecord);
    }
    let template = decode_job(encoded_job)?;
    Ok(DurableSchedule {
        id,
        job: JobSpec {
            handler: template.handler,
            payload: template.payload,
            available_at_ms: template.available_at_ms,
            deadline_ms: template.deadline_ms,
            priority: template.priority,
            max_attempts: template.max_attempts,
            retry_backoff_ms: template.retry_backoff_ms,
            idempotency_key: template.idempotency_key,
        },
        mode,
        next_run_ms,
        enabled,
    })
}

#[derive(Debug, Error)]
pub enum JobError {
    #[error("store error: {0}")]
    Store(#[from] EngineError),
    #[error("job handler cannot be empty")]
    EmptyHandler,
    #[error("max attempts must be at least one")]
    InvalidMaxAttempts,
    #[error("schedule is invalid")]
    InvalidSchedule,
    #[error("job field is too large")]
    FieldTooLarge,
    #[error("job record is corrupt or unsupported")]
    CorruptRecord,
    #[error("job was not found")]
    NotFound,
    #[error("job is not currently leased")]
    NotLeased,
    #[error("job lease generation mismatch: expected {expected}, provided {provided}")]
    LeaseMismatch { expected: u32, provided: u32 },
    #[error("job attempt counter exhausted")]
    AttemptExhausted,
    #[error("job history sequence exhausted")]
    HistoryExhausted,
    #[error("time value overflowed")]
    TimeOverflow,
    #[error("operating-system entropy is unavailable")]
    EntropyUnavailable,
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn temp_store_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("voodoo-store-job-{name}-{nonce}.vstore"))
    }

    #[test]
    fn job_claim_retry_completion_and_history_are_durable() {
        let path = temp_store_path("lifecycle");
        let id;
        {
            let mut store = Store::open(&path).unwrap();
            let mut spec = JobSpec::new(b"email.send".to_vec(), b"hello".to_vec());
            spec.max_attempts = 2;
            spec.retry_backoff_ms = 10;
            id = store.submit_job(spec, 0).unwrap();
            let first = store.claim_job(0, 100).unwrap().unwrap();
            assert_eq!(first.id, id);
            assert_eq!(
                store
                    .fail_job(&id, first.lease_generation, 5, b"temporary")
                    .unwrap(),
                DurableJobState::Ready
            );
            assert!(store.claim_job(14, 100).unwrap().is_none());
            let second = store.claim_job(15, 100).unwrap().unwrap();
            store
                .complete_job(&id, second.lease_generation, 16)
                .unwrap();
        }
        {
            let store = Store::open(&path).unwrap();
            assert_eq!(
                store.get_job(&id).unwrap().unwrap().state,
                DurableJobState::Completed
            );
            let history = store.job_history(&id).unwrap();
            assert_eq!(history.len(), 5);
            assert_eq!(history[0].kind, JobHistoryKind::Submitted);
            assert_eq!(history[4].kind, JobHistoryKind::Completed);
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn deadline_expiry_marks_job_dead_with_history() {
        let path = temp_store_path("deadline");
        let mut store = Store::open(&path).unwrap();
        let mut spec = JobSpec::new(b"expire".to_vec(), Vec::new());
        spec.deadline_ms = Some(10);
        let id = store.submit_job(spec, 0).unwrap();

        assert!(store.claim_job(10, 100).unwrap().is_none());
        let job = store.get_job(&id).unwrap().unwrap();
        assert_eq!(job.state, DurableJobState::Dead);
        let history = store.job_history(&id).unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[1].kind, JobHistoryKind::Dead);
        assert_eq!(history[1].detail, b"deadline exceeded");

        drop(store);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn idempotency_key_reuses_existing_job() {
        let path = temp_store_path("idempotency");
        let mut store = Store::open(&path).unwrap();
        let mut spec = JobSpec::new(b"invoice".to_vec(), b"x".to_vec());
        spec.idempotency_key = Some(b"invoice:42".to_vec());
        let first = store.submit_job(spec.clone(), 0).unwrap();
        let second = store.submit_job(spec, 1).unwrap();
        assert_eq!(first, second);
        drop(store);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn interval_schedule_fires_and_advances_durably() {
        let path = temp_store_path("schedule");
        let schedule_id;
        {
            let mut store = Store::open(&path).unwrap();
            schedule_id = store
                .create_schedule(
                    JobSpec::new(b"heartbeat".to_vec(), Vec::new()),
                    ScheduleMode::Interval { every_ms: 100 },
                    50,
                )
                .unwrap();
            assert_eq!(store.tick_schedules(49, 10).unwrap().fired, 0);
            assert_eq!(store.tick_schedules(250, 10).unwrap().fired, 1);
            assert_eq!(
                store
                    .get_schedule(&schedule_id)
                    .unwrap()
                    .unwrap()
                    .next_run_ms,
                350
            );
            let claimed = store.claim_job(250, 100).unwrap().unwrap();
            assert_eq!(claimed.handler, b"heartbeat");
            assert_eq!(store.job_history(&claimed.id).unwrap().len(), 2);
        }
        {
            let store = Store::open(&path).unwrap();
            assert_eq!(
                store
                    .get_schedule(&schedule_id)
                    .unwrap()
                    .unwrap()
                    .next_run_ms,
                350
            );
        }
        let _ = fs::remove_file(path);
    }
}
