//! Durable cron schedules backed by the Jobs subsystem.
//!
//! Each occurrence derives a deterministic job idempotency key from the cron
//! schedule id and scheduled timestamp. This makes tick retry crash-safe even
//! though the job and cron cursor are persisted through separate subsystem
//! calls: a retry reuses the already-created durable job instead of duplicating
//! work.

use thiserror::Error;

use crate::{CronError, CronExpression, EngineError, JobError, JobId, JobSpec, Store};

const CRON_PREFIX: &[u8] = b"\xffvds:cron:data:";
const VERSION: u8 = 1;

pub type CronScheduleId = [u8; 16];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableCronSchedule {
    pub id: CronScheduleId,
    pub expression: Vec<u8>,
    pub job: JobSpec,
    pub next_run_ms: i64,
    pub enabled: bool,
    pub fire_count: u64,
    pub last_fired_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CronTickReport {
    pub scanned: usize,
    pub fired: usize,
}

#[derive(Debug, Error)]
pub enum CronSchedulerError {
    #[error("store error: {0}")]
    Store(#[from] EngineError),
    #[error("job error: {0}")]
    Job(#[from] JobError),
    #[error("cron error: {0}")]
    Cron(#[from] CronError),
    #[error("cron expression must be UTF-8")]
    InvalidUtf8,
    #[error("cron schedule record is corrupt")]
    CorruptRecord,
    #[error("cron fire count overflow")]
    FireCountOverflow,
    #[error("OS entropy unavailable")]
    EntropyUnavailable,
}

impl Store {
    pub fn create_cron_schedule(
        &mut self,
        expression: impl AsRef<str>,
        job: JobSpec,
        after_ms: i64,
    ) -> Result<CronScheduleId, CronSchedulerError> {
        validate_job(&job)?;
        let expression = expression.as_ref();
        let cron = CronExpression::parse(expression)?;
        let id = random_id()?;
        let schedule = DurableCronSchedule {
            id,
            expression: expression.as_bytes().to_vec(),
            job,
            next_run_ms: cron.next_after_utc_ms(after_ms)?,
            enabled: true,
            fire_count: 0,
            last_fired_at_ms: None,
        };
        self.put_internal(cron_key(&id), encode_schedule(&schedule)?)?;
        Ok(id)
    }

    pub fn get_cron_schedule(
        &self,
        id: &CronScheduleId,
    ) -> Result<Option<DurableCronSchedule>, CronSchedulerError> {
        self.get(cron_key(id)).map(decode_schedule).transpose()
    }

    pub fn list_cron_schedules(&self) -> Result<Vec<DurableCronSchedule>, CronSchedulerError> {
        let mut schedules = self
            .scan_prefix(CRON_PREFIX)
            .into_iter()
            .map(|(_, value)| decode_schedule(&value))
            .collect::<Result<Vec<_>, _>>()?;
        schedules.sort_unstable_by_key(|schedule| schedule.next_run_ms);
        Ok(schedules)
    }

    pub fn set_cron_schedule_enabled(
        &mut self,
        id: &CronScheduleId,
        enabled: bool,
    ) -> Result<bool, CronSchedulerError> {
        let Some(mut schedule) = self.get_cron_schedule(id)? else {
            return Ok(false);
        };
        schedule.enabled = enabled;
        self.put_internal(cron_key(id), encode_schedule(&schedule)?)?;
        Ok(true)
    }

    pub fn tick_cron_schedules(
        &mut self,
        now_ms: i64,
        limit: usize,
    ) -> Result<CronTickReport, CronSchedulerError> {
        let schedules = self.list_cron_schedules()?;
        let scanned = schedules.len();
        let mut fired = 0usize;

        for mut schedule in schedules {
            if fired == limit {
                break;
            }
            if !schedule.enabled || schedule.next_run_ms > now_ms {
                continue;
            }

            let expression = std::str::from_utf8(&schedule.expression)
                .map_err(|_| CronSchedulerError::InvalidUtf8)?;
            let cron = CronExpression::parse(expression)?;
            let scheduled_at = schedule.next_run_ms;
            let mut spec = schedule.job.clone();
            spec.available_at_ms = scheduled_at;
            spec.idempotency_key = Some(occurrence_key(&schedule.id, scheduled_at));

            let _job_id: JobId = self.submit_job(spec, now_ms)?;

            schedule.fire_count = schedule
                .fire_count
                .checked_add(1)
                .ok_or(CronSchedulerError::FireCountOverflow)?;
            schedule.last_fired_at_ms = Some(scheduled_at);
            schedule.next_run_ms = cron.next_after_utc_ms(scheduled_at)?;
            self.put_internal(cron_key(&schedule.id), encode_schedule(&schedule)?)?;
            fired += 1;
        }

        Ok(CronTickReport { scanned, fired })
    }
}

fn occurrence_key(id: &CronScheduleId, scheduled_at_ms: i64) -> Vec<u8> {
    let mut key = Vec::with_capacity(9 + id.len() + 8);
    key.extend_from_slice(b"vds:cron:");
    key.extend_from_slice(id);
    key.extend_from_slice(&scheduled_at_ms.to_be_bytes());
    key
}

fn validate_job(spec: &JobSpec) -> Result<(), CronSchedulerError> {
    if spec.handler.is_empty() || spec.max_attempts == 0 {
        return Err(CronSchedulerError::CorruptRecord);
    }
    Ok(())
}

fn random_id() -> Result<[u8; 16], CronSchedulerError> {
    let mut id = [0u8; 16];
    getrandom::fill(&mut id).map_err(|_| CronSchedulerError::EntropyUnavailable)?;
    Ok(id)
}

fn cron_key(id: &CronScheduleId) -> Vec<u8> {
    let mut key = Vec::with_capacity(CRON_PREFIX.len() + id.len());
    key.extend_from_slice(CRON_PREFIX);
    key.extend_from_slice(id);
    key
}

fn encode_schedule(schedule: &DurableCronSchedule) -> Result<Vec<u8>, CronSchedulerError> {
    let mut out = Vec::new();
    out.push(VERSION);
    out.extend_from_slice(&schedule.id);
    write_bytes(&mut out, &schedule.expression)?;
    encode_job_spec(&mut out, &schedule.job)?;
    out.extend_from_slice(&schedule.next_run_ms.to_le_bytes());
    out.push(u8::from(schedule.enabled));
    out.extend_from_slice(&schedule.fire_count.to_le_bytes());
    match schedule.last_fired_at_ms {
        Some(value) => {
            out.push(1);
            out.extend_from_slice(&value.to_le_bytes());
        }
        None => out.push(0),
    }
    Ok(out)
}

fn decode_schedule(bytes: &[u8]) -> Result<DurableCronSchedule, CronSchedulerError> {
    let mut cursor = Cursor::new(bytes);
    if cursor.u8()? != VERSION {
        return Err(CronSchedulerError::CorruptRecord);
    }
    let id = cursor.array_16()?;
    let expression = cursor.bytes()?;
    let job = decode_job_spec(&mut cursor)?;
    let next_run_ms = cursor.i64()?;
    let enabled = match cursor.u8()? {
        0 => false,
        1 => true,
        _ => return Err(CronSchedulerError::CorruptRecord),
    };
    let fire_count = cursor.u64()?;
    let last_fired_at_ms = match cursor.u8()? {
        0 => None,
        1 => Some(cursor.i64()?),
        _ => return Err(CronSchedulerError::CorruptRecord),
    };
    if !cursor.finished() {
        return Err(CronSchedulerError::CorruptRecord);
    }
    let expression_str = std::str::from_utf8(&expression).map_err(|_| CronSchedulerError::InvalidUtf8)?;
    CronExpression::parse(expression_str)?;
    validate_job(&job)?;
    Ok(DurableCronSchedule {
        id,
        expression,
        job,
        next_run_ms,
        enabled,
        fire_count,
        last_fired_at_ms,
    })
}

fn encode_job_spec(out: &mut Vec<u8>, spec: &JobSpec) -> Result<(), CronSchedulerError> {
    write_bytes(out, &spec.handler)?;
    write_bytes(out, &spec.payload)?;
    out.extend_from_slice(&spec.available_at_ms.to_le_bytes());
    match spec.deadline_ms {
        Some(value) => {
            out.push(1);
            out.extend_from_slice(&value.to_le_bytes());
        }
        None => out.push(0),
    }
    out.extend_from_slice(&spec.priority.to_le_bytes());
    out.extend_from_slice(&spec.max_attempts.to_le_bytes());
    out.extend_from_slice(&spec.retry_backoff_ms.to_le_bytes());
    match &spec.idempotency_key {
        Some(value) => {
            out.push(1);
            write_bytes(out, value)?;
        }
        None => out.push(0),
    }
    Ok(())
}

fn decode_job_spec(cursor: &mut Cursor<'_>) -> Result<JobSpec, CronSchedulerError> {
    let handler = cursor.bytes()?;
    let payload = cursor.bytes()?;
    let available_at_ms = cursor.i64()?;
    let deadline_ms = match cursor.u8()? {
        0 => None,
        1 => Some(cursor.i64()?),
        _ => return Err(CronSchedulerError::CorruptRecord),
    };
    let priority = cursor.i32()?;
    let max_attempts = cursor.u32()?;
    let retry_backoff_ms = cursor.u64()?;
    let idempotency_key = match cursor.u8()? {
        0 => None,
        1 => Some(cursor.bytes()?),
        _ => return Err(CronSchedulerError::CorruptRecord),
    };
    Ok(JobSpec {
        handler,
        payload,
        available_at_ms,
        deadline_ms,
        priority,
        max_attempts,
        retry_backoff_ms,
        idempotency_key,
    })
}

fn write_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<(), CronSchedulerError> {
    let len = u32::try_from(value.len()).map_err(|_| CronSchedulerError::CorruptRecord)?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value);
    Ok(())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], CronSchedulerError> {
        let end = self.pos.checked_add(len).ok_or(CronSchedulerError::CorruptRecord)?;
        let value = self
            .bytes
            .get(self.pos..end)
            .ok_or(CronSchedulerError::CorruptRecord)?;
        self.pos = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, CronSchedulerError> {
        Ok(*self.take(1)?.first().ok_or(CronSchedulerError::CorruptRecord)?)
    }

    fn u32(&mut self) -> Result<u32, CronSchedulerError> {
        Ok(u32::from_le_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| CronSchedulerError::CorruptRecord)?,
        ))
    }

    fn i32(&mut self) -> Result<i32, CronSchedulerError> {
        Ok(i32::from_le_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| CronSchedulerError::CorruptRecord)?,
        ))
    }

    fn u64(&mut self) -> Result<u64, CronSchedulerError> {
        Ok(u64::from_le_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| CronSchedulerError::CorruptRecord)?,
        ))
    }

    fn i64(&mut self) -> Result<i64, CronSchedulerError> {
        Ok(i64::from_le_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| CronSchedulerError::CorruptRecord)?,
        ))
    }

    fn array_16(&mut self) -> Result<[u8; 16], CronSchedulerError> {
        self.take(16)?
            .try_into()
            .map_err(|_| CronSchedulerError::CorruptRecord)
    }

    fn bytes(&mut self) -> Result<Vec<u8>, CronSchedulerError> {
        let len = usize::try_from(self.u32()?).map_err(|_| CronSchedulerError::CorruptRecord)?;
        Ok(self.take(len)?.to_vec())
    }

    fn finished(&self) -> bool {
        self.pos == self.bytes.len()
    }
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
        std::env::temp_dir().join(format!("voodoo-store-cron-{name}-{nonce}.vstore"))
    }

    #[test]
    fn cron_schedule_survives_reopen_and_fires_jobs() {
        let path = temp_store_path("fire");
        let id;
        {
            let mut store = Store::open(&path).unwrap();
            id = store
                .create_cron_schedule(
                    "*/5 * * * *",
                    JobSpec::new(b"digest".to_vec(), b"payload".to_vec()),
                    0,
                )
                .unwrap();
        }
        {
            let mut store = Store::open(&path).unwrap();
            assert_eq!(store.get_cron_schedule(&id).unwrap().unwrap().next_run_ms, 300_000);
            let report = store.tick_cron_schedules(300_000, 10).unwrap();
            assert_eq!(report.fired, 1);
            let schedule = store.get_cron_schedule(&id).unwrap().unwrap();
            assert_eq!(schedule.fire_count, 1);
            assert_eq!(schedule.next_run_ms, 600_000);
            let job = store.claim_job(300_000, 1_000).unwrap().unwrap();
            assert_eq!(job.handler, b"digest");
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn disabled_cron_schedule_does_not_fire() {
        let path = temp_store_path("disabled");
        let mut store = Store::open(&path).unwrap();
        let id = store
            .create_cron_schedule(
                "* * * * *",
                JobSpec::new(b"job".to_vec(), Vec::new()),
                0,
            )
            .unwrap();
        assert!(store.set_cron_schedule_enabled(&id, false).unwrap());
        assert_eq!(store.tick_cron_schedules(60_000, 10).unwrap().fired, 0);
        let _ = fs::remove_file(path);
    }
}
