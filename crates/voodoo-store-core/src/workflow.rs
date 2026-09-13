//! Durable workflow state without embedding a workflow executor.
//!
//! The Store persists orchestration state, waits, signals, timers, and history.
//! Voodoo Runtime remains responsible for deciding and executing application
//! steps.

use thiserror::Error;

use crate::{EngineError, Store};

const INSTANCE_PREFIX: &[u8] = b"\xffvds:wf:instance:";
const HISTORY_PREFIX: &[u8] = b"\xffvds:wf:history:";
const VERSION: u8 = 1;

pub type WorkflowId = [u8; 16];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WorkflowStatus {
    Running = 0,
    Waiting = 1,
    Completed = 2,
    Failed = 3,
    Cancelled = 4,
}

impl TryFrom<u8> for WorkflowStatus {
    type Error = WorkflowError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Running),
            1 => Ok(Self::Waiting),
            2 => Ok(Self::Completed),
            3 => Ok(Self::Failed),
            4 => Ok(Self::Cancelled),
            _ => Err(WorkflowError::CorruptRecord),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowWait {
    Signal { name: Vec<u8> },
    Timer { resume_at_ms: i64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowInstance {
    pub id: WorkflowId,
    pub workflow_type: Vec<u8>,
    pub status: WorkflowStatus,
    pub current_step: Vec<u8>,
    pub state: Vec<u8>,
    pub parent_id: Option<WorkflowId>,
    pub wait: Option<WorkflowWait>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WorkflowHistoryKind {
    Created = 0,
    StepChanged = 1,
    WaitingSignal = 2,
    SignalReceived = 3,
    WaitingTimer = 4,
    TimerFired = 5,
    Completed = 6,
    Failed = 7,
    Cancelled = 8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowHistoryEntry {
    pub sequence: u64,
    pub at_ms: i64,
    pub kind: WorkflowHistoryKind,
    pub detail: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkflowResumeReport {
    pub scanned: usize,
    pub resumed: usize,
}

impl Store {
    pub fn create_workflow(
        &mut self,
        workflow_type: impl AsRef<[u8]>,
        initial_step: impl AsRef<[u8]>,
        initial_state: impl AsRef<[u8]>,
        parent_id: Option<WorkflowId>,
        now_ms: i64,
    ) -> Result<WorkflowId, WorkflowError> {
        let workflow_type = workflow_type.as_ref();
        let initial_step = initial_step.as_ref();
        if workflow_type.is_empty() {
            return Err(WorkflowError::EmptyWorkflowType);
        }
        if initial_step.is_empty() {
            return Err(WorkflowError::EmptyStep);
        }
        let id = random_id()?;
        let instance = WorkflowInstance {
            id,
            workflow_type: workflow_type.to_vec(),
            status: WorkflowStatus::Running,
            current_step: initial_step.to_vec(),
            state: initial_state.as_ref().to_vec(),
            parent_id,
            wait: None,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
        };
        let history = WorkflowHistoryEntry {
            sequence: 0,
            at_ms: now_ms,
            kind: WorkflowHistoryKind::Created,
            detail: Vec::new(),
        };
        let mut tx = self.begin()?;
        tx.put_internal(instance_key(&id), encode_instance(&instance)?)?;
        tx.put_internal(history_key(&id, 0), encode_history(&history)?)?;
        tx.commit()?;
        Ok(id)
    }

    pub fn get_workflow(
        &self,
        id: &WorkflowId,
    ) -> Result<Option<WorkflowInstance>, WorkflowError> {
        self.get(instance_key(id))
            .map(decode_instance)
            .transpose()
    }

    pub fn set_workflow_step(
        &mut self,
        id: &WorkflowId,
        step: impl AsRef<[u8]>,
        state: impl AsRef<[u8]>,
        now_ms: i64,
    ) -> Result<(), WorkflowError> {
        let step = step.as_ref();
        if step.is_empty() {
            return Err(WorkflowError::EmptyStep);
        }
        let mut instance = self
            .get_workflow(id)?
            .ok_or(WorkflowError::NotFound)?;
        ensure_active(&instance)?;
        instance.status = WorkflowStatus::Running;
        instance.current_step = step.to_vec();
        instance.state = state.as_ref().to_vec();
        instance.wait = None;
        instance.updated_at_ms = now_ms;
        self.persist_instance_with_history(
            &instance,
            now_ms,
            WorkflowHistoryKind::StepChanged,
            step,
        )
    }

    pub fn wait_for_signal(
        &mut self,
        id: &WorkflowId,
        signal: impl AsRef<[u8]>,
        now_ms: i64,
    ) -> Result<(), WorkflowError> {
        let signal = signal.as_ref();
        if signal.is_empty() {
            return Err(WorkflowError::EmptySignal);
        }
        let mut instance = self
            .get_workflow(id)?
            .ok_or(WorkflowError::NotFound)?;
        ensure_active(&instance)?;
        instance.status = WorkflowStatus::Waiting;
        instance.wait = Some(WorkflowWait::Signal {
            name: signal.to_vec(),
        });
        instance.updated_at_ms = now_ms;
        self.persist_instance_with_history(
            &instance,
            now_ms,
            WorkflowHistoryKind::WaitingSignal,
            signal,
        )
    }

    pub fn signal_workflow(
        &mut self,
        id: &WorkflowId,
        signal: impl AsRef<[u8]>,
        payload: impl AsRef<[u8]>,
        now_ms: i64,
    ) -> Result<bool, WorkflowError> {
        let signal = signal.as_ref();
        let mut instance = self
            .get_workflow(id)?
            .ok_or(WorkflowError::NotFound)?;
        let Some(WorkflowWait::Signal { name }) = instance.wait.as_ref() else {
            return Ok(false);
        };
        if name.as_slice() != signal {
            return Ok(false);
        }
        instance.status = WorkflowStatus::Running;
        instance.wait = None;
        instance.state = payload.as_ref().to_vec();
        instance.updated_at_ms = now_ms;
        self.persist_instance_with_history(
            &instance,
            now_ms,
            WorkflowHistoryKind::SignalReceived,
            signal,
        )?;
        Ok(true)
    }

    pub fn wait_until(
        &mut self,
        id: &WorkflowId,
        resume_at_ms: i64,
        now_ms: i64,
    ) -> Result<(), WorkflowError> {
        let mut instance = self
            .get_workflow(id)?
            .ok_or(WorkflowError::NotFound)?;
        ensure_active(&instance)?;
        instance.status = WorkflowStatus::Waiting;
        instance.wait = Some(WorkflowWait::Timer { resume_at_ms });
        instance.updated_at_ms = now_ms;
        self.persist_instance_with_history(
            &instance,
            now_ms,
            WorkflowHistoryKind::WaitingTimer,
            &resume_at_ms.to_le_bytes(),
        )
    }

    pub fn resume_due_workflows(
        &mut self,
        now_ms: i64,
        limit: usize,
    ) -> Result<WorkflowResumeReport, WorkflowError> {
        let records = self.scan_prefix(INSTANCE_PREFIX);
        let scanned = records.len();
        let mut due = Vec::new();
        for (_, encoded) in records {
            if due.len() == limit {
                break;
            }
            let instance = decode_instance(&encoded)?;
            if instance.status == WorkflowStatus::Waiting
                && matches!(
                    instance.wait,
                    Some(WorkflowWait::Timer { resume_at_ms }) if resume_at_ms <= now_ms
                )
            {
                due.push(instance);
            }
        }
        let resumed = due.len();
        for mut instance in due {
            instance.status = WorkflowStatus::Running;
            instance.wait = None;
            instance.updated_at_ms = now_ms;
            self.persist_instance_with_history(
                &instance,
                now_ms,
                WorkflowHistoryKind::TimerFired,
                &[],
            )?;
        }
        Ok(WorkflowResumeReport { scanned, resumed })
    }

    pub fn complete_workflow(
        &mut self,
        id: &WorkflowId,
        final_state: impl AsRef<[u8]>,
        now_ms: i64,
    ) -> Result<(), WorkflowError> {
        self.finish_workflow(
            id,
            WorkflowStatus::Completed,
            WorkflowHistoryKind::Completed,
            final_state.as_ref(),
            now_ms,
        )
    }

    pub fn fail_workflow(
        &mut self,
        id: &WorkflowId,
        error: impl AsRef<[u8]>,
        now_ms: i64,
    ) -> Result<(), WorkflowError> {
        self.finish_workflow(
            id,
            WorkflowStatus::Failed,
            WorkflowHistoryKind::Failed,
            error.as_ref(),
            now_ms,
        )
    }

    pub fn cancel_workflow(
        &mut self,
        id: &WorkflowId,
        reason: impl AsRef<[u8]>,
        now_ms: i64,
    ) -> Result<bool, WorkflowError> {
        let Some(mut instance) = self.get_workflow(id)? else {
            return Ok(false);
        };
        if is_terminal(instance.status) {
            return Ok(false);
        }
        instance.status = WorkflowStatus::Cancelled;
        instance.wait = None;
        instance.updated_at_ms = now_ms;
        self.persist_instance_with_history(
            &instance,
            now_ms,
            WorkflowHistoryKind::Cancelled,
            reason.as_ref(),
        )?;
        Ok(true)
    }

    pub fn workflow_history(
        &self,
        id: &WorkflowId,
    ) -> Result<Vec<WorkflowHistoryEntry>, WorkflowError> {
        let mut entries = Vec::new();
        for (_, encoded) in self.scan_prefix(history_prefix(id)) {
            entries.push(decode_history(&encoded)?);
        }
        entries.sort_unstable_by_key(|entry| entry.sequence);
        Ok(entries)
    }

    pub fn workflow_children(
        &self,
        parent_id: &WorkflowId,
    ) -> Result<Vec<WorkflowInstance>, WorkflowError> {
        let mut children = Vec::new();
        for (_, encoded) in self.scan_prefix(INSTANCE_PREFIX) {
            let instance = decode_instance(&encoded)?;
            if instance.parent_id.as_ref() == Some(parent_id) {
                children.push(instance);
            }
        }
        children.sort_unstable_by_key(|instance| instance.created_at_ms);
        Ok(children)
    }

    fn finish_workflow(
        &mut self,
        id: &WorkflowId,
        status: WorkflowStatus,
        kind: WorkflowHistoryKind,
        detail: &[u8],
        now_ms: i64,
    ) -> Result<(), WorkflowError> {
        let mut instance = self
            .get_workflow(id)?
            .ok_or(WorkflowError::NotFound)?;
        if is_terminal(instance.status) {
            return Err(WorkflowError::AlreadyTerminal);
        }
        instance.status = status;
        instance.state = detail.to_vec();
        instance.wait = None;
        instance.updated_at_ms = now_ms;
        self.persist_instance_with_history(&instance, now_ms, kind, detail)
    }

    fn persist_instance_with_history(
        &mut self,
        instance: &WorkflowInstance,
        at_ms: i64,
        kind: WorkflowHistoryKind,
        detail: &[u8],
    ) -> Result<(), WorkflowError> {
        let sequence = self.next_workflow_history_sequence(&instance.id)?;
        let history = WorkflowHistoryEntry {
            sequence,
            at_ms,
            kind,
            detail: detail.to_vec(),
        };
        let mut tx = self.begin()?;
        tx.put_internal(instance_key(&instance.id), encode_instance(instance)?)?;
        tx.put_internal(history_key(&instance.id, sequence), encode_history(&history)?)?;
        tx.commit()?;
        Ok(())
    }

    fn next_workflow_history_sequence(&self, id: &WorkflowId) -> Result<u64, WorkflowError> {
        let records = self.scan_prefix(history_prefix(id));
        match records.last() {
            None => Ok(0),
            Some((key, _)) => decode_history_sequence(id, key)?
                .checked_add(1)
                .ok_or(WorkflowError::HistoryExhausted),
        }
    }
}

fn ensure_active(instance: &WorkflowInstance) -> Result<(), WorkflowError> {
    if is_terminal(instance.status) {
        Err(WorkflowError::AlreadyTerminal)
    } else {
        Ok(())
    }
}

const fn is_terminal(status: WorkflowStatus) -> bool {
    matches!(
        status,
        WorkflowStatus::Completed | WorkflowStatus::Failed | WorkflowStatus::Cancelled
    )
}

fn random_id() -> Result<WorkflowId, WorkflowError> {
    let mut id = [0u8; 16];
    getrandom::fill(&mut id).map_err(|_| WorkflowError::EntropyUnavailable)?;
    Ok(id)
}

fn instance_key(id: &WorkflowId) -> Vec<u8> {
    let mut key = Vec::with_capacity(INSTANCE_PREFIX.len() + 16);
    key.extend_from_slice(INSTANCE_PREFIX);
    key.extend_from_slice(id);
    key
}

fn history_prefix(id: &WorkflowId) -> Vec<u8> {
    let mut key = Vec::with_capacity(HISTORY_PREFIX.len() + 16);
    key.extend_from_slice(HISTORY_PREFIX);
    key.extend_from_slice(id);
    key
}

fn history_key(id: &WorkflowId, sequence: u64) -> Vec<u8> {
    let mut key = history_prefix(id);
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

fn decode_history_sequence(id: &WorkflowId, key: &[u8]) -> Result<u64, WorkflowError> {
    let prefix = history_prefix(id);
    if !key.starts_with(&prefix) || key.len() != prefix.len() + 8 {
        return Err(WorkflowError::CorruptRecord);
    }
    Ok(u64::from_be_bytes(
        key[prefix.len()..]
            .try_into()
            .map_err(|_| WorkflowError::CorruptRecord)?,
    ))
}

fn push_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<(), WorkflowError> {
    let len = u32::try_from(value.len()).map_err(|_| WorkflowError::FieldTooLarge)?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value);
    Ok(())
}

fn read_bytes<'a>(input: &'a [u8], cursor: &mut usize) -> Result<&'a [u8], WorkflowError> {
    if *cursor + 4 > input.len() {
        return Err(WorkflowError::CorruptRecord);
    }
    let len = u32::from_le_bytes(
        input[*cursor..*cursor + 4]
            .try_into()
            .map_err(|_| WorkflowError::CorruptRecord)?,
    ) as usize;
    *cursor += 4;
    let end = (*cursor)
        .checked_add(len)
        .ok_or(WorkflowError::CorruptRecord)?;
    if end > input.len() {
        return Err(WorkflowError::CorruptRecord);
    }
    let value = &input[*cursor..end];
    *cursor = end;
    Ok(value)
}

fn encode_instance(instance: &WorkflowInstance) -> Result<Vec<u8>, WorkflowError> {
    let mut out = Vec::new();
    out.push(VERSION);
    out.extend_from_slice(&instance.id);
    out.push(instance.status as u8);
    out.extend_from_slice(&instance.created_at_ms.to_le_bytes());
    out.extend_from_slice(&instance.updated_at_ms.to_le_bytes());
    match instance.parent_id {
        Some(parent) => {
            out.push(1);
            out.extend_from_slice(&parent);
        }
        None => out.push(0),
    }
    push_bytes(&mut out, &instance.workflow_type)?;
    push_bytes(&mut out, &instance.current_step)?;
    push_bytes(&mut out, &instance.state)?;
    match &instance.wait {
        None => out.push(0),
        Some(WorkflowWait::Signal { name }) => {
            out.push(1);
            push_bytes(&mut out, name)?;
        }
        Some(WorkflowWait::Timer { resume_at_ms }) => {
            out.push(2);
            out.extend_from_slice(&resume_at_ms.to_le_bytes());
        }
    }
    Ok(out)
}

fn decode_instance(input: &[u8]) -> Result<WorkflowInstance, WorkflowError> {
    if input.len() < 35 || input[0] != VERSION {
        return Err(WorkflowError::CorruptRecord);
    }
    let id: WorkflowId = input[1..17]
        .try_into()
        .map_err(|_| WorkflowError::CorruptRecord)?;
    let status = WorkflowStatus::try_from(input[17])?;
    let created_at_ms = i64::from_le_bytes(input[18..26].try_into().unwrap());
    let updated_at_ms = i64::from_le_bytes(input[26..34].try_into().unwrap());
    let mut cursor = 34;
    let parent_id = match input[cursor] {
        0 => {
            cursor += 1;
            None
        }
        1 => {
            cursor += 1;
            if cursor + 16 > input.len() {
                return Err(WorkflowError::CorruptRecord);
            }
            let parent = input[cursor..cursor + 16]
                .try_into()
                .map_err(|_| WorkflowError::CorruptRecord)?;
            cursor += 16;
            Some(parent)
        }
        _ => return Err(WorkflowError::CorruptRecord),
    };
    let workflow_type = read_bytes(input, &mut cursor)?.to_vec();
    let current_step = read_bytes(input, &mut cursor)?.to_vec();
    let state = read_bytes(input, &mut cursor)?.to_vec();
    if cursor >= input.len() {
        return Err(WorkflowError::CorruptRecord);
    }
    let wait = match input[cursor] {
        0 => {
            cursor += 1;
            None
        }
        1 => {
            cursor += 1;
            Some(WorkflowWait::Signal {
                name: read_bytes(input, &mut cursor)?.to_vec(),
            })
        }
        2 => {
            cursor += 1;
            if cursor + 8 > input.len() {
                return Err(WorkflowError::CorruptRecord);
            }
            let resume_at_ms = i64::from_le_bytes(input[cursor..cursor + 8].try_into().unwrap());
            cursor += 8;
            Some(WorkflowWait::Timer { resume_at_ms })
        }
        _ => return Err(WorkflowError::CorruptRecord),
    };
    if cursor != input.len() {
        return Err(WorkflowError::CorruptRecord);
    }
    Ok(WorkflowInstance {
        id,
        workflow_type,
        status,
        current_step,
        state,
        parent_id,
        wait,
        created_at_ms,
        updated_at_ms,
    })
}

fn encode_history(entry: &WorkflowHistoryEntry) -> Result<Vec<u8>, WorkflowError> {
    let mut out = Vec::new();
    out.push(VERSION);
    out.extend_from_slice(&entry.sequence.to_le_bytes());
    out.extend_from_slice(&entry.at_ms.to_le_bytes());
    out.push(entry.kind as u8);
    push_bytes(&mut out, &entry.detail)?;
    Ok(out)
}

fn decode_history(input: &[u8]) -> Result<WorkflowHistoryEntry, WorkflowError> {
    if input.len() < 18 || input[0] != VERSION {
        return Err(WorkflowError::CorruptRecord);
    }
    let sequence = u64::from_le_bytes(input[1..9].try_into().unwrap());
    let at_ms = i64::from_le_bytes(input[9..17].try_into().unwrap());
    let kind = match input[17] {
        0 => WorkflowHistoryKind::Created,
        1 => WorkflowHistoryKind::StepChanged,
        2 => WorkflowHistoryKind::WaitingSignal,
        3 => WorkflowHistoryKind::SignalReceived,
        4 => WorkflowHistoryKind::WaitingTimer,
        5 => WorkflowHistoryKind::TimerFired,
        6 => WorkflowHistoryKind::Completed,
        7 => WorkflowHistoryKind::Failed,
        8 => WorkflowHistoryKind::Cancelled,
        _ => return Err(WorkflowError::CorruptRecord),
    };
    let mut cursor = 18;
    let detail = read_bytes(input, &mut cursor)?.to_vec();
    if cursor != input.len() {
        return Err(WorkflowError::CorruptRecord);
    }
    Ok(WorkflowHistoryEntry {
        sequence,
        at_ms,
        kind,
        detail,
    })
}

#[derive(Debug, Error)]
pub enum WorkflowError {
    #[error("store error: {0}")]
    Store(#[from] EngineError),
    #[error("workflow type cannot be empty")]
    EmptyWorkflowType,
    #[error("workflow step cannot be empty")]
    EmptyStep,
    #[error("signal name cannot be empty")]
    EmptySignal,
    #[error("workflow was not found")]
    NotFound,
    #[error("workflow is already terminal")]
    AlreadyTerminal,
    #[error("workflow field is too large")]
    FieldTooLarge,
    #[error("workflow record is corrupt or unsupported")]
    CorruptRecord,
    #[error("workflow history sequence exhausted")]
    HistoryExhausted,
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
        std::env::temp_dir().join(format!("voodoo-store-workflow-{name}-{nonce}.vstore"))
    }

    #[test]
    fn signal_wait_survives_reopen_and_resumes() {
        let path = temp_store_path("signal");
        let id;
        {
            let mut store = Store::open(&path).unwrap();
            id = store
                .create_workflow(b"approval", b"request", b"pending", None, 0)
                .unwrap();
            store.wait_for_signal(&id, b"approved", 1).unwrap();
        }
        {
            let mut store = Store::open(&path).unwrap();
            assert_eq!(
                store.get_workflow(&id).unwrap().unwrap().status,
                WorkflowStatus::Waiting
            );
            assert!(store.signal_workflow(&id, b"approved", b"yes", 2).unwrap());
            let instance = store.get_workflow(&id).unwrap().unwrap();
            assert_eq!(instance.status, WorkflowStatus::Running);
            assert_eq!(instance.state, b"yes");
            assert_eq!(store.workflow_history(&id).unwrap().len(), 3);
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn timers_resume_and_terminal_state_is_enforced() {
        let path = temp_store_path("timer");
        let mut store = Store::open(&path).unwrap();
        let id = store
            .create_workflow(b"report", b"sleep", Vec::new(), None, 0)
            .unwrap();
        store.wait_until(&id, 100, 1).unwrap();
        assert_eq!(store.resume_due_workflows(99, 10).unwrap().resumed, 0);
        assert_eq!(store.resume_due_workflows(100, 10).unwrap().resumed, 1);
        store.complete_workflow(&id, b"done", 101).unwrap();
        assert!(matches!(
            store.set_workflow_step(&id, b"again", b"x", 102),
            Err(WorkflowError::AlreadyTerminal)
        ));
        drop(store);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn parent_child_relationship_is_queryable() {
        let path = temp_store_path("children");
        let mut store = Store::open(&path).unwrap();
        let parent = store
            .create_workflow(b"parent", b"start", Vec::new(), None, 0)
            .unwrap();
        let child = store
            .create_workflow(b"child", b"start", Vec::new(), Some(parent), 1)
            .unwrap();
        let children = store.workflow_children(&parent).unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].id, child);
        drop(store);
        let _ = fs::remove_file(path);
    }
}
