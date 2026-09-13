//! Durable request/reply primitives.
//!
//! RPC here means durable correlation state, not code execution or network
//! transport. A Runtime/worker consumes pending requests, executes application
//! code elsewhere, and persists a response back into the same `.vstore`.

use thiserror::Error;

use crate::{EngineError, Store, Transaction};

const REQUEST_PREFIX: &[u8] = b"\xffvds:rpc:req:";
const RESPONSE_PREFIX: &[u8] = b"\xffvds:rpc:resp:";
const VERSION: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RpcRequestId {
    pub tx_id: u64,
    pub nonce: [u8; 16],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcRequest {
    pub id: RpcRequestId,
    pub method: Vec<u8>,
    pub payload: Vec<u8>,
    pub created_at_ms: i64,
    pub deadline_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcResponse {
    pub request_id: RpcRequestId,
    pub payload: Vec<u8>,
    pub completed_at_ms: i64,
    pub is_error: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RpcExpireReport {
    pub scanned: usize,
    pub expired: usize,
}

#[derive(Debug, Error)]
pub enum RpcError {
    #[error("store error: {0}")]
    Store(#[from] EngineError),
    #[error("RPC method cannot be empty")]
    EmptyMethod,
    #[error("RPC field is too large")]
    FieldTooLarge,
    #[error("RPC record is corrupt")]
    CorruptRecord,
    #[error("RPC request was not found")]
    NotFound,
    #[error("RPC response already exists")]
    AlreadyResponded,
    #[error("operating-system entropy is unavailable")]
    EntropyUnavailable,
}

impl Transaction<'_> {
    /// Stages a durable RPC request in this transaction.
    pub fn request_rpc(
        &mut self,
        method: impl AsRef<[u8]>,
        payload: impl AsRef<[u8]>,
        created_at_ms: i64,
        deadline_ms: Option<i64>,
    ) -> Result<RpcRequestId, RpcError> {
        let method = method.as_ref();
        if method.is_empty() {
            return Err(RpcError::EmptyMethod);
        }
        let mut nonce = [0u8; 16];
        getrandom::fill(&mut nonce).map_err(|_| RpcError::EntropyUnavailable)?;
        let id = RpcRequestId {
            tx_id: self.id(),
            nonce,
        };
        let request = RpcRequest {
            id,
            method: method.to_vec(),
            payload: payload.as_ref().to_vec(),
            created_at_ms,
            deadline_ms,
        };
        self.put_internal(request_key(id), encode_request(&request)?)?;
        Ok(id)
    }
}

impl Store {
    pub fn submit_rpc_request(
        &mut self,
        method: impl AsRef<[u8]>,
        payload: impl AsRef<[u8]>,
        created_at_ms: i64,
        deadline_ms: Option<i64>,
    ) -> Result<RpcRequestId, RpcError> {
        let mut tx = self.begin()?;
        let id = tx.request_rpc(method, payload, created_at_ms, deadline_ms)?;
        tx.commit()?;
        Ok(id)
    }

    pub fn pending_rpc_requests_after(
        &self,
        after_tx_id: Option<u64>,
        limit: usize,
    ) -> Result<Vec<RpcRequest>, RpcError> {
        let mut requests = Vec::new();
        for (_, encoded) in self.scan_prefix(REQUEST_PREFIX) {
            let request = decode_request(&encoded)?;
            if after_tx_id.is_some_and(|after| request.id.tx_id <= after) {
                continue;
            }
            requests.push(request);
            if requests.len() == limit {
                break;
            }
        }
        Ok(requests)
    }

    pub fn get_rpc_response(&self, id: RpcRequestId) -> Result<Option<RpcResponse>, RpcError> {
        self.get(response_key(id)).map(decode_response).transpose()
    }

    /// Atomically removes the pending request and persists its response.
    pub fn respond_rpc(
        &mut self,
        id: RpcRequestId,
        payload: impl AsRef<[u8]>,
        completed_at_ms: i64,
        is_error: bool,
    ) -> Result<(), RpcError> {
        let request_key = request_key(id);
        if !self.contains_key(&request_key) {
            if self.contains_key(response_key(id)) {
                return Err(RpcError::AlreadyResponded);
            }
            return Err(RpcError::NotFound);
        }
        let response = RpcResponse {
            request_id: id,
            payload: payload.as_ref().to_vec(),
            completed_at_ms,
            is_error,
        };
        let mut tx = self.begin()?;
        tx.delete_internal(request_key)?;
        tx.put_internal(response_key(id), encode_response(&response)?)?;
        tx.commit()?;
        Ok(())
    }

    pub fn ack_rpc_response(&mut self, id: RpcRequestId) -> Result<bool, RpcError> {
        let key = response_key(id);
        if !self.contains_key(&key) {
            return Ok(false);
        }
        self.delete_internal(key)?;
        Ok(true)
    }

    /// Turns expired pending requests into durable error responses.
    pub fn expire_rpc_requests(
        &mut self,
        now_ms: i64,
        limit: usize,
    ) -> Result<RpcExpireReport, RpcError> {
        let pending = self.scan_prefix(REQUEST_PREFIX);
        let scanned = pending.len();
        let mut expired = 0usize;
        for (_, encoded) in pending {
            if expired == limit {
                break;
            }
            let request = decode_request(&encoded)?;
            if request.deadline_ms.is_some_and(|deadline| deadline <= now_ms) {
                self.respond_rpc(request.id, b"deadline exceeded", now_ms, true)?;
                expired += 1;
            }
        }
        Ok(RpcExpireReport { scanned, expired })
    }
}

fn request_key(id: RpcRequestId) -> Vec<u8> {
    let mut key = Vec::with_capacity(REQUEST_PREFIX.len() + 24);
    key.extend_from_slice(REQUEST_PREFIX);
    key.extend_from_slice(&id.tx_id.to_be_bytes());
    key.extend_from_slice(&id.nonce);
    key
}

fn response_key(id: RpcRequestId) -> Vec<u8> {
    let mut key = Vec::with_capacity(RESPONSE_PREFIX.len() + 24);
    key.extend_from_slice(RESPONSE_PREFIX);
    key.extend_from_slice(&id.tx_id.to_be_bytes());
    key.extend_from_slice(&id.nonce);
    key
}

fn encode_request(request: &RpcRequest) -> Result<Vec<u8>, RpcError> {
    let mut out = Vec::new();
    out.push(VERSION);
    out.extend_from_slice(&request.id.tx_id.to_le_bytes());
    out.extend_from_slice(&request.id.nonce);
    out.extend_from_slice(&request.created_at_ms.to_le_bytes());
    match request.deadline_ms {
        Some(deadline) => {
            out.push(1);
            out.extend_from_slice(&deadline.to_le_bytes());
        }
        None => out.push(0),
    }
    write_bytes(&mut out, &request.method)?;
    write_bytes(&mut out, &request.payload)?;
    Ok(out)
}

fn decode_request(bytes: &[u8]) -> Result<RpcRequest, RpcError> {
    let mut cursor = Cursor::new(bytes);
    if cursor.u8()? != VERSION {
        return Err(RpcError::CorruptRecord);
    }
    let tx_id = cursor.u64()?;
    let nonce = cursor.array_16()?;
    let created_at_ms = cursor.i64()?;
    let deadline_ms = match cursor.u8()? {
        0 => None,
        1 => Some(cursor.i64()?),
        _ => return Err(RpcError::CorruptRecord),
    };
    let method = cursor.bytes()?;
    let payload = cursor.bytes()?;
    if method.is_empty() || !cursor.finished() {
        return Err(RpcError::CorruptRecord);
    }
    Ok(RpcRequest {
        id: RpcRequestId { tx_id, nonce },
        method,
        payload,
        created_at_ms,
        deadline_ms,
    })
}

fn encode_response(response: &RpcResponse) -> Result<Vec<u8>, RpcError> {
    let mut out = Vec::new();
    out.push(VERSION);
    out.extend_from_slice(&response.request_id.tx_id.to_le_bytes());
    out.extend_from_slice(&response.request_id.nonce);
    out.extend_from_slice(&response.completed_at_ms.to_le_bytes());
    out.push(u8::from(response.is_error));
    write_bytes(&mut out, &response.payload)?;
    Ok(out)
}

fn decode_response(bytes: &[u8]) -> Result<RpcResponse, RpcError> {
    let mut cursor = Cursor::new(bytes);
    if cursor.u8()? != VERSION {
        return Err(RpcError::CorruptRecord);
    }
    let tx_id = cursor.u64()?;
    let nonce = cursor.array_16()?;
    let completed_at_ms = cursor.i64()?;
    let is_error = match cursor.u8()? {
        0 => false,
        1 => true,
        _ => return Err(RpcError::CorruptRecord),
    };
    let payload = cursor.bytes()?;
    if !cursor.finished() {
        return Err(RpcError::CorruptRecord);
    }
    Ok(RpcResponse {
        request_id: RpcRequestId { tx_id, nonce },
        payload,
        completed_at_ms,
        is_error,
    })
}

fn write_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<(), RpcError> {
    let len = u32::try_from(value.len()).map_err(|_| RpcError::FieldTooLarge)?;
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

    fn take(&mut self, len: usize) -> Result<&'a [u8], RpcError> {
        let end = self.pos.checked_add(len).ok_or(RpcError::CorruptRecord)?;
        let value = self.bytes.get(self.pos..end).ok_or(RpcError::CorruptRecord)?;
        self.pos = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, RpcError> {
        Ok(*self.take(1)?.first().ok_or(RpcError::CorruptRecord)?)
    }

    fn u32(&mut self) -> Result<u32, RpcError> {
        Ok(u32::from_le_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| RpcError::CorruptRecord)?,
        ))
    }

    fn u64(&mut self) -> Result<u64, RpcError> {
        Ok(u64::from_le_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| RpcError::CorruptRecord)?,
        ))
    }

    fn i64(&mut self) -> Result<i64, RpcError> {
        Ok(i64::from_le_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| RpcError::CorruptRecord)?,
        ))
    }

    fn array_16(&mut self) -> Result<[u8; 16], RpcError> {
        self.take(16)?
            .try_into()
            .map_err(|_| RpcError::CorruptRecord)
    }

    fn bytes(&mut self) -> Result<Vec<u8>, RpcError> {
        let len = usize::try_from(self.u32()?).map_err(|_| RpcError::CorruptRecord)?;
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
        std::env::temp_dir().join(format!("voodoo-store-rpc-{name}-{nonce}.vstore"))
    }

    #[test]
    fn request_response_round_trip_is_durable() {
        let path = temp_store_path("roundtrip");
        let id;
        {
            let mut store = Store::open(&path).unwrap();
            id = store
                .submit_rpc_request(b"sum", b"1,2", 10, Some(100))
                .unwrap();
            let pending = store.pending_rpc_requests_after(None, 10).unwrap();
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].method, b"sum");
            store.respond_rpc(id, b"3", 20, false).unwrap();
        }
        {
            let store = Store::open(&path).unwrap();
            assert!(store.pending_rpc_requests_after(None, 10).unwrap().is_empty());
            let response = store.get_rpc_response(id).unwrap().unwrap();
            assert_eq!(response.payload, b"3");
            assert!(!response.is_error);
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn request_can_share_transaction_with_application_state() {
        let path = temp_store_path("transaction");
        let id;
        let mut store = Store::open(&path).unwrap();
        {
            let mut tx = store.begin().unwrap();
            tx.put(b"payment:7", b"pending").unwrap();
            id = tx
                .request_rpc(b"charge", b"7", 1, Some(100))
                .unwrap();
            tx.commit().unwrap();
        }
        assert_eq!(store.get(b"payment:7"), Some(b"pending".as_slice()));
        assert_eq!(store.pending_rpc_requests_after(None, 10).unwrap()[0].id, id);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn expired_request_becomes_error_response() {
        let path = temp_store_path("expire");
        let mut store = Store::open(&path).unwrap();
        let id = store
            .submit_rpc_request(b"slow", Vec::<u8>::new(), 0, Some(10))
            .unwrap();
        let report = store.expire_rpc_requests(10, 10).unwrap();
        assert_eq!(report.expired, 1);
        let response = store.get_rpc_response(id).unwrap().unwrap();
        assert!(response.is_error);
        assert_eq!(response.payload, b"deadline exceeded");
        let _ = fs::remove_file(path);
    }
}
