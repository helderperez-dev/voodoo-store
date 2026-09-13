//! Language-neutral domain primitives shared by future Store capabilities.
//!
//! These types intentionally contain no Voodoo Framework concepts. They model
//! durable semantics that can be represented consistently through Rust, C ABI,
//! and future language bindings.

/// Delivery behavior requested by a consumer or messaging primitive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliverySemantics {
    /// Delivery may be lost, but a message is never redelivered intentionally.
    AtMostOnce,
    /// Delivery is retried until acknowledged or moved to a terminal policy.
    AtLeastOnce,
    /// Effects are expected to be deduplicated using durable message identity.
    EffectivelyOnce,
}

/// Durable destination category for a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestinationKind {
    Queue,
    Topic,
    Stream,
    Reply,
}

/// State machine for a durable job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    Ready,
    Leased,
    Completed,
    Failed,
    Dead,
    Cancelled,
}

impl JobState {
    /// Returns whether no further normal execution transition is expected.
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Dead | Self::Cancelled)
    }
}

/// Durable scheduling mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleKind {
    /// Execute once at an absolute Unix timestamp expressed in milliseconds.
    OnceAt(i64),
    /// Execute after a relative delay expressed in milliseconds.
    Delay(u64),
    /// Execute repeatedly using a language-neutral cron expression.
    Cron(String),
}

/// Opaque observability identifiers carried through Store without interpreting
/// application-specific meaning.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TraceContext {
    pub trace_id: Option<Vec<u8>>,
    pub correlation_id: Option<Vec<u8>>,
    pub causation_id: Option<Vec<u8>>,
    pub execution_id: Option<Vec<u8>>,
    pub parent_execution_id: Option<Vec<u8>>,
    pub source_id: Option<Vec<u8>>,
}

/// Metadata common to queues, topics, streams, and request/reply messaging.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageEnvelope {
    /// Stable application-visible identity used for deduplication and tracing.
    pub id: Vec<u8>,
    /// Queue/topic/stream/reply category.
    pub destination_kind: DestinationKind,
    /// Language-neutral destination bytes. Higher layers may conventionally use UTF-8.
    pub destination: Vec<u8>,
    /// Opaque payload bytes. Codec identity belongs to a higher-level schema layer.
    pub payload: Vec<u8>,
    /// Creation time in Unix milliseconds. Ordering is never derived from this field.
    pub created_at_ms: i64,
    /// Earliest eligible delivery time in Unix milliseconds, when applicable.
    pub available_at_ms: Option<i64>,
    /// Optional request/reply destination.
    pub reply_to: Option<Vec<u8>>,
    /// Optional partition/ordering key.
    pub partition_key: Option<Vec<u8>>,
    /// Optional durable idempotency identity.
    pub idempotency_key: Option<Vec<u8>>,
    /// Number of completed delivery attempts known to the durable state.
    pub attempts: u32,
    /// Cross-cutting observability metadata.
    pub trace: TraceContext,
}

impl MessageEnvelope {
    /// Creates a minimal envelope without imposing a host-language serializer.
    pub fn new(
        id: impl Into<Vec<u8>>,
        destination_kind: DestinationKind,
        destination: impl Into<Vec<u8>>,
        payload: impl Into<Vec<u8>>,
        created_at_ms: i64,
    ) -> Self {
        Self {
            id: id.into(),
            destination_kind,
            destination: destination.into(),
            payload: payload.into(),
            created_at_ms,
            available_at_ms: None,
            reply_to: None,
            partition_key: None,
            idempotency_key: None,
            attempts: 0,
            trace: TraceContext::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_terminal_job_states_report_terminal() {
        assert!(!JobState::Ready.is_terminal());
        assert!(!JobState::Leased.is_terminal());
        assert!(!JobState::Failed.is_terminal());
        assert!(JobState::Completed.is_terminal());
        assert!(JobState::Dead.is_terminal());
        assert!(JobState::Cancelled.is_terminal());
    }

    #[test]
    fn message_envelope_is_byte_oriented() {
        let envelope = MessageEnvelope::new(
            b"msg-1".to_vec(),
            DestinationKind::Topic,
            b"order.created".to_vec(),
            vec![0, 1, 2, 255],
            1_000,
        );

        assert_eq!(envelope.destination, b"order.created");
        assert_eq!(envelope.payload, vec![0, 1, 2, 255]);
        assert_eq!(envelope.attempts, 0);
        assert_eq!(envelope.available_at_ms, None);
    }
}
