// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A source consuming a NATS JetStream stream.
//!
//! The source relies on an *ordered* (ephemeral, ack-less) pull consumer and
//! solely on Quickwit checkpoints for delivery semantics: the metastore
//! checkpoint maps the stream name (`PartitionId`) to the stream sequence
//! number of the last indexed message (`Position`). On restart, the consumer
//! is created with `DeliverPolicy::ByStartSequence(checkpoint + 1)`, which
//! gives exactly-once indexing without durable consumers or acknowledgments.
//! The configured deliver policy only applies the very first time the source
//! runs, before any checkpoint exists.
//!
//! The trade-off is that no consumer state lives in NATS while a pipeline is
//! down: the stream must retain messages (limits retention) long enough to
//! cover indexing downtime, and lag monitoring must rely on the running
//! source rather than on NATS durable consumer metrics. To that end, the
//! source exports its pending-messages count and the last time it was caught
//! up as Prometheus gauges.
//!
//! When a message carries a W3C `traceparent` header, the source stitches the
//! processing of the message into the publisher's distributed trace.

use std::fmt;
use std::str::FromStr;
use std::time::{Duration, Instant};

use ::time::OffsetDateTime;
use ::time::format_description::well_known::Rfc3339;
use anyhow::{Context as _, anyhow, bail};
use async_nats::header::{HeaderMap, HeaderName};
use async_nats::jetstream::consumer::DeliverPolicy;
use async_nats::jetstream::consumer::pull::{Ordered as OrderedMessageStream, OrderedConfig};
use async_nats::{ConnectOptions, jetstream};
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use opentelemetry::propagation::Extractor;
use opentelemetry::trace::TraceContextExt;
use opentelemetry::{Context as OtelContext, global};
use quickwit_actors::ActorExitStatus;
use quickwit_config::{NatsSourceAuth, NatsSourceDeliverPolicy, NatsSourceParams};
use quickwit_metastore::checkpoint::PartitionId;
use quickwit_metrics::{Gauge, gauge, label_values};
use quickwit_proto::metastore::SourceType;
use quickwit_proto::types::{IndexUid, Position};
use serde_json::{Value as JsonValue, json};
use tokio::time;
use tracing::{Span, debug, info, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::metrics::{
    INDEX_SOURCE, NATS_SOURCE_CAUGHT_UP_TIMESTAMP_SECONDS, NATS_SOURCE_PENDING_MESSAGES,
};
use crate::source::{
    BATCH_NUM_BYTES_LIMIT, BatchBuilder, EMIT_BATCHES_TIMEOUT, Source, SourceContext,
    SourceRuntime, SourceSink, TypedSourceFactory,
};

/// The pending-messages metrics only feed Prometheus, so refreshing them
/// faster than typical scrape intervals would add NATS round-trips for
/// nothing. Backfill mode bypasses this throttle on idle windows.
const CONSUMER_INFO_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

pub struct NatsSourceFactory;

#[async_trait]
impl TypedSourceFactory for NatsSourceFactory {
    type Source = NatsSource;
    type Params = NatsSourceParams;

    async fn typed_create_source(
        source_runtime: SourceRuntime,
        source_params: NatsSourceParams,
    ) -> anyhow::Result<Self::Source> {
        NatsSource::try_new(source_runtime, source_params).await
    }
}

#[derive(Default, Debug)]
pub struct NatsSourceState {
    /// Number of bytes processed by the source.
    pub num_bytes_processed: u64,
    /// Number of messages processed by the source (including invalid messages).
    pub num_messages_processed: u64,
    /// Number of invalid messages, i.e., that were empty.
    pub num_invalid_messages: u64,
    /// Number of messages skipped because their stream sequence was not past
    /// the current position.
    pub num_skipped_messages: u64,
    /// Number of times the source looped without receiving a single message.
    pub num_consecutive_empty_batches: u64,
    /// Number of messages matching the subject filters that the consumer has
    /// not delivered yet, as of the last consumer info refresh.
    pub num_pending: Option<u64>,
}

pub struct NatsSource {
    source_runtime: SourceRuntime,
    source_params: NatsSourceParams,
    // Kept around to drain the connection on finalize.
    nats_client: async_nats::Client,
    // Kept around to query the consumer info for the pending-messages
    // metrics and the backfill mode.
    jetstream_stream: jetstream::stream::Stream,
    // The ordered message stream transparently recreates the underlying
    // ephemeral consumer from the last delivered stream sequence when it is
    // deleted or its heartbeats are missed, so delivery stays gap-free and
    // in stream sequence order for the lifetime of this source.
    message_stream: OrderedMessageStream,
    backfill_mode_enabled: bool,
    partition_id: PartitionId,
    current_position: Position,
    consumer_name: String,
    next_consumer_info_refresh: Instant,
    pending_messages_gauge: Gauge,
    caught_up_timestamp_gauge: Gauge,
    state: NatsSourceState,
}

impl fmt::Debug for NatsSource {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("NatsSource")
            .field("index_uid", self.source_runtime.index_uid())
            .field("source_id", &self.source_runtime.source_id())
            .field("stream", &self.source_params.stream)
            .field("consumer_name", &self.consumer_name)
            .finish()
    }
}

impl NatsSource {
    pub async fn try_new(
        source_runtime: SourceRuntime,
        source_params: NatsSourceParams,
    ) -> anyhow::Result<Self> {
        let consumer_name = consumer_name(source_runtime.index_uid(), source_runtime.source_id());

        info!(
            index_id=%source_runtime.index_id(),
            source_id=%source_runtime.source_id(),
            stream=%source_params.stream,
            subjects=?source_params.subjects,
            %consumer_name,
            "starting NATS source"
        );

        let nats_client = connect_nats(&source_params).await?;
        let jetstream_ctx = jetstream::new(nats_client.clone());
        let jetstream_stream = jetstream_ctx
            .get_stream(&source_params.stream)
            .await
            .with_context(|| {
                format!(
                    "failed to find NATS JetStream stream `{}`",
                    source_params.stream
                )
            })?;

        let partition_id = PartitionId::from(source_params.stream.as_str());
        let checkpoint = source_runtime.fetch_checkpoint().await?;
        let current_position = checkpoint
            .position_for_partition(&partition_id)
            .cloned()
            .unwrap_or(Position::Beginning);

        log_retention_gap(
            &current_position,
            jetstream_stream.cached_info().state.first_sequence,
            &source_params.stream,
            &source_params.subjects,
        );

        let consumer_config = build_ordered_consumer_config(
            consumer_name.clone(),
            &source_params,
            &source_runtime,
            &current_position,
        )?;

        let consumer = jetstream_stream
            .create_consumer(consumer_config)
            .await
            .with_context(|| {
                format!(
                    "failed to create NATS consumer `{consumer_name}` on stream `{}`",
                    source_params.stream
                )
            })?;
        let message_stream = consumer
            .messages()
            .await
            .context("failed to subscribe to NATS consumer messages")?;

        let backfill_mode_enabled = source_params.enable_backfill_mode;

        let index_label = quickwit_common::metrics::index_label(source_runtime.index_id());
        let source_labels = label_values!(
            INDEX_SOURCE =>
            index_label.to_string(),
            source_runtime.source_id().to_string()
        );
        let pending_messages_gauge =
            gauge!(parent: NATS_SOURCE_PENDING_MESSAGES, labels: [source_labels.clone()]);
        let caught_up_timestamp_gauge =
            gauge!(parent: NATS_SOURCE_CAUGHT_UP_TIMESTAMP_SECONDS, labels: [source_labels]);

        Ok(NatsSource {
            source_runtime,
            source_params,
            nats_client,
            jetstream_stream,
            message_stream,
            backfill_mode_enabled,
            partition_id,
            current_position,
            consumer_name,
            // An immediate first refresh makes the metrics appear right after
            // the pipeline starts.
            next_consumer_info_refresh: Instant::now(),
            pending_messages_gauge,
            caught_up_timestamp_gauge,
            state: NatsSourceState::default(),
        })
    }

    fn process_message(
        &mut self,
        message: jetstream::Message,
        batch: &mut BatchBuilder,
    ) -> anyhow::Result<()> {
        let stream_sequence = message
            .info()
            .map_err(|error| anyhow!("failed to parse NATS message metadata: {error}"))?
            .stream_sequence;
        let _span_guard = self
            .remote_parented_span(&message, stream_sequence)
            .map(Span::entered);
        self.add_doc_to_batch(
            Position::offset(stream_sequence),
            message.message.payload,
            batch,
        )
    }

    /// Builds a span parented on the publisher's trace when the message
    /// carries a W3C `traceparent` header, stitching the processing of the
    /// message into the publisher's distributed trace. Messages without a
    /// propagated context cost nothing: no span is created.
    ///
    /// The span ends once the message is added to the batch: documents are
    /// batched downstream, so the per-message trace context stops here.
    fn remote_parented_span(
        &self,
        message: &jetstream::Message,
        stream_sequence: u64,
    ) -> Option<Span> {
        let headers = message.headers.as_ref()?;
        let parent_context = extract_remote_context(headers);
        if !parent_context.span().span_context().is_valid() {
            return None;
        }
        let span = tracing::info_span!(
            "process_nats_message",
            index_id = %self.source_runtime.index_id(),
            source_id = %self.source_runtime.source_id(),
            subject = %message.subject,
            stream_sequence,
        );
        let _ = span.set_parent(parent_context);
        Some(span)
    }

    fn add_doc_to_batch(
        &mut self,
        message_position: Position,
        doc: Bytes,
        batch: &mut BatchBuilder,
    ) -> anyhow::Result<()> {
        // The ordered message stream is not expected to redeliver messages,
        // but a stale message would corrupt the checkpoint delta, so we drop
        // it defensively.
        if message_position <= self.current_position {
            self.state.num_skipped_messages += 1;
            return Ok(());
        }
        let num_bytes = doc.len() as u64;

        if doc.is_empty() {
            warn!("message received from NATS was empty");
            self.state.num_invalid_messages += 1;
        } else {
            batch.add_doc(doc);
        }
        // The position advances even for invalid messages so that they are
        // not redelivered after a restart.
        batch
            .checkpoint_delta
            .record_partition_delta(
                self.partition_id.clone(),
                self.current_position.clone(),
                message_position.clone(),
            )
            .context("failed to record partition delta")?;
        self.current_position = message_position;

        self.state.num_bytes_processed += num_bytes;
        self.state.num_messages_processed += 1;

        Ok(())
    }

    /// Refreshes the pending-messages metrics from the consumer info and
    /// returns the number of messages the consumer has not delivered yet.
    ///
    /// The consumer tracks its pending messages with the subject filters
    /// applied, so the count is exact, unlike comparing the current position
    /// with the last sequence of the stream. Errors return `None` because the
    /// ephemeral consumer is briefly absent while the message stream
    /// recreates it.
    async fn refresh_consumer_info(&mut self, ctx: &SourceContext) -> Option<u64> {
        self.next_consumer_info_refresh = Instant::now() + CONSUMER_INFO_REFRESH_INTERVAL;
        match ctx
            .protect_future(self.jetstream_stream.consumer_info(&self.consumer_name))
            .await
        {
            Ok(consumer_info) => {
                let num_pending = consumer_info.num_pending;
                self.state.num_pending = Some(num_pending);
                self.pending_messages_gauge.set(num_pending as f64);
                if num_pending == 0 {
                    self.caught_up_timestamp_gauge
                        .set(OffsetDateTime::now_utc().unix_timestamp() as f64);
                }
                Some(num_pending)
            }
            Err(error) => {
                warn!(%error, "failed to fetch NATS consumer info");
                None
            }
        }
    }
}

#[async_trait]
impl Source for NatsSource {
    async fn emit_batches(
        &mut self,
        source_sink: &SourceSink,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        let now = Instant::now();
        let mut batch_builder = BatchBuilder::new(SourceType::Nats);
        let deadline = time::sleep(*EMIT_BATCHES_TIMEOUT);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                message = self.message_stream.next() => {
                    let message = message
                        .ok_or_else(|| ActorExitStatus::from(anyhow!("NATS message stream ended unexpectedly")))?
                        .map_err(|error| ActorExitStatus::from(anyhow!("failed to pull message from NATS consumer: {error}")))?;

                    self.process_message(message, &mut batch_builder).map_err(ActorExitStatus::from)?;

                    if batch_builder.num_bytes >= BATCH_NUM_BYTES_LIMIT {
                        break;
                    }
                }
                _ = &mut deadline => {
                    break;
                }
            }
            ctx.record_progress();
        }

        let batch_is_empty = batch_builder.checkpoint_delta.is_empty();

        if batch_is_empty {
            self.state.num_consecutive_empty_batches += 1;
        } else {
            self.state.num_consecutive_empty_batches = 0;
            debug!(
                num_docs=%batch_builder.docs.len(),
                num_bytes=%batch_builder.num_bytes,
                num_millis=%now.elapsed().as_millis(),
                "sending doc batch to indexer"
            );
            let message = batch_builder.build();
            source_sink.send_raw_doc_batch(message, ctx).await?;
        }
        // Backfill mode needs a fresh pending count on every idle window to
        // detect the end of the stream.
        let backfill_check = self.backfill_mode_enabled && batch_is_empty;
        let num_pending_opt = if backfill_check || self.next_consumer_info_refresh <= Instant::now()
        {
            self.refresh_consumer_info(ctx).await
        } else {
            None
        };
        if backfill_check && num_pending_opt == Some(0) {
            info!(stream=%self.source_params.stream, "reached end of stream");
            source_sink.send_exit_with_success(ctx).await?;
            return Err(ActorExitStatus::Success);
        }
        Ok(Duration::default())
    }

    fn name(&self) -> String {
        format!("{self:?}")
    }

    async fn finalize(
        &mut self,
        _exit_status: &ActorExitStatus,
        _ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        // The ephemeral consumer is reaped by the server via its inactivity
        // threshold; only the connection needs to be cleaned up.
        self.nats_client.drain().await?;
        Ok(())
    }

    fn observable_state(&self) -> JsonValue {
        json!({
            "index_id": self.source_runtime.index_id(),
            "source_id": self.source_runtime.source_id(),
            "stream": self.source_params.stream,
            "subjects": self.source_params.subjects,
            "consumer_name": self.consumer_name,
            "current_position": self.current_position,
            "num_bytes_processed": self.state.num_bytes_processed,
            "num_messages_processed": self.state.num_messages_processed,
            "num_invalid_messages": self.state.num_invalid_messages,
            "num_skipped_messages": self.state.num_skipped_messages,
            "num_consecutive_empty_batches": self.state.num_consecutive_empty_batches,
            "num_pending": self.state.num_pending,
        })
    }
}

/// Builds the consumer configuration.
///
/// The pull loop driving the message stream is not configurable in
/// `async-nats` 0.50: it requests batches of 500 messages expiring after 30
/// seconds with a 15 seconds idle heartbeat, and the server reaps the
/// consumer after 30 seconds of inactivity.
fn build_ordered_consumer_config(
    consumer_name: String,
    source_params: &NatsSourceParams,
    source_runtime: &SourceRuntime,
    current_position: &Position,
) -> Result<OrderedConfig, anyhow::Error> {
    let description = format!(
        "Quickwit source `{}` of index `{}`",
        source_runtime.source_id(),
        source_runtime.index_id()
    );
    let deliver_policy =
        deliver_policy_from_position(current_position, &source_params.deliver_policy)?;
    Ok(OrderedConfig {
        name: Some(consumer_name),
        description: Some(description),
        // `filter_subject` (singular) is the pre-2.10 variant and must not be combined with
        // `filter_subjects`.
        filter_subject: String::new(),
        filter_subjects: source_params.subjects.clone(),
        deliver_policy,
        ..Default::default()
    })
}

/// Maps a checkpoint position to the deliver policy of the consumer created
/// at source startup. The deliver policy configured on the source only
/// applies before the first checkpoint exists.
///
/// Quickwit positions are inclusive whereas `ByStartSequence` is not, hence
/// the increment by 1.
fn deliver_policy_from_position(
    position: &Position,
    initial_deliver_policy: &NatsSourceDeliverPolicy,
) -> anyhow::Result<DeliverPolicy> {
    match position {
        Position::Beginning => initial_deliver_policy_to_nats(initial_deliver_policy),
        Position::Offset(offset) => {
            let stream_sequence = offset.as_u64().ok_or_else(|| {
                anyhow!("invalid checkpoint position `{offset}`: expected a stream sequence")
            })?;
            Ok(DeliverPolicy::ByStartSequence {
                start_sequence: stream_sequence + 1,
            })
        }
        Position::Eof(_) => bail!("position of a NATS stream should never be EOF"),
    }
}

fn initial_deliver_policy_to_nats(
    deliver_policy: &NatsSourceDeliverPolicy,
) -> anyhow::Result<DeliverPolicy> {
    match deliver_policy {
        NatsSourceDeliverPolicy::All => Ok(DeliverPolicy::All),
        NatsSourceDeliverPolicy::New => Ok(DeliverPolicy::New),
        NatsSourceDeliverPolicy::Last => Ok(DeliverPolicy::Last),
        NatsSourceDeliverPolicy::ByStartTime(start_time) => {
            let start_time = OffsetDateTime::parse(start_time, &Rfc3339)
                .with_context(|| format!("invalid `deliver_policy` start time `{start_time}`"))?;
            Ok(DeliverPolicy::ByStartTime { start_time })
        }
    }
}

/// Number of stream sequences between the checkpoint and the first retained
/// message. Stream sequences are dense, so a non-zero gap means that many
/// messages were deleted by the retention policy past the checkpoint, before
/// this source could index them.
fn retention_gap(checkpoint_position: &Position, first_sequence: u64) -> u64 {
    let Some(checkpoint_sequence) = checkpoint_position.as_u64() else {
        return 0;
    };
    first_sequence.saturating_sub(checkpoint_sequence + 1)
}

/// Without subject filters, a retention gap is certain message loss. With
/// filters, the deleted messages may all have belonged to subjects this
/// source does not consume — the checkpoint of a long-idle source lags the
/// stream head forever — and NATS keeps no trace allowing to tell the
/// difference after the fact, so we only leave an info-level log.
fn log_retention_gap(
    checkpoint_position: &Position,
    first_sequence: u64,
    stream: &str,
    subjects: &[String],
) {
    let num_deleted_messages = retention_gap(checkpoint_position, first_sequence);
    if num_deleted_messages == 0 {
        return;
    }
    if subjects.is_empty() {
        warn!(
            stream,
            checkpoint_position=%checkpoint_position,
            first_sequence,
            num_deleted_messages,
            "messages were deleted by the NATS retention policy before they could be indexed"
        );
    } else {
        info!(
            stream,
            checkpoint_position=%checkpoint_position,
            first_sequence,
            num_deleted_messages,
            "messages were deleted by the NATS retention policy before this source caught up; \
             they may or may not have matched the subject filters"
        );
    }
}

/// W3C trace context extraction from NATS message headers, following the
/// pattern of `quickwit_common::tracing_utils` for gRPC metadata.
struct NatsHeaderExtractor<'a>(&'a HeaderMap);

impl Extractor for NatsHeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        let header_name = HeaderName::from_str(key).ok()?;
        self.0.get(header_name).map(|value| value.as_str())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.iter().map(|(key, _)| key.as_ref()).collect()
    }
}

/// Extracts an OpenTelemetry context from the message headers. Returns the
/// empty context when no global text-map propagator is installed (the
/// telemetry initialization of the Quickwit binaries installs one).
fn extract_remote_context(headers: &HeaderMap) -> OtelContext {
    global::get_text_map_propagator(|propagator| propagator.extract(&NatsHeaderExtractor(headers)))
}

async fn connect_nats(params: &NatsSourceParams) -> anyhow::Result<async_nats::Client> {
    let mut connect_options = ConnectOptions::new();
    match params.authentication.clone() {
        None => {}
        Some(NatsSourceAuth::UserPassword { user, password }) => {
            connect_options = connect_options.user_and_password(user, password);
        }
        Some(NatsSourceAuth::Token(token)) => {
            connect_options = connect_options.token(token);
        }
    }
    let client = async_nats::connect_with_options(&params.uris, connect_options)
        .await
        .with_context(|| {
            format!(
                "failed to connect to NATS servers `{}`",
                params.uris.join(", ")
            )
        })?;
    Ok(client)
}

/// Checks whether we can connect to the NATS servers and find the JetStream
/// stream.
pub(crate) async fn check_connectivity(params: &NatsSourceParams) -> anyhow::Result<()> {
    let client = connect_nats(params).await?;
    let jetstream_ctx = jetstream::new(client);
    jetstream_ctx
        .get_stream(&params.stream)
        .await
        .with_context(|| format!("failed to find NATS JetStream stream `{}`", params.stream))?;
    Ok(())
}

/// The incarnation ID keeps the name unique when an index is deleted and
/// recreated with the same ID.
fn consumer_name(index_uid: &IndexUid, source_id: &str) -> String {
    sanitize_nats_name(&format!(
        "quickwit-{}-{}-{}",
        index_uid.index_id, source_id, index_uid.incarnation_id
    ))
}

/// NATS names cannot contain whitespace, `.`, `*` or `>`, while Quickwit IDs
/// may contain `.`.
fn sanitize_nats_name(name: &str) -> String {
    name.chars()
        .map(|char| {
            if char.is_ascii_alphanumeric() || char == '-' || char == '_' {
                char
            } else {
                '-'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deliver_policy_from_position() {
        assert_eq!(
            deliver_policy_from_position(&Position::Beginning, &NatsSourceDeliverPolicy::All)
                .unwrap(),
            DeliverPolicy::All
        );
        assert_eq!(
            deliver_policy_from_position(&Position::Beginning, &NatsSourceDeliverPolicy::New)
                .unwrap(),
            DeliverPolicy::New
        );
        assert_eq!(
            deliver_policy_from_position(&Position::Beginning, &NatsSourceDeliverPolicy::Last)
                .unwrap(),
            DeliverPolicy::Last
        );
        assert_eq!(
            deliver_policy_from_position(
                &Position::Beginning,
                &NatsSourceDeliverPolicy::ByStartTime("2026-08-31T01:02:03Z".to_string())
            )
            .unwrap(),
            DeliverPolicy::ByStartTime {
                start_time: OffsetDateTime::parse("2026-08-31T01:02:03Z", &Rfc3339).unwrap()
            }
        );
        deliver_policy_from_position(
            &Position::Beginning,
            &NatsSourceDeliverPolicy::ByStartTime("not-a-timestamp".to_string()),
        )
        .unwrap_err();

        // The configured deliver policy is ignored once a checkpoint exists.
        assert_eq!(
            deliver_policy_from_position(&Position::offset(41u64), &NatsSourceDeliverPolicy::New)
                .unwrap(),
            DeliverPolicy::ByStartSequence { start_sequence: 42 }
        );
        deliver_policy_from_position(&Position::Eof(None), &NatsSourceDeliverPolicy::All)
            .unwrap_err();
        deliver_policy_from_position(
            &Position::from("not-a-sequence".to_string()),
            &NatsSourceDeliverPolicy::All,
        )
        .unwrap_err();
    }

    #[test]
    fn test_consumer_name() {
        let index_uid = IndexUid::for_test("test-index", 1);
        assert_eq!(
            consumer_name(&index_uid, "test-source"),
            format!(
                "quickwit-test-index-test-source-{}",
                index_uid.incarnation_id
            )
        );
        let index_uid = IndexUid::for_test("test.index", 2);
        let consumer_name = consumer_name(&index_uid, "test.source");
        assert!(!consumer_name.contains('.'));
        assert!(consumer_name.starts_with("quickwit-test-index-test-source-"));
    }

    #[test]
    fn test_sanitize_nats_name() {
        assert_eq!(sanitize_nats_name("my-name_09"), "my-name_09");
        assert_eq!(sanitize_nats_name("my.name my>name*"), "my-name-my-name-");
    }

    #[test]
    fn test_extract_trace_context_from_headers() {
        use opentelemetry::propagation::TextMapPropagator;
        use opentelemetry_sdk::propagation::TraceContextPropagator;

        // The global propagator is not installed in tests, so the extractor
        // is exercised against an explicit W3C propagator.
        let propagator = TraceContextPropagator::new();

        let mut headers = HeaderMap::new();
        headers.insert(
            "traceparent",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        );
        let span_context = propagator
            .extract(&NatsHeaderExtractor(&headers))
            .span()
            .span_context()
            .clone();
        assert!(span_context.is_valid());
        assert_eq!(
            span_context.trace_id().to_string(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
        assert_eq!(span_context.span_id().to_string(), "00f067aa0ba902b7");

        let empty_headers = HeaderMap::new();
        let span_context = propagator
            .extract(&NatsHeaderExtractor(&empty_headers))
            .span()
            .span_context()
            .clone();
        assert!(!span_context.is_valid());
    }

    #[test]
    fn test_retention_gap() {
        // A fresh source has no checkpoint: retention having already deleted
        // old messages is normal, not a gap.
        assert_eq!(retention_gap(&Position::Beginning, 100), 0);
        // The message right after the checkpoint is still retained.
        assert_eq!(retention_gap(&Position::offset(41u64), 42), 0);
        assert_eq!(retention_gap(&Position::offset(41u64), 10), 0);
        // Messages 42, 43 and 44 were deleted.
        assert_eq!(retention_gap(&Position::offset(41u64), 45), 3);
    }
}

#[cfg(all(test, feature = "nats-broker-tests"))]
mod nats_broker_tests {
    use std::num::NonZeroUsize;
    use std::ops::Range;

    use quickwit_actors::{ActorHandle, Inbox, Universe};
    use quickwit_common::rand::append_random_suffix;
    use quickwit_config::{SourceConfig, SourceInputFormat, SourceParams};
    use quickwit_metastore::checkpoint::SourceCheckpointDelta;
    use quickwit_metastore::metastore_for_test;
    use quickwit_proto::metastore::MetastoreServiceClient;

    use super::*;
    use crate::actors::DocProcessor;
    use crate::models::RawDocBatch;
    use crate::source::test_setup_helper::setup_index;
    use crate::source::tests::SourceRuntimeBuilder;
    use crate::source::{SourceActor, quickwit_supported_sources};

    static NATS_URI: &str = "nats://localhost:4222";

    fn get_source_config(
        stream: &str,
        subjects: Vec<String>,
        deliver_policy: NatsSourceDeliverPolicy,
        enable_backfill_mode: bool,
    ) -> SourceConfig {
        let source_id = append_random_suffix("test-nats-source--source");
        SourceConfig {
            source_id,
            num_pipelines: NonZeroUsize::MIN,
            enabled: true,
            source_params: SourceParams::Nats(NatsSourceParams {
                uris: vec![NATS_URI.to_string()],
                stream: stream.to_string(),
                subjects,
                deliver_policy,
                enable_backfill_mode,
                authentication: None,
            }),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        }
    }

    async fn setup_nats_stream(stream_name: &str) -> jetstream::Context {
        let client = async_nats::connect(NATS_URI).await.unwrap();
        let jetstream_ctx = jetstream::new(client);
        jetstream_ctx
            .create_stream(jetstream::stream::Config {
                name: stream_name.to_string(),
                subjects: vec![format!("{stream_name}.>")],
                ..Default::default()
            })
            .await
            .unwrap();
        jetstream_ctx
    }

    /// Publishes one JSON doc per ID on the subject and waits for each
    /// publish ack, so stream sequences are assigned in `ids` order. Messages
    /// carry a W3C `traceparent` header to exercise the trace propagation
    /// path.
    async fn publish_docs(
        jetstream_ctx: &jetstream::Context,
        subject: &str,
        ids: Range<usize>,
    ) -> Vec<String> {
        let mut headers = HeaderMap::new();
        headers.insert(
            "traceparent",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        );
        let mut docs = Vec::with_capacity(ids.len());
        for id in ids {
            let doc = json!({ "id": id, "subject": subject }).to_string();
            jetstream_ctx
                .publish_with_headers(subject.to_string(), headers.clone(), doc.clone().into())
                .await
                .unwrap()
                .await
                .unwrap();
            docs.push(doc);
        }
        docs
    }

    async fn create_source_actor(
        universe: &Universe,
        metastore: MetastoreServiceClient,
        index_uid: IndexUid,
        source_config: SourceConfig,
    ) -> (ActorHandle<SourceActor>, Inbox<DocProcessor>) {
        let source_runtime = SourceRuntimeBuilder::new(index_uid, source_config)
            .with_metastore(metastore)
            .build();
        let source = quickwit_supported_sources()
            .load_source(source_runtime)
            .await
            .unwrap();
        let (doc_processor_mailbox, doc_processor_inbox) = universe.create_test_mailbox();
        let source_actor = SourceActor::new(source, doc_processor_mailbox);
        let (_source_mailbox, source_handle) = universe.spawn_builder().spawn(source_actor);
        (source_handle, doc_processor_inbox)
    }

    async fn wait_for_processed_messages(
        source_handle: &ActorHandle<SourceActor>,
        num_expected: u64,
    ) {
        loop {
            let observation = source_handle.observe().await;
            let num_messages_processed = observation
                .state
                .get("num_messages_processed")
                .unwrap()
                .as_u64()
                .unwrap();
            if num_messages_processed >= num_expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn merge_doc_batches(batches: Vec<RawDocBatch>) -> RawDocBatch {
        let mut merged_batch = RawDocBatch::default();
        for batch in batches {
            merged_batch.docs.extend(batch.docs);
            merged_batch
                .checkpoint_delta
                .extend(batch.checkpoint_delta)
                .unwrap();
        }
        merged_batch.docs.sort();
        merged_batch
    }

    fn expected_checkpoint_delta(stream: &str, to_sequence: u64) -> SourceCheckpointDelta {
        let mut checkpoint_delta = SourceCheckpointDelta::default();
        checkpoint_delta
            .record_partition_delta(
                PartitionId::from(stream),
                Position::Beginning,
                Position::offset(to_sequence),
            )
            .unwrap();
        checkpoint_delta
    }

    #[tokio::test]
    async fn test_doc_batching_logic() {
        let stream = append_random_suffix("test-nats-source--batching--stream");
        let jetstream_ctx = setup_nats_stream(&stream).await;

        let index_id = append_random_suffix("test-nats-source--batching--index");
        let index_uid = IndexUid::new_with_random_ulid(&index_id);
        let source_config =
            get_source_config(&stream, Vec::new(), NatsSourceDeliverPolicy::All, false);
        let SourceParams::Nats(params) = source_config.clone().source_params else {
            unreachable!()
        };
        let source_runtime = SourceRuntimeBuilder::new(index_uid, source_config).build();
        let mut nats_source = NatsSource::try_new(source_runtime, params).await.unwrap();

        let mut batch = BatchBuilder::new(SourceType::Nats);
        nats_source
            .add_doc_to_batch(Position::offset(1u64), Bytes::from_static(b""), &mut batch)
            .unwrap();
        // Invalid messages advance the position without adding a doc.
        assert_eq!(nats_source.state.num_invalid_messages, 1);
        assert_eq!(nats_source.state.num_messages_processed, 1);
        assert_eq!(nats_source.state.num_bytes_processed, 0);
        assert_eq!(nats_source.current_position, Position::offset(1u64));
        assert!(batch.docs.is_empty());
        assert!(!batch.checkpoint_delta.is_empty());

        // Stale positions are skipped.
        let mut batch = BatchBuilder::new(SourceType::Nats);
        nats_source
            .add_doc_to_batch(
                Position::offset(1u64),
                Bytes::from_static(b"stale"),
                &mut batch,
            )
            .unwrap();
        assert_eq!(nats_source.state.num_skipped_messages, 1);
        assert_eq!(nats_source.state.num_messages_processed, 1);
        assert!(batch.docs.is_empty());
        assert!(batch.checkpoint_delta.is_empty());

        let mut batch = BatchBuilder::new(SourceType::Nats);
        nats_source
            .add_doc_to_batch(
                Position::offset(2u64),
                Bytes::from_static(b"some-demo-data"),
                &mut batch,
            )
            .unwrap();
        nats_source
            .add_doc_to_batch(
                Position::offset(4u64),
                Bytes::from_static(b"some-demo-data-2"),
                &mut batch,
            )
            .unwrap();
        assert_eq!(nats_source.state.num_messages_processed, 3);
        assert_eq!(nats_source.state.num_bytes_processed, 30);
        assert_eq!(nats_source.current_position, Position::offset(4u64));
        assert_eq!(batch.docs.len(), 2);
        assert_eq!(batch.num_bytes, 30);

        let mut expected_delta = SourceCheckpointDelta::default();
        expected_delta
            .record_partition_delta(
                PartitionId::from(stream.as_str()),
                Position::offset(1u64),
                Position::offset(4u64),
            )
            .unwrap();
        assert_eq!(batch.checkpoint_delta, expected_delta);

        drop(nats_source);
        jetstream_ctx.delete_stream(&stream).await.unwrap();
    }

    #[tokio::test]
    async fn test_stream_ingestion_with_subject_filter() {
        let universe = Universe::with_accelerated_time();
        let metastore = metastore_for_test();
        let stream = append_random_suffix("test-nats-source--ingestion--stream");
        let jetstream_ctx = setup_nats_stream(&stream).await;

        let matching_subject = format!("{stream}.logs");
        let other_subject = format!("{stream}.metrics");
        let expected_docs = publish_docs(&jetstream_ctx, &matching_subject, 0..10).await;

        let index_id = append_random_suffix("test-nats-source--ingestion--index");
        let source_config = get_source_config(
            &stream,
            vec![matching_subject.clone()],
            NatsSourceDeliverPolicy::All,
            false,
        );
        let index_uid = setup_index(metastore.clone(), &index_id, &source_config, &[]).await;

        let (source_handle, doc_processor_inbox) =
            create_source_actor(&universe, metastore, index_uid, source_config).await;

        // Messages published on non-matching subjects must not be indexed.
        publish_docs(&jetstream_ctx, &other_subject, 0..3).await;

        wait_for_processed_messages(&source_handle, 10).await;
        source_handle.quit().await;

        let batches: Vec<RawDocBatch> = doc_processor_inbox.drain_for_test_typed();
        assert!(!batches.is_empty());
        let batch = merge_doc_batches(batches);
        assert_eq!(batch.docs, expected_docs);
        assert_eq!(
            batch.checkpoint_delta,
            expected_checkpoint_delta(&stream, 10)
        );

        jetstream_ctx.delete_stream(&stream).await.unwrap();
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_resume_from_checkpoint() {
        let universe = Universe::with_accelerated_time();
        let metastore = metastore_for_test();
        let stream = append_random_suffix("test-nats-source--resume--stream");
        let jetstream_ctx = setup_nats_stream(&stream).await;

        let subject = format!("{stream}.logs");
        let docs = publish_docs(&jetstream_ctx, &subject, 0..10).await;

        let index_id = append_random_suffix("test-nats-source--resume--index");
        let source_config =
            get_source_config(&stream, Vec::new(), NatsSourceDeliverPolicy::All, false);
        // Docs 0 to 4 hold stream sequences 1 to 5 and are behind the
        // checkpoint: only docs 5 to 9 should be indexed.
        let index_uid = setup_index(
            metastore.clone(),
            &index_id,
            &source_config,
            &[(
                PartitionId::from(stream.as_str()),
                Position::Beginning,
                Position::offset(5u64),
            )],
        )
        .await;

        let (source_handle, doc_processor_inbox) =
            create_source_actor(&universe, metastore, index_uid, source_config).await;

        wait_for_processed_messages(&source_handle, 5).await;
        source_handle.quit().await;

        let batches: Vec<RawDocBatch> = doc_processor_inbox.drain_for_test_typed();
        let batch = merge_doc_batches(batches);
        assert_eq!(batch.docs, docs[5..]);

        let mut expected_delta = SourceCheckpointDelta::default();
        expected_delta
            .record_partition_delta(
                PartitionId::from(stream.as_str()),
                Position::offset(5u64),
                Position::offset(10u64),
            )
            .unwrap();
        assert_eq!(batch.checkpoint_delta, expected_delta);

        jetstream_ctx.delete_stream(&stream).await.unwrap();
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_deliver_policy_new() {
        let universe = Universe::with_accelerated_time();
        let metastore = metastore_for_test();
        let stream = append_random_suffix("test-nats-source--deliver-new--stream");
        let jetstream_ctx = setup_nats_stream(&stream).await;

        let subject = format!("{stream}.logs");
        // Published before the consumer exists: must not be indexed.
        publish_docs(&jetstream_ctx, &subject, 0..5).await;

        let index_id = append_random_suffix("test-nats-source--deliver-new--index");
        let source_config =
            get_source_config(&stream, Vec::new(), NatsSourceDeliverPolicy::New, false);
        let index_uid = setup_index(metastore.clone(), &index_id, &source_config, &[]).await;

        let (source_handle, doc_processor_inbox) =
            create_source_actor(&universe, metastore, index_uid, source_config).await;

        let expected_docs = publish_docs(&jetstream_ctx, &subject, 5..10).await;

        wait_for_processed_messages(&source_handle, 5).await;
        source_handle.quit().await;

        let batches: Vec<RawDocBatch> = doc_processor_inbox.drain_for_test_typed();
        let batch = merge_doc_batches(batches);
        assert_eq!(batch.docs, expected_docs);
        assert_eq!(
            batch.checkpoint_delta,
            expected_checkpoint_delta(&stream, 10)
        );

        jetstream_ctx.delete_stream(&stream).await.unwrap();
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_backfill_mode() {
        let universe = Universe::with_accelerated_time();
        let metastore = metastore_for_test();
        let stream = append_random_suffix("test-nats-source--backfill--stream");
        let jetstream_ctx = setup_nats_stream(&stream).await;

        let subject = format!("{stream}.logs");
        let expected_docs = publish_docs(&jetstream_ctx, &subject, 0..5).await;

        let index_id = append_random_suffix("test-nats-source--backfill--index");
        let source_config =
            get_source_config(&stream, Vec::new(), NatsSourceDeliverPolicy::All, true);
        let index_uid = setup_index(metastore.clone(), &index_id, &source_config, &[]).await;

        let (source_handle, doc_processor_inbox) =
            create_source_actor(&universe, metastore, index_uid, source_config).await;

        let (exit_status, exit_state) = source_handle.join().await;
        assert!(exit_status.is_success());
        // The exit condition is the consumer reporting zero pending messages.
        assert_eq!(exit_state.get("num_pending").unwrap().as_u64(), Some(0));

        let batches: Vec<RawDocBatch> = doc_processor_inbox.drain_for_test_typed();
        let batch = merge_doc_batches(batches);
        assert_eq!(batch.docs, expected_docs);
        assert_eq!(
            batch.checkpoint_delta,
            expected_checkpoint_delta(&stream, 5)
        );

        jetstream_ctx.delete_stream(&stream).await.unwrap();
        universe.assert_quit().await;
    }
}
