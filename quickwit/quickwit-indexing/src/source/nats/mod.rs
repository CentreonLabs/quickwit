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
//!
//! With `durable_mode`, the source instead binds to a pre-provisioned durable
//! consumer — only ever fetched, never created nor updated — and acknowledges
//! each message once the split containing it is published: delivery becomes
//! at-least-once, several pipelines can share the consumer, and lag is
//! observed through NATS's own consumer metrics rather than Quickwit gauges.

mod durable;
mod ordered;

use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use anyhow::Context as _;
use async_nats::header::{HeaderMap, HeaderName};
use async_nats::{ConnectOptions, jetstream};
use async_trait::async_trait;
pub use durable::DurableNatsSource;
use opentelemetry::propagation::Extractor;
use opentelemetry::trace::TraceContextExt;
use opentelemetry::{Context as OtelContext, global};
pub use ordered::OrderedNatsSource;
use quickwit_actors::ActorExitStatus;
use quickwit_config::{NatsSourceAuth, NatsSourceParams};
use quickwit_metastore::checkpoint::SourceCheckpoint;
use serde_json::Value as JsonValue;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::source::{Source, SourceContext, SourceRuntime, SourceSink, TypedSourceFactory};

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

/// The two flavors of the source, dispatched on the `durable_mode` parameter:
/// [`OrderedNatsSource`] tracks its progress with checkpoints (exactly-once),
/// [`DurableNatsSource`] binds to a pre-provisioned durable consumer and
/// acknowledges published messages (at-least-once).
pub enum NatsSource {
    Ordered(OrderedNatsSource),
    Durable(DurableNatsSource),
}

impl NatsSource {
    pub async fn try_new(
        source_runtime: SourceRuntime,
        source_params: NatsSourceParams,
    ) -> anyhow::Result<Self> {
        if let Some(durable_mode) = source_params.durable_mode.clone() {
            let source =
                DurableNatsSource::try_new(source_runtime, source_params, durable_mode).await?;
            Ok(NatsSource::Durable(source))
        } else {
            let source = OrderedNatsSource::try_new(source_runtime, source_params).await?;
            Ok(NatsSource::Ordered(source))
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
        match self {
            NatsSource::Ordered(source) => source.emit_batches(source_sink, ctx).await,
            NatsSource::Durable(source) => source.emit_batches(source_sink, ctx).await,
        }
    }

    async fn suggest_truncate(
        &mut self,
        checkpoint: SourceCheckpoint,
        ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        match self {
            NatsSource::Ordered(source) => source.suggest_truncate(checkpoint, ctx).await,
            NatsSource::Durable(source) => source.suggest_truncate(checkpoint, ctx).await,
        }
    }

    async fn finalize(
        &mut self,
        exit_status: &ActorExitStatus,
        ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        match self {
            NatsSource::Ordered(source) => source.finalize(exit_status, ctx).await,
            NatsSource::Durable(source) => source.finalize(exit_status, ctx).await,
        }
    }

    fn name(&self) -> String {
        match self {
            NatsSource::Ordered(source) => source.name(),
            NatsSource::Durable(source) => source.name(),
        }
    }

    fn observable_state(&self) -> JsonValue {
        match self {
            NatsSource::Ordered(source) => source.observable_state(),
            NatsSource::Durable(source) => source.observable_state(),
        }
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

/// Builds a span parented on the publisher's trace when the message carries
/// a W3C `traceparent` header, stitching the processing of the message into
/// the publisher's distributed trace. Messages without a propagated context
/// cost nothing: no span is created.
///
/// The span ends once the message is added to the batch: documents are
/// batched downstream, so the per-message trace context stops here.
fn remote_parented_span(
    message: &jetstream::Message,
    stream_sequence: u64,
    source_runtime: &SourceRuntime,
) -> Option<Span> {
    let headers = message.headers.as_ref()?;
    let parent_context = extract_remote_context(headers);
    if !parent_context.span().span_context().is_valid() {
        return None;
    }
    let span = tracing::info_span!(
        "process_nats_message",
        index_id = %source_runtime.index_id(),
        source_id = %source_runtime.source_id(),
        subject = %message.subject,
        stream_sequence,
    );
    let _ = span.set_parent(parent_context);
    Some(span)
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
    if let Some(tls) = &params.tls {
        if let Some(ca_certificates_path) = &tls.ca_certificates_path {
            connect_options =
                connect_options.add_root_certificates(PathBuf::from(ca_certificates_path));
        }
        if let (Some(certificate_path), Some(key_path)) =
            (&tls.client_certificate_path, &tls.client_key_path)
        {
            connect_options = connect_options
                .add_client_certificate(PathBuf::from(certificate_path), PathBuf::from(key_path));
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
/// stream and, in durable mode, the pre-provisioned consumer.
pub(crate) async fn check_connectivity(params: &NatsSourceParams) -> anyhow::Result<()> {
    let client = connect_nats(params).await?;
    let jetstream_ctx = jetstream::new(client);
    let jetstream_stream = jetstream_ctx
        .get_stream(&params.stream)
        .await
        .with_context(|| format!("failed to find NATS JetStream stream `{}`", params.stream))?;
    if let Some(durable_mode) = &params.durable_mode {
        durable::fetch_durable_consumer(&jetstream_stream, &durable_mode.consumer).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
}

/// Helpers shared by the broker tests of the two source flavors.
#[cfg(all(test, feature = "nats-broker-tests"))]
pub(crate) mod broker_test_helpers {
    use std::ops::Range;
    use std::time::Duration;

    use async_nats::header::HeaderMap;
    use async_nats::jetstream;
    use quickwit_actors::{ActorHandle, Inbox, Universe};
    use quickwit_config::SourceConfig;
    use quickwit_proto::metastore::MetastoreServiceClient;
    use quickwit_proto::types::IndexUid;
    use serde_json::json;

    use crate::actors::DocProcessor;
    use crate::models::RawDocBatch;
    use crate::source::tests::SourceRuntimeBuilder;
    use crate::source::{SourceActor, quickwit_supported_sources};

    pub(super) static NATS_URI: &str = "nats://localhost:4222";

    pub(super) async fn setup_nats_stream(stream_name: &str) -> jetstream::Context {
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
    pub(super) async fn publish_docs(
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

    pub(super) async fn create_source_actor(
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

    pub(super) async fn wait_for_processed_messages(
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

    pub(super) fn merge_doc_batches(batches: Vec<RawDocBatch>) -> RawDocBatch {
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
}
