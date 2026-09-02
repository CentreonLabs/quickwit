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

//! Durable flavor of the NATS source: binds to a pre-provisioned durable
//! consumer and acknowledges messages once they are published. See the module
//! documentation of [`super`] for the full picture.

use std::collections::BTreeMap;
use std::fmt;
use std::time::{Duration, Instant};

use anyhow::{Context as _, anyhow, bail, ensure};
use async_nats::jetstream::consumer::pull::Stream as DurableMessageStream;
use async_nats::jetstream::consumer::{AckPolicy, PullConsumer};
use async_nats::{Subject, jetstream};
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use quickwit_actors::ActorExitStatus;
use quickwit_common::rand::append_random_suffix;
use quickwit_config::{NatsSourceDurableMode, NatsSourceParams};
use quickwit_metastore::checkpoint::{PartitionId, SourceCheckpoint};
use quickwit_proto::metastore::SourceType;
use quickwit_proto::types::Position;
use serde_json::{Value as JsonValue, json};
use tokio::time;
use tracing::{Span, debug, info, warn};

use super::{connect_nats, remote_parented_span};
use crate::source::{
    BATCH_NUM_BYTES_LIMIT, BatchBuilder, EMIT_BATCHES_TIMEOUT, Source, SourceContext,
    SourceRuntime, SourceSink,
};

#[derive(Default, Debug)]
pub struct DurableNatsSourceState {
    /// Number of bytes processed by the source.
    pub num_bytes_processed: u64,
    /// Number of messages processed by the source (including invalid messages).
    pub num_messages_processed: u64,
    /// Number of invalid messages, i.e., that were empty.
    pub num_invalid_messages: u64,
}

/// Source flavor binding to a pre-provisioned durable consumer.
///
/// The consumer is only ever fetched — never created, updated, nor deleted —
/// so its lifecycle, subject filters, deliver policy, and ack tuning
/// (`ack_wait`, `max_ack_pending`) belong to whoever provisioned it. Each
/// message is acknowledged once the split containing it is published
/// ([`Source::suggest_truncate`]), which makes delivery at-least-once: after
/// a crash, messages delivered but not yet published are redelivered and
/// indexed again.
///
/// The metastore checkpoint only carries synthetic positions (a per-pipeline
/// partition and a delivery counter) used to correlate published batches with
/// the acknowledgments they release; the resume point is the consumer's ack
/// floor, not the checkpoint.
pub struct DurableNatsSource {
    source_runtime: SourceRuntime,
    source_params: NatsSourceParams,
    // Kept around to send the acks and drain the connection on finalize.
    nats_client: async_nats::Client,
    message_stream: DurableMessageStream,
    consumer_name: String,
    partition_id: PartitionId,
    current_position: Position,
    delivery_counter: u64,
    /// Ack subjects of the messages delivered but not published yet, keyed by
    /// delivery counter. Bounded by the consumer's `max_ack_pending`: the
    /// server stops delivering when too many messages are unacknowledged.
    pending_acks: BTreeMap<u64, Subject>,
    state: DurableNatsSourceState,
}

impl fmt::Debug for DurableNatsSource {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("DurableNatsSource")
            .field("index_uid", self.source_runtime.index_uid())
            .field("source_id", &self.source_runtime.source_id())
            .field("stream", &self.source_params.stream)
            .field("consumer_name", &self.consumer_name)
            .finish()
    }
}

impl DurableNatsSource {
    pub async fn try_new(
        source_runtime: SourceRuntime,
        source_params: NatsSourceParams,
        durable_mode: NatsSourceDurableMode,
    ) -> anyhow::Result<Self> {
        let consumer_name = durable_mode.consumer;

        info!(
            index_id=%source_runtime.index_id(),
            source_id=%source_runtime.source_id(),
            stream=%source_params.stream,
            %consumer_name,
            "starting NATS source (durable mode)"
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
        let consumer = fetch_durable_consumer(&jetstream_stream, &consumer_name).await?;
        let message_stream = consumer
            .messages()
            .await
            .context("failed to subscribe to NATS consumer messages")?;

        // A fresh partition per pipeline: durable-mode positions are delivery
        // counters local to this instance, and pipelines sharing the consumer
        // must not collide on a partition.
        let partition_id =
            PartitionId::from(append_random_suffix(&format!("nats-{consumer_name}")));

        Ok(DurableNatsSource {
            source_runtime,
            source_params,
            nats_client,
            message_stream,
            consumer_name,
            partition_id,
            current_position: Position::Beginning,
            delivery_counter: 0,
            pending_acks: BTreeMap::new(),
            state: DurableNatsSourceState::default(),
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
        let _span_guard = remote_parented_span(&message, stream_sequence, &self.source_runtime)
            .map(Span::entered);
        let Some(ack_subject) = message.message.reply else {
            bail!("NATS message carries no reply subject to acknowledge it on");
        };
        let doc = message.message.payload;
        let num_bytes = doc.len() as u64;

        if doc.is_empty() {
            warn!("message received from NATS was empty");
            self.state.num_invalid_messages += 1;
        } else {
            batch.add_doc(doc);
        }
        self.delivery_counter += 1;
        let to_position = Position::offset(self.delivery_counter);
        batch
            .checkpoint_delta
            .record_partition_delta(
                self.partition_id.clone(),
                self.current_position.clone(),
                to_position.clone(),
            )
            .context("failed to record partition delta")?;
        self.current_position = to_position;
        self.pending_acks.insert(self.delivery_counter, ack_subject);

        self.state.num_bytes_processed += num_bytes;
        self.state.num_messages_processed += 1;

        Ok(())
    }
}

#[async_trait]
impl Source for DurableNatsSource {
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

        if !batch_builder.checkpoint_delta.is_empty() {
            debug!(
                num_docs=%batch_builder.docs.len(),
                num_bytes=%batch_builder.num_bytes,
                num_millis=%now.elapsed().as_millis(),
                "sending doc batch to indexer"
            );
            let message = batch_builder.build();
            source_sink.send_raw_doc_batch(message, ctx).await?;
        }
        Ok(Duration::default())
    }

    async fn suggest_truncate(
        &mut self,
        checkpoint: SourceCheckpoint,
        _ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        let Some(position) = checkpoint.position_for_partition(&self.partition_id) else {
            return Ok(());
        };
        let Some(published_up_to) = position.as_u64() else {
            return Ok(());
        };
        let still_pending = self.pending_acks.split_off(&(published_up_to + 1));
        let acks = std::mem::replace(&mut self.pending_acks, still_pending);
        if acks.is_empty() {
            return Ok(());
        }
        let num_acks = acks.len();
        for (_, ack_subject) in acks {
            // Mirrors `jetstream::Message::ack()`: an empty payload published
            // to the reply subject acknowledges the message. A failed ack is
            // simply redelivered after the consumer's `ack_wait`.
            if let Err(error) = self.nats_client.publish(ack_subject, Bytes::new()).await {
                warn!(%error, "failed to ack NATS message");
            }
        }
        self.nats_client
            .flush()
            .await
            .context("failed to flush acks")?;
        debug!(num_acks, "acked published messages");
        Ok(())
    }

    fn name(&self) -> String {
        format!("{self:?}")
    }

    async fn finalize(
        &mut self,
        _exit_status: &ActorExitStatus,
        _ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        // Messages delivered but not acked are redelivered after the
        // consumer's `ack_wait`; the durable consumer itself is left
        // untouched.
        self.nats_client.drain().await?;
        Ok(())
    }

    fn observable_state(&self) -> JsonValue {
        json!({
            "index_id": self.source_runtime.index_id(),
            "source_id": self.source_runtime.source_id(),
            "stream": self.source_params.stream,
            "consumer_name": self.consumer_name,
            "num_bytes_processed": self.state.num_bytes_processed,
            "num_messages_processed": self.state.num_messages_processed,
            "num_invalid_messages": self.state.num_invalid_messages,
            "num_pending_acks": self.pending_acks.len(),
        })
    }
}

/// Fetches the pre-provisioned durable consumer. Read-only by contract: the
/// consumer is never created nor updated.
pub(super) async fn fetch_durable_consumer(
    jetstream_stream: &jetstream::stream::Stream,
    consumer_name: &str,
) -> anyhow::Result<PullConsumer> {
    let consumer: PullConsumer = jetstream_stream
        .get_consumer(consumer_name)
        .await
        .map_err(|error| anyhow!("failed to find NATS consumer `{consumer_name}`: {error}"))?;
    let ack_policy = consumer.cached_info().config.ack_policy;
    ensure!(
        ack_policy == AckPolicy::Explicit,
        "NATS consumer `{consumer_name}` must use the explicit ack policy, got `{ack_policy:?}`"
    );
    Ok(consumer)
}

#[cfg(all(test, feature = "nats-broker-tests"))]
mod nats_broker_tests {
    use std::num::NonZeroUsize;

    use quickwit_actors::Universe;
    use quickwit_common::rand::append_random_suffix;
    use quickwit_config::{NatsSourceDeliverPolicy, SourceConfig, SourceInputFormat, SourceParams};
    use quickwit_metastore::metastore_for_test;

    use super::*;
    use crate::models::RawDocBatch;
    use crate::source::nats::broker_test_helpers::*;
    use crate::source::test_setup_helper::setup_index;
    use crate::source::tests::SourceRuntimeBuilder;
    use crate::source::{SuggestTruncate, quickwit_supported_sources};

    fn get_durable_source_config(stream: &str, consumer: &str) -> SourceConfig {
        let source_id = append_random_suffix("test-nats-source--durable-source");
        SourceConfig {
            source_id,
            num_pipelines: NonZeroUsize::MIN,
            enabled: true,
            source_params: SourceParams::Nats(NatsSourceParams {
                uris: vec![NATS_URI.to_string()],
                stream: stream.to_string(),
                subjects: Vec::new(),
                deliver_policy: NatsSourceDeliverPolicy::All,
                enable_backfill_mode: false,
                durable_mode: Some(NatsSourceDurableMode {
                    consumer: consumer.to_string(),
                }),
                tls: None,
                authentication: None,
            }),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        }
    }

    /// Provisions the durable consumer the way an operator would: the source
    /// itself only ever fetches it.
    async fn provision_durable_consumer(
        jetstream_ctx: &jetstream::Context,
        stream: &str,
        consumer_name: &str,
    ) {
        jetstream_ctx
            .create_consumer_on_stream(
                jetstream::consumer::pull::Config {
                    name: Some(consumer_name.to_string()),
                    durable_name: Some(consumer_name.to_string()),
                    ack_policy: AckPolicy::Explicit,
                    // Long enough for the tests to never hit a redelivery.
                    ack_wait: Duration::from_secs(300),
                    ..Default::default()
                },
                stream,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_durable_mode_ingestion_and_ack() {
        let universe = Universe::with_accelerated_time();
        let metastore = metastore_for_test();
        let stream = append_random_suffix("test-nats-source--durable--stream");
        let jetstream_ctx = setup_nats_stream(&stream).await;
        let consumer_name = "durable-ack-consumer";
        provision_durable_consumer(&jetstream_ctx, &stream, consumer_name).await;

        let subject = format!("{stream}.logs");
        let expected_docs = publish_docs(&jetstream_ctx, &subject, 0..10).await;

        let index_id = append_random_suffix("test-nats-source--durable--index");
        let source_config = get_durable_source_config(&stream, consumer_name);
        let index_uid = setup_index(metastore.clone(), &index_id, &source_config, &[]).await;

        let (source_handle, doc_processor_inbox) =
            create_source_actor(&universe, metastore, index_uid, source_config).await;

        wait_for_processed_messages(&source_handle, 10).await;

        let batches: Vec<RawDocBatch> = doc_processor_inbox.drain_for_test_typed();
        let batch = merge_doc_batches(batches);
        assert_eq!(batch.docs, expected_docs);
        // Positions are synthetic delivery counters on a per-pipeline
        // partition, not stream sequences.
        assert_eq!(batch.checkpoint_delta.num_partitions(), 1);

        // A `SuggestTruncate` simulates the split publication notification:
        // it must release the acks of the published messages.
        let checkpoint = batch.checkpoint_delta.get_source_checkpoint();
        source_handle
            .mailbox()
            .send_message(SuggestTruncate(checkpoint))
            .await
            .unwrap();

        let mut consumer: PullConsumer = jetstream_ctx
            .get_consumer_from_stream(consumer_name, stream.as_str())
            .await
            .unwrap();
        let mut acked = false;
        for _ in 0..100 {
            let consumer_info = consumer.info().await.unwrap();
            if consumer_info.num_ack_pending == 0 {
                assert_eq!(consumer_info.ack_floor.stream_sequence, 10);
                acked = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(acked, "messages should be acked after truncation");

        source_handle.quit().await;
        jetstream_ctx.delete_stream(&stream).await.unwrap();
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_durable_mode_load_balancing() {
        let universe = Universe::with_accelerated_time();
        let metastore = metastore_for_test();
        let stream = append_random_suffix("test-nats-source--durable-lb--stream");
        let jetstream_ctx = setup_nats_stream(&stream).await;
        let consumer_name = "durable-lb-consumer";
        provision_durable_consumer(&jetstream_ctx, &stream, consumer_name).await;

        let index_id = append_random_suffix("test-nats-source--durable-lb--index");
        let source_config = get_durable_source_config(&stream, consumer_name);
        let index_uid = setup_index(metastore.clone(), &index_id, &source_config, &[]).await;

        let (source_handle_1, doc_processor_inbox_1) = create_source_actor(
            &universe,
            metastore.clone(),
            index_uid.clone(),
            source_config.clone(),
        )
        .await;
        let (source_handle_2, doc_processor_inbox_2) =
            create_source_actor(&universe, metastore, index_uid, source_config).await;

        let subject = format!("{stream}.logs");
        let mut expected_docs = publish_docs(&jetstream_ctx, &subject, 0..20).await;
        expected_docs.sort();

        // Work-queue delivery splits the messages between the two pipelines
        // in some arbitrary way; together they must cover all of them
        // exactly once.
        loop {
            let num_processed_1 = source_handle_1
                .observe()
                .await
                .state
                .get("num_messages_processed")
                .unwrap()
                .as_u64()
                .unwrap();
            let num_processed_2 = source_handle_2
                .observe()
                .await
                .state
                .get("num_messages_processed")
                .unwrap()
                .as_u64()
                .unwrap();
            if num_processed_1 + num_processed_2 >= 20 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        source_handle_1.quit().await;
        source_handle_2.quit().await;

        let mut all_docs: Vec<Bytes> = Vec::new();
        for batch in doc_processor_inbox_1
            .drain_for_test_typed::<RawDocBatch>()
            .into_iter()
            .chain(doc_processor_inbox_2.drain_for_test_typed::<RawDocBatch>())
        {
            all_docs.extend(batch.docs);
        }
        all_docs.sort();
        assert_eq!(all_docs, expected_docs);

        jetstream_ctx.delete_stream(&stream).await.unwrap();
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_durable_mode_missing_consumer() {
        let metastore = metastore_for_test();
        let stream = append_random_suffix("test-nats-source--durable-missing--stream");
        let jetstream_ctx = setup_nats_stream(&stream).await;

        let index_id = append_random_suffix("test-nats-source--durable-missing--index");
        let source_config = get_durable_source_config(&stream, "does-not-exist");
        let index_uid = setup_index(metastore.clone(), &index_id, &source_config, &[]).await;

        let source_runtime = SourceRuntimeBuilder::new(index_uid, source_config)
            .with_metastore(metastore)
            .build();
        let load_source_result = quickwit_supported_sources()
            .load_source(source_runtime)
            .await;
        assert!(
            load_source_result.is_err(),
            "binding to a missing durable consumer should fail"
        );

        jetstream_ctx.delete_stream(&stream).await.unwrap();
    }
}
