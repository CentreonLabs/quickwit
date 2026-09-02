---
title: Source configuration
sidebar_position: 5
---

Quickwit can insert data into an index from one or multiple sources.
A source can be added after index creation using the [CLI command](../reference/cli.md#source) `quickwit source create`.
It can also be enabled or disabled with the `quickwit source enable/disable` subcommands.

A source is declared using an object called source config, which defines the source's settings. It consists of multiple parameters:

- source ID
- source type
- source parameters
- input_format
- maximum number of pipelines per indexer (optional)
- desired number of pipelines (optional)
- transform parameters (optional)

## Source ID

The source ID is a string that uniquely identifies the source within an index. It may only contain uppercase or lowercase ASCII letters, digits, hyphens (`-`), and underscores (`_`). Finally, it must start with a letter and contain at least 3 characters but no more than 255.

## Source type

The source type designates the kind of source being configured. As of version 0.5, available source types are `ingest-api`, `kafka`, `kinesis`, and `pulsar`. The `file` type is also supported but only for local ingestion from [the CLI](/docs/reference/cli.md#tool-local-ingest).

## Source parameters

The source parameters indicate how to connect to a data store and are specific to the source type.

### File source

A file source reads data from files containing JSON objects separated by newlines (NDJSON). Gzip compression is supported provided that the file name ends with the `.gz` suffix.

#### Ingest a single file (CLI only)

To ingest a specific file, run the indexing directly in an adhoc CLI process with:

```bash
./quickwit tool local-ingest --index <index> --input-path <input-path>
```

Both local and object files are supported, provided that the environment is configured with the appropriate permissions. A tutorial is available [here](/docs/ingest-data/ingest-local-file.md).

#### Notification based file ingestion (beta)

Quickwit can automatically ingest all new files that are uploaded to an S3 bucket. This requires creating and configuring an [SQS notification queue](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ways-to-add-notification-config-to-bucket.html). A complete example can be found [in this tutorial](/docs/ingest-data/sqs-files.md).


The `notifications` parameter takes an array of notification settings. Currently one notifier can be configured per source and only the SQS notification `type` is supported.

Required fields for the SQS `notifications` parameter items:
- `type`: `sqs`
- `queue_url`: complete URL of the SQS queue (e.g `https://sqs.us-east-1.amazonaws.com/123456789012/queue-name`)
- `message_type`: format of the message payload, either
  - `s3_notification`: an [S3 event notification](https://docs.aws.amazon.com/AmazonS3/latest/userguide/EventNotifications.html)
  - `raw_uri`: a message containing just the file object URI (e.g. `s3://mybucket/mykey`)
  - `deduplication_window_duration_sec`: maximum duration for which ingested files checkpoints are kept (default 3600)
  - `deduplication_window_max_messages`: maximum number of ingested file checkpoints kept (default 100k)
  - `deduplication_cleanup_interval_secs`: frequency at which outdated file checkpoints are cleaned up

*Adding a file source with SQS notifications to an index with the [CLI](../reference/cli.md#source)*

```bash
cat << EOF > source-config.yaml
version: 0.8
source_id: my-sqs-file-source
source_type: file
num_pipelines: 2
params:
  notifications:
    - type: sqs
      queue_url: https://sqs.us-east-1.amazonaws.com/123456789012/queue-name
      message_type: s3_notification
EOF
./quickwit source create --index my-index --source-config source-config.yaml
```

:::note

- Quickwit does not automatically delete the source files after a successful ingestion. You can use [S3 object expiration](https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-expire-general-considerations.html) to configure how long they should be retained in the bucket.
- Configure the notification to only forward events of type `s3:ObjectCreated:*`. Other events are acknowledged by the source without further processing and an warning is logged.
- We strongly recommend using a [dead letter queue](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html) to receive all messages that couldn't be processed by the file source. A `maxReceiveCount` of 5 is a good default value. Here are some common situations where the notification message ends up in the dead letter queue:
  - the notification message could not be parsed (e.g it is not a valid S3 notification)
  - the file was not found
  - the file is corrupted (e.g unexpected compression)
- AWS S3 notifications and AWS SQS provide "at least once" delivery guaranties. To avoid duplicates, the file source includes a mechanism that prevents the same file from being ingested twice. It works by storing checkpoints in the metastore that track the indexing progress for each file. You can decrease `deduplication_window_*` or increase `deduplication_cleanup_interval_secs` to reduce the load on the metastore.

:::

### Ingest API source

An ingest API source reads data from the [Ingest API](/docs/reference/rest-api.md#ingest-data-into-an-index). This source is automatically created at the index creation and cannot be deleted nor disabled.

### Kafka source

A Kafka source reads data from a Kafka stream. Each message in the stream must hold a JSON object.

A tutorial is available [here](/docs/ingest-data/kafka.md).

#### Kafka source parameters

The Kafka source consumes a `topic` using the client library [librdkafka](https://github.com/edenhill/librdkafka) and forwards the key-value pairs carried by the parameter `client_params` to the underlying librdkafka consumer. Common `client_params` options are bootstrap servers (`bootstrap.servers`), or security protocol (`security.protocol`). Please, refer to [Kafka](https://kafka.apache.org/documentation/#consumerconfigs) and [librdkafka](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) documentation pages for more advanced options.

| Property | Description | Default value |
| --- | --- | --- |
| `topic` | Name of the topic to consume. | required |
| `client_log_level` | librdkafka client log level. Possible values are: debug, info, warn, error. | `info` |
| `client_params` | librdkafka client configuration parameters. | `{}` |
| `enable_backfill_mode` | Backfill mode stops the source after reaching the end of the topic. | `false` |

**Kafka client parameters**

- `bootstrap.servers`
Comma-separated list of host and port pairs that are the addresses of a subset of the Kafka brokers in the Kafka cluster.

- `auto.offset.reset`
Defines the behavior of the source when consuming a partition for which there is no initial offset saved in the checkpoint. `earliest` consumes from the beginning of the partition, whereas `latest` (default) consumes from the end.

- `enable.auto.commit`
This setting is ignored because the Kafka source manages commit offsets internally using the [checkpoint API](../overview/concepts/indexing.md#checkpoint) and forces auto-commits to be disabled.

- `group.id`
Kafka-based distributed indexing relies on consumer groups. Unless overridden in the client parameters, the default group ID assigned to each consumer managed by the source is `quickwit-{index_uid}-{source_id}`.

- `max.poll.interval.ms`
Short max poll interval durations may cause a source to crash when back pressure from the indexer occurs. Therefore, Quickwit recommends using the default value of `300000` (5 minutes).

*Adding a Kafka source to an index with the [CLI](../reference/cli.md#source)*

```bash
cat << EOF > source-config.yaml
version: 0.8
source_id: my-kafka-source
source_type: kafka
num_pipelines: 2
params:
  topic: my-topic
  client_params:
    bootstrap.servers: localhost:9092
    security.protocol: SSL
EOF
./quickwit source create --index my-index --source-config source-config.yaml
```

### Kinesis source

A Kinesis source reads data from an [Amazon Kinesis](https://aws.amazon.com/kinesis/) stream. Each message in the stream must hold a JSON object.

A tutorial is available [here](/docs/ingest-data/kinesis.md).

**Kinesis source parameters**

The Kinesis source consumes a stream identified by a `stream_name` and a `region`.

| Property | Description | Default value |
| --- | --- | --- |
| `stream_name` | Name of the stream to consume. | required |
| `region` | The AWS region of the stream. Mutually exclusive with `endpoint`. | `us-east-1` |
| `endpoint` | Custom endpoint for use with AWS-compatible Kinesis service. Mutually exclusive with `region`. | optional |

If no region is specified, Quickwit will attempt to find one in multiple other locations and with the following order of precedence:

1. Environment variables (`AWS_REGION` then `AWS_DEFAULT_REGION`)

2. Config file, typically located at `~/.aws/config` or otherwise specified by the `AWS_CONFIG_FILE` environment variable if set and not empty.

3. Amazon EC2 instance metadata service determining the region of the currently running Amazon EC2 instance.

4. Default value: `us-east-1`

*Adding a Kinesis source to an index with the [CLI](../reference/cli.md#source)*

```bash
cat << EOF > source-config.yaml
version: 0.7
source_id: my-kinesis-source
source_type: kinesis
params:
  stream_name: my-stream
EOF
quickwit source create --index my-index --source-config source-config.yaml
```

### NATS source

A NATS source reads data from a [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream) stream. Each message carries one payload in the source's [input format](#input-format): a single JSON object (`json`, the default), a plain text document (`plain_text`), or an OTLP export request whose log records or spans are each indexed as a separate document (`otlp_*` formats). Payloads must not exceed 1 MiB.

A tutorial is available [here](/docs/ingest-data/nats.md).

The source uses an ephemeral [ordered consumer](https://docs.nats.io/nats-concepts/jetstream/consumers) and tracks its progress with the [checkpoint API](../overview/concepts/indexing.md#checkpoint): the position saved in the metastore is the stream sequence number of the last indexed message, which provides exactly-once indexing without durable consumers or acknowledgments. Two consequences to keep in mind:

- The stream must retain messages (e.g. limits retention with a sufficient `max-age`) long enough to cover any indexing downtime; messages deleted by the retention policy before being indexed are lost. When the source consumes the entire stream (no `subjects` filter), it logs a warning at startup when it detects such a gap. With subject filters, the gap is ambiguous — the deleted messages may not have matched the filters — so the source only logs it at info level.
- No consumer state is stored in NATS while the pipeline is down: the consumer visible in `nats consumer ls`, named `quickwit-{index_id}-{source_id}-{incarnation_id}`, is recreated at each pipeline start and reaped by the server shortly after the pipeline stops.

**Monitoring**

In the default (ordered) mode, the source exports two Prometheus gauges, labeled by `index` and `source` and refreshed every 60 seconds:

- `quickwit_indexing_nats_source_pending_messages`: number of messages matching the subject filters and not yet delivered to the source.
- `quickwit_indexing_nats_source_caught_up_timestamp_seconds`: unix timestamp of the last time the source observed zero pending messages.

Since the ephemeral consumer disappears shortly after a pipeline stops, NATS itself exposes no per-source lag metric while Quickwit is down. The caught-up timestamp covers that case: it stops advancing when the pipeline is down or lagging, so alerting on its staleness (e.g. `time() - quickwit_indexing_nats_source_caught_up_timestamp_seconds` exceeding a fraction of the stream's retention) flags messages at risk of aging out of retention before they are indexed.

These gauges are not exported in durable mode: only a durable consumer can be observed through NATS's own monitoring (`nats consumer info`, exporters), and unlike the ephemeral case, that observability remains available while the pipelines are down.

**Durable mode**

Setting `durable_mode` binds the source to a durable consumer provisioned externally: Quickwit only ever *fetches* the consumer — it never creates, updates, nor deletes it — so its lifecycle, subject filters, deliver policy, and ack tuning (`ack_wait`, `max_ack_pending`) belong to whoever provisioned it. The consumer must use the explicit ack policy.

In this mode, the consumer's ack floor is the resume point instead of the checkpoint: the source holds the pending acknowledgments of delivered messages and releases them once the split containing the messages is published. Delivery is **at-least-once**: messages delivered but not yet published when a pipeline stops are redelivered and indexed again, as duplicates. In exchange, `num_pipelines` can be greater than 1 — the pipelines share the consumer and NATS load-balances the messages across them — so scaling up and down is a plain `num_pipelines` update.

Because they are properties of the pre-provisioned consumer, `subjects`, `deliver_policy`, and `enable_backfill_mode` cannot be set in durable mode, and a source cannot be switched between ordered and durable mode.

**Distributed tracing**

When a message carries a W3C `traceparent` header, the source records a `process_nats_message` span parented on the publisher's trace, stitching the indexing of the message into end-to-end distributed traces. The spans are exported when the node's OpenTelemetry trace exporter is configured (standard `OTEL_EXPORTER_OTLP_*` environment variables). Messages without the header incur no tracing overhead.

**Scaling beyond one pipeline**

A NATS source always runs a single indexing pipeline (`num_pipelines: 1`): the stream is consumed by one ordered consumer, and additional pipelines for the same source would receive the same messages and conflict on the same checkpoint.

To index faster than a single pipeline allows, partition the data and create several NATS sources on the same index. Checkpoints are tracked per source, so the sources do not conflict as long as their `subjects` filters are disjoint, and each source runs its own indexing pipeline, placed across the cluster's indexers by the control plane.

[Deterministic subject token partitioning](https://docs.nats.io/learn/core-nats/subject-mapping#partition-by-a-token) is the natural way to split a stream: a subject mapping such as `logs.* -> logs.{{partition(3, 1)}}.{{wildcard(1)}}` stamps a stable partition number into the subjects, and one source per partition consumes it — three sources filtering `logs.0.>`, `logs.1.>`, and `logs.2.>` respectively index the stream with three pipelines. The same pattern works with data split across multiple streams: create one source per stream.

Two caveats:

- Quickwit cannot validate that the filters of the different sources are disjoint and cover the whole stream: an overlap indexes messages twice, and a hole silently drops a partition.
- The partition count can be changed later: stored messages keep the subject they were published with, so the existing sources and their checkpoints remain valid. However, new messages are remapped across all partitions, so create a source for every token of the new mapping before changing it (or shortly after, within the retention window): a partition token that no source consumes is silently dropped. Per-token ordering is not preserved across the change.

**NATS source parameters**

| Property | Description | Default value |
| --- | --- | --- |
| `uris` | List of NATS server URIs (e.g. `nats://localhost:4222`). | required |
| `stream` | Name of the JetStream stream to consume. | required |
| `subjects` | List of subjects (wildcards allowed) filtering the messages consumed from the stream. When empty, the entire stream is consumed. Filtering on more than one subject requires NATS server 2.10+. | `[]` |
| `deliver_policy` | Where to start consuming when the source has no checkpoint yet: `all` consumes all the retained messages, `new` only messages published after the source first starts, `last` starts with the last message in the stream, `by_start_time` (with an RFC 3339 timestamp) with the first message published at or after that time, and `by_start_sequence` (with a stream sequence) with that sequence, inclusive. Once a checkpoint exists, the source always resumes right after the last indexed message and this parameter is ignored. | `all` |
| `enable_backfill_mode` | Backfill mode stops the source after it caught up with the stream, i.e. once the consumer reports no pending messages. | `false` |
| `durable_mode` | Binds the source to a pre-provisioned durable consumer (`consumer`: its name) instead of an ephemeral one. See the durable mode section below. | optional |
| `tls` | TLS options: `ca_certificates_path` (PEM file whose root certificates are trusted in addition to the system ones), and `client_certificate_path` + `client_key_path` (PEM files, set together) for mutual TLS. TLS itself is enabled by connecting to `tls://` URIs. The files are read by the indexer nodes when the connection is established. | optional |
| `authentication` | Authentication parameters: either `user_password` (with `user` and `password`) or `token`. | optional |

*Adding a NATS source to an index with the [CLI](../reference/cli.md#source)*

```bash
cat << EOF > source-config.yaml
version: 0.8
source_id: my-nats-source
source_type: nats
params:
  uris:
    - nats://localhost:4222
  stream: my-stream
  subjects:
    - logs.>
  deliver_policy: new
EOF
./quickwit source create --index my-index --source-config source-config.yaml
```

### Pulsar source

A Puslar source reads data from one or several Pulsar topics. Each message in topic(s) must hold a JSON object.

A tutorial is available [here](/docs/ingest-data/pulsar.md).

**Pulsar source parameters**

The Pulsar source consumes `topics` using the client library [pulsar-rs](https://github.com/streamnative/pulsar-rs).

| Property | Description | Default value |
| --- | --- | --- |
| `topics` | List of topics to consume. | required |
| `address` | Pulsar URL (pulsar:// and pulsar+ssl://). | required |
| `consumer_name` | The consumer name to register with the pulsar source. | `quickwit` |

*Adding a Pulsar source to an index with the [CLI](../reference/cli.md#source)*

```bash
cat << EOF > source-config.yaml
version: 0.7
source_id: my-pulsar-source
source_type: pulsar
params:
  topics:
    - my-topic
  address: pulsar://localhost:6650
EOF
./quickwit source create --index my-index --source-config source-config.yaml
```

## Number of pipelines

The `num_pipelines` parameter is only available for distributed sources like Kafka, GCP PubSub, and Pulsar.

It defines the number of pipelines to run on a cluster for the source. The actual placement of these pipelines on the different indexer
will be decided by the control plane.

:::info

Note that distributing the indexing load of partitioned sources like Kafka is done by assigning the different partitions to different pipelines. As a result, it is important to ensure that the number of partitions is a multiple of `num_pipelines`.

Also, assuming you are only indexing a single Kafka source in your Quickwit cluster, you should set the number of pipelines to a multiple of the number of indexers. Finally, if your indexing throughput is high, you should provision between 2 and 4 vCPUs per pipeline.

For instance, assume you want to index a 60-partition topic, with each partition receiving a throughput of 10 MB/s. If you measured that Quickwit can index your data at a pace of 40MB/s per pipeline, a possible setting could be:
- 5 indexers with 8 vCPUs each
- 15 pipelines

Each indexer will then be in charge of 3 pipelines, and each pipeline will cover 4 partitions.
:::


## Transform parameters

For all source types but the `ingest-api`, ingested documents can be transformed before being indexed using [Vector Remap Language (VRL)](https://vector.dev/docs/reference/vrl/) scripts.

| Property | Description | Default value |
| --- | --- | --- |
| `script` | Source code of the VRL program executed to transform documents. | required |
| `timezone` | Timezone used in the VRL program for date and time manipulations. It must be a valid name in the [TZ database](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) | `UTC` |

```yaml
# Your source config here
# ...
transform:
  script: |
    .message = downcase(string!(.message))
    .timestamp = now()
    del(.username)
  timezone: local
```

## Input format

The `input_format` parameter specifies the expected data format of the source. The formats currently supported are:
- `json` (default)
- `otlp_logs_json`
- `otlp_logs_proto`
- `otlp_traces_json`
- `otlp_traces_proto`
- `plain_text`

*OTLP formats*

When ingesting OTLP data into an OTLP logs or traces index with a source other than the native OTEL endpoints, use this parameter to specify whether the exported logs or traces will be serialized in JSON or Protobuf. When possible, prefer the latter, which is a more compact encoding.

*Plaint text format*

Use this parameter for unstructured text data. Internally, Quickwit can only index JSON data. To allow the ingestion of plain text documents, Quickwit transform them on the fly into JSON objects of the following form: `{"plain_text": "<original plain text document>"}`. Then, they can be optionally transformed into more complex documents using a VRL script. (see [transform feature](#transform-parameters)).

The following is an example of how one could parse and transform a CSV dataset containing a list of users described by 3 attributes: first name, last name, and age.

```yaml
# Your source config here
# ...
input_format: plain_text
transform:
  script: |
    user = parse_csv!(.plain_text)
    .first_name = user[0]
    .last_name = user[1]
    .age = to_int!(user[2])
    del(.plain_text)
```

## Enabling/disabling a source from an index

A source can be enabled or disabled from an index using the [CLI command](../reference/cli.md) `quickwit source enable` or `quickwit source disable`:

```bash
quickwit source disable --index my-index --source my-source
```

A source is enabled by default. When disabling a source, the related indexing pipelines will be shut down on each relevant indexer and indexing for this source will be paused.

## Deleting a source from an index

A source can be removed from an index using the [CLI command](../reference/cli.md) `quickwit source delete`:

```bash
quickwit source delete --index my-index --source my-source
```

When deleting a source, the checkpoint associated with the source is also removed.
