# NATS vs Kafka log injection bench

Spins up a single-node NATS (JetStream, file storage) and a single-node Kafka
(KRaft), and injects a target volume of synthetic JSON log lines (~1 KiB each,
spread over 16 tenants) into one JetStream stream (`logs`, subjects
`logs.{tenant}`) and one Kafka topic (`logs`, keyed by tenant). The same
generator feeds both, so the corpora are comparable — e.g. to compare the
Quickwit NATS and Kafka sources on identical data.

Disk budget for the default 5 GiB run: ~12 GiB across the two Docker volumes.

## Usage

```bash
cd bench/nats-kafka-logs

# Start the brokers.
docker compose up -d nats kafka

# Inject 5 GiB into each (run one or both; sequential is gentler on disk I/O).
docker compose run --rm inject-nats
docker compose run --rm inject-kafka

# Smaller run, e.g. 100 MiB:
TARGET_GB=0.1 docker compose run --rm inject-nats
```

Tunables (env vars): `TARGET_GB` (default 5), `MSG_BYTES` (~line size,
default 1024), `NUM_TENANTS` (default 16), `KAFKA_PARTITIONS` (default 1).

## Verify

```bash
# NATS: message and byte counts of the stream.
docker run --rm --network nats-kafka-logs-bench_default natsio/nats-box \
  nats -s nats://nats:4222 stream info logs

# Kafka: end offsets (sum = message count).
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic logs
```

## Point Quickwit at it

Brokers are exposed on the host: NATS on `localhost:4222`, Kafka on
`localhost:9092`. Note the quickwit repo's own dev `docker-compose.yml` uses
the same ports — don't run both at once.

```yaml
# nats-source.yaml
version: 0.8
source_id: bench-nats-source
source_type: nats
params:
  uris: [nats://localhost:4222]
  stream: logs
```

```yaml
# kafka-source.yaml
version: 0.8
source_id: bench-kafka-source
source_type: kafka
params:
  topic: logs
  client_params:
    bootstrap.servers: localhost:9092
    auto.offset.reset: earliest
```

## Tear down

```bash
docker compose down -v   # -v deletes the ~12 GiB of broker data
```
