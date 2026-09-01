#!/usr/bin/env python3
"""Injects a target volume of synthetic JSON log lines into NATS JetStream or Kafka.

The same generator feeds both targets so the ingested corpora are comparable:
one JSON log document per message, ~MSG_BYTES each, spread over NUM_TENANTS
tenants (NATS subject `logs.{tenant}` / Kafka message key `{tenant}`).
"""

import asyncio
import json
import os
import random
import sys
import time
from datetime import datetime, timezone

TARGET = os.environ.get("TARGET", "nats")
TARGET_BYTES = int(float(os.environ.get("TARGET_GB", "5")) * 1024**3)
MSG_BYTES = int(os.environ.get("MSG_BYTES", "1024"))
NUM_TENANTS = int(os.environ.get("NUM_TENANTS", "16"))
PIPELINE = int(os.environ.get("PIPELINE", "2048"))
REPORT_EVERY_BYTES = 256 * 1024**2

NATS_URL = os.environ.get("NATS_URL", "nats://nats:4222")
NATS_STREAM = os.environ.get("NATS_STREAM", "logs")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "logs")
KAFKA_PARTITIONS = int(os.environ.get("KAFKA_PARTITIONS", "1"))

LEVELS = ["DEBUG", "INFO", "INFO", "INFO", "WARN", "ERROR"]
SERVICES = ["api-gateway", "auth", "billing", "checkout", "search", "worker"]
WORDS = (
    "request completed connection refused retry timeout upstream cache miss hit "
    "user login token refresh queue drained batch flushed shard rebalance failed "
    "latency spike gc pause disk pressure backoff circuit open closed recovered"
).split()

random.seed(42)

# Pre-generated message bodies: cheap to emit, varied enough to be log-shaped.
_PAD_BYTES = max(16, MSG_BYTES - 220)
_POOL = [
    " ".join(random.choices(WORDS, k=_PAD_BYTES // 6))[:_PAD_BYTES]
    for _ in range(4096)
]


def log_line(seq: int) -> tuple[str, bytes]:
    tenant = f"tenant-{seq % NUM_TENANTS}"
    doc = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "level": LEVELS[seq % len(LEVELS)],
        "service": SERVICES[seq % len(SERVICES)],
        "tenant": tenant,
        "seq": seq,
        "trace_id": f"{seq:032x}",
        "message": _POOL[seq % len(_POOL)],
    }
    return tenant, json.dumps(doc, separators=(",", ":")).encode()


class Reporter:
    def __init__(self) -> None:
        self.started = time.monotonic()
        self.last_report = 0

    def maybe_report(self, sent_bytes: int, num_messages: int) -> None:
        if sent_bytes - self.last_report < REPORT_EVERY_BYTES:
            return
        self.last_report = sent_bytes
        elapsed = time.monotonic() - self.started
        rate = sent_bytes / 1024**2 / max(elapsed, 0.001)
        print(
            f"{sent_bytes / 1024**3:.2f} GiB / {TARGET_BYTES / 1024**3:.2f} GiB "
            f"({num_messages} messages, {rate:.0f} MiB/s)",
            flush=True,
        )

    def final(self, sent_bytes: int, num_messages: int) -> None:
        elapsed = time.monotonic() - self.started
        print(
            f"done: {sent_bytes / 1024**3:.2f} GiB in {num_messages} messages "
            f"in {elapsed:.0f}s ({sent_bytes / 1024**2 / max(elapsed, 0.001):.0f} MiB/s)",
            flush=True,
        )


async def inject_nats() -> None:
    import nats
    from nats.js.api import StorageType, StreamConfig

    client = await nats.connect(NATS_URL)
    jetstream = client.jetstream()
    try:
        await jetstream.add_stream(
            StreamConfig(
                name=NATS_STREAM,
                subjects=[f"{NATS_STREAM}.>"],
                storage=StorageType.FILE,
            )
        )
        print(f"created stream `{NATS_STREAM}`", flush=True)
    except Exception as exc:
        print(f"stream `{NATS_STREAM}` not created (already exists?): {exc}", flush=True)

    reporter = Reporter()
    sent_bytes = 0
    seq = 0
    while sent_bytes < TARGET_BYTES:
        publishes = []
        while len(publishes) < PIPELINE and sent_bytes < TARGET_BYTES:
            tenant, payload = log_line(seq)
            publishes.append(jetstream.publish(f"{NATS_STREAM}.{tenant}", payload))
            sent_bytes += len(payload)
            seq += 1
        await asyncio.gather(*publishes)
        reporter.maybe_report(sent_bytes, seq)
    reporter.final(sent_bytes, seq)
    await client.drain()


def inject_kafka() -> None:
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    creations = admin.create_topics(
        [NewTopic(KAFKA_TOPIC, num_partitions=KAFKA_PARTITIONS, replication_factor=1)]
    )
    for topic, creation in creations.items():
        try:
            creation.result()
            print(f"created topic `{topic}` ({KAFKA_PARTITIONS} partitions)", flush=True)
        except Exception as exc:
            print(f"topic `{topic}` not created (already exists?): {exc}", flush=True)

    errors = 0

    def on_delivery(err, _msg) -> None:
        nonlocal errors
        if err is not None:
            errors += 1

    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "linger.ms": 20,
            "batch.size": 1_000_000,
            "acks": "1",
        }
    )
    reporter = Reporter()
    sent_bytes = 0
    seq = 0
    while sent_bytes < TARGET_BYTES:
        tenant, payload = log_line(seq)
        while True:
            try:
                producer.produce(KAFKA_TOPIC, key=tenant, value=payload, on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.1)
        producer.poll(0)
        sent_bytes += len(payload)
        seq += 1
        reporter.maybe_report(sent_bytes, seq)
    producer.flush()
    reporter.final(sent_bytes, seq)
    if errors:
        print(f"{errors} messages failed to be delivered", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    print(
        f"injecting {TARGET_BYTES / 1024**3:.2f} GiB of ~{MSG_BYTES}B log lines "
        f"across {NUM_TENANTS} tenants into {TARGET}",
        flush=True,
    )
    if TARGET == "nats":
        asyncio.run(inject_nats())
    elif TARGET == "kafka":
        inject_kafka()
    else:
        sys.exit(f"unknown TARGET `{TARGET}`, expected `nats` or `kafka`")
