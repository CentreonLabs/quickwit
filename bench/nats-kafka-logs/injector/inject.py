#!/usr/bin/env python3
"""Injects a fixed number of synthetic JSON log lines into NATS JetStream or Kafka.

The payload of a message is a pure function of its sequence number, so the two
corpora are byte-for-byte identical and the NATS and Kafka sources of Quickwit
can be compared on the same data. Messages are spread over `NUM_TENANTS`
tenants: NATS subject `logs.{tenant}`, Kafka partition `seq % KAFKA_PARTITIONS`
(with the tenant as message key). With `NUM_TENANTS == KAFKA_PARTITIONS`, a
Kafka partition holds exactly the messages of a NATS subject.

Injection is spread over `WORKERS` processes, each owning a contiguous range of
sequence numbers, because a single Python process saturates well below what
either broker accepts.

NATS throughput hinges on not waiting for a JetStream ack per message: messages
are published on core NATS (no round-trip) and every `ACK_EVERY` messages one is
published through JetStream instead, which both flushes the socket and confirms
persistence up to that point. That bounds the in-flight window without paying a
round-trip per message.
"""

import multiprocessing
import os
import random
import sys
import time
from datetime import datetime, timezone

TARGET = os.environ.get("TARGET", "nats")
# 5 * 1024^2 messages of ~1 KiB is ~5 GiB.
NUM_MESSAGES = int(os.environ.get("NUM_MESSAGES", 5 * 1024**2))
MSG_BYTES = int(os.environ.get("MSG_BYTES", "1024"))
NUM_TENANTS = int(os.environ.get("NUM_TENANTS", "16"))
WORKERS = int(os.environ.get("WORKERS", "8"))
ACK_EVERY = int(os.environ.get("ACK_EVERY", "1000"))
# Optional cap on the aggregate injection rate, in MiB/s. Injection speed is not
# what this bench measures, and a burst well above what the store behind the
# broker can absorb stalls the broker: a 5 GiB unpaced run peaked at 180 MiB/s
# into the page cache, then every JetStream ack timed out once writeback had to
# keep up. 0 disables the cap (fine when the store is tmpfs).
RATE_MIB_S = float(os.environ.get("RATE_MIB_S", "0"))
# Interval between the timestamps of two consecutive messages. The default
# spreads the corpus over ~23 h of log time.
TS_STEP_MS = int(os.environ.get("TS_STEP_MS", "16"))
# Fixed origin, so that the corpus does not depend on the injection date.
BASE_MS = int(os.environ.get("BASE_MS", "1767225600000"))  # 2026-01-01T00:00:00Z

NATS_URL = os.environ.get("NATS_URL", "nats://nats:4222")
NATS_STREAM = os.environ.get("NATS_STREAM", "logs")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "logs")
KAFKA_PARTITIONS = int(os.environ.get("KAFKA_PARTITIONS", "16"))

LEVELS = ["DEBUG", "INFO", "INFO", "INFO", "WARN", "ERROR"]
SERVICES = ["api-gateway", "auth", "billing", "checkout", "search", "worker"]
WORDS = (
    "request completed connection refused retry timeout upstream cache miss hit "
    "user login token refresh queue drained batch flushed shard rebalance failed "
    "latency spike gc pause disk pressure backoff circuit open closed recovered"
).split()

# Pre-generated message bodies: cheap to emit, varied enough to be log-shaped.
# Seeded so that every worker builds the same pool.
_PAD_BYTES = max(16, MSG_BYTES - 220)
# Prime, so that it stays coprime with any tenant count. With a pool size
# sharing factors with NUM_TENANTS (4096 and 16 did), tenant n only ever draws
# from pool indices congruent to n, so one shard sees a small fraction of the
# bodies. Kafka delivers per-partition batches and NATS delivers in stream
# order, so that correlation made Kafka's docstore blocks far more compressible
# than NATS's on the same corpus (0.13 vs 0.19 of raw) and put the two sources
# on measurably different work per byte.
_POOL_SIZE = 4093
_POOL = None


def build_pool():
    generator = random.Random(42)
    return [
        " ".join(generator.choices(WORDS, k=_PAD_BYTES // 6))[:_PAD_BYTES]
        for _ in range(_POOL_SIZE)
    ]


class Pacer:
    """Holds a target message rate by sleeping, checked every `check_every` messages."""

    def __init__(self, messages_per_second: float, check_every: int = 200) -> None:
        self.interval = 1.0 / messages_per_second if messages_per_second > 0 else 0.0
        self.check_every = check_every
        self.started = time.monotonic()
        self.sent = 0

    def due_sleep(self) -> float:
        """Seconds to sleep to stay on target; 0 when uncapped or not yet due."""
        self.sent += 1
        if self.interval == 0.0 or self.sent % self.check_every != 0:
            return 0.0
        target = self.started + self.sent * self.interval
        return max(0.0, target - time.monotonic())


def worker_rate() -> float:
    """Target messages per second for one worker, 0 when uncapped."""
    if RATE_MIB_S <= 0:
        return 0.0
    return RATE_MIB_S * 1024**2 / MSG_BYTES / WORKERS


class TimestampFormatter:
    """Formats epoch milliseconds as RFC 3339, caching the whole-second part.

    Consecutive messages are milliseconds apart, so the cache hits almost
    always and the (comparatively expensive) calendar conversion runs once per
    second of log time instead of once per message.
    """

    def __init__(self) -> None:
        self._cached_second = -1
        self._cached_prefix = ""

    def format(self, epoch_millis: int) -> str:
        seconds, millis = divmod(epoch_millis, 1000)
        if seconds != self._cached_second:
            self._cached_second = seconds
            self._cached_prefix = time.strftime(
                "%Y-%m-%dT%H:%M:%S", time.gmtime(seconds)
            )
        return f"{self._cached_prefix}.{millis:03d}Z"


def log_line(seq: int, timestamps: TimestampFormatter) -> tuple[str, bytes]:
    """Builds the message of sequence number `seq`. Deterministic."""
    tenant = f"tenant-{seq % NUM_TENANTS}"
    timestamp = timestamps.format(BASE_MS + seq * TS_STEP_MS)
    document = (
        f'{{"timestamp":"{timestamp}",'
        f'"level":"{LEVELS[seq % len(LEVELS)]}",'
        f'"service":"{SERVICES[seq % len(SERVICES)]}",'
        f'"tenant":"{tenant}",'
        f'"seq":{seq},'
        f'"trace_id":"{seq:032x}",'
        f'"message":"{_POOL[seq % _POOL_SIZE]}"}}'
    )
    return tenant, document.encode()


def worker_range(worker_id: int) -> tuple[int, int]:
    """Contiguous slice of the sequence space owned by a worker."""
    chunk = NUM_MESSAGES // WORKERS
    remainder = NUM_MESSAGES % WORKERS
    start = worker_id * chunk + min(worker_id, remainder)
    length = chunk + (1 if worker_id < remainder else 0)
    return start, start + length


# ---------------------------------------------------------------------------
# NATS
# ---------------------------------------------------------------------------


async def inject_nats_worker(worker_id: int, progress) -> int:
    import asyncio

    import nats

    start, end = worker_range(worker_id)
    # The pending buffer only needs to hold what accumulates between two acks.
    client = await nats.connect(
        NATS_URL,
        pending_size=max(8 * 1024**2, ACK_EVERY * MSG_BYTES * 4),
        flusher_queue_size=1024,
    )
    jetstream = client.jetstream(timeout=60)
    timestamps = TimestampFormatter()
    pacer = Pacer(worker_rate())
    sent_bytes = 0
    since_report = 0
    for seq in range(start, end):
        sleep_seconds = pacer.due_sleep()
        if sleep_seconds:
            await asyncio.sleep(sleep_seconds)
        tenant, payload = log_line(seq, timestamps)
        subject = f"{NATS_STREAM}.{tenant}"
        if seq % ACK_EVERY == 0:
            # Doubles as a flush: bounds the in-flight window and confirms
            # that JetStream persisted everything published before it.
            await jetstream.publish(subject, payload)
        else:
            await client.publish(subject, payload)
        sent_bytes += len(payload)
        since_report += 1
        if since_report == 10_000:
            with progress.get_lock():
                progress.value += since_report
            since_report = 0
    await client.flush(timeout=60)
    with progress.get_lock():
        progress.value += since_report
    await client.drain()
    return sent_bytes


def run_nats_worker(worker_id: int, progress, results) -> None:
    import asyncio

    global _POOL
    _POOL = build_pool()
    results[worker_id] = asyncio.run(inject_nats_worker(worker_id, progress))


def create_nats_stream() -> None:
    import asyncio

    import nats
    from nats.js.api import RetentionPolicy, StorageType, StreamConfig

    async def create() -> None:
        client = await nats.connect(NATS_URL)
        jetstream = client.jetstream()
        try:
            await jetstream.add_stream(
                StreamConfig(
                    name=NATS_STREAM,
                    subjects=[f"{NATS_STREAM}.>"],
                    storage=StorageType.FILE,
                    # The Quickwit source is ack-less: anything but limits
                    # retention would delete messages before they are indexed.
                    retention=RetentionPolicy.LIMITS,
                )
            )
            print(f"created stream `{NATS_STREAM}`", flush=True)
        except Exception as exc:
            print(f"stream `{NATS_STREAM}` not created: {exc}", flush=True)
        await client.drain()

    asyncio.run(create())


def nats_stream_state() -> tuple[int, int]:
    import asyncio

    import nats

    async def state() -> tuple[int, int]:
        client = await nats.connect(NATS_URL)
        info = await client.jetstream().stream_info(NATS_STREAM)
        await client.drain()
        return info.state.messages, info.state.bytes

    return asyncio.run(state())


# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------


def run_kafka_worker(worker_id: int, progress, results) -> None:
    from confluent_kafka import Producer

    global _POOL
    _POOL = build_pool()

    start, end = worker_range(worker_id)
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
            "queue.buffering.max.messages": 100_000,
            "queue.buffering.max.kbytes": 131_072,
            "compression.type": "none",
            "acks": "1",
        }
    )
    timestamps = TimestampFormatter()
    pacer = Pacer(worker_rate())
    sent_bytes = 0
    since_report = 0
    for seq in range(start, end):
        sleep_seconds = pacer.due_sleep()
        if sleep_seconds:
            # `poll` doubles as the sleep: it serves delivery callbacks meanwhile.
            producer.poll(sleep_seconds)
        tenant, payload = log_line(seq, timestamps)
        while True:
            try:
                producer.produce(
                    KAFKA_TOPIC,
                    key=tenant,
                    value=payload,
                    partition=seq % KAFKA_PARTITIONS,
                    on_delivery=on_delivery,
                )
                break
            except BufferError:
                producer.poll(0.1)
        producer.poll(0)
        sent_bytes += len(payload)
        since_report += 1
        if since_report == 10_000:
            with progress.get_lock():
                progress.value += since_report
            since_report = 0
    producer.flush()
    with progress.get_lock():
        progress.value += since_report
    if errors:
        sys.exit(f"worker {worker_id}: {errors} messages failed to be delivered")
    results[worker_id] = sent_bytes


def create_kafka_topic() -> None:
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
            print(f"topic `{topic}` not created: {exc}", flush=True)


def kafka_topic_state() -> int:
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": "inject-verify"}
    )
    metadata = consumer.list_topics(KAFKA_TOPIC, timeout=30)
    num_messages = 0
    for partition_id in metadata.topics[KAFKA_TOPIC].partitions:
        _, high = consumer.get_watermark_offsets(
            TopicPartition(KAFKA_TOPIC, partition_id), timeout=30
        )
        num_messages += high
    consumer.close()
    return num_messages


# ---------------------------------------------------------------------------


def report_progress(processes, progress, started: float) -> None:
    while any(process.is_alive() for process in processes):
        time.sleep(5)
        sent = progress.value
        elapsed = time.monotonic() - started
        print(
            f"{sent * MSG_BYTES / 1024**3:.2f} GiB / "
            f"{NUM_MESSAGES * MSG_BYTES / 1024**3:.2f} GiB "
            f"({sent} messages, {sent / max(elapsed, 0.001):.0f} msg/s, "
            f"{sent * MSG_BYTES / 1024**2 / max(elapsed, 0.001):.0f} MiB/s)",
            flush=True,
        )


def main() -> None:
    print(
        f"injecting {NUM_MESSAGES} messages of ~{MSG_BYTES}B "
        f"across {NUM_TENANTS} tenants into {TARGET} with {WORKERS} workers "
        f"{f'capped at {RATE_MIB_S:.0f} MiB/s ' if RATE_MIB_S > 0 else ''}"
        f"(started {datetime.now(timezone.utc).isoformat(timespec='seconds')})",
        flush=True,
    )
    if TARGET == "nats":
        create_nats_stream()
        worker_entrypoint = run_nats_worker
    elif TARGET == "kafka":
        create_kafka_topic()
        worker_entrypoint = run_kafka_worker
    else:
        sys.exit(f"unknown TARGET `{TARGET}`, expected `nats` or `kafka`")

    progress = multiprocessing.Value("q", 0)
    results = multiprocessing.Array("q", WORKERS)
    started = time.monotonic()
    processes = [
        multiprocessing.Process(
            target=worker_entrypoint, args=(worker_id, progress, results)
        )
        for worker_id in range(WORKERS)
    ]
    for process in processes:
        process.start()
    report_progress(processes, progress, started)
    for process in processes:
        process.join()
    elapsed = time.monotonic() - started

    failed = [process.exitcode for process in processes if process.exitcode != 0]
    sent_bytes = sum(results)
    print(
        f"done: {sent_bytes / 1024**3:.2f} GiB in {progress.value} messages "
        f"in {elapsed:.0f}s "
        f"({sent_bytes / 1024**2 / max(elapsed, 0.001):.0f} MiB/s)",
        flush=True,
    )
    if failed:
        sys.exit(f"{len(failed)} workers failed: {failed}")

    # The brokers are the source of truth: a mismatch means messages were
    # dropped, which would silently shrink the corpus of the indexing bench.
    if TARGET == "nats":
        stored_messages, stored_bytes = nats_stream_state()
        print(
            f"stream `{NATS_STREAM}`: {stored_messages} messages, "
            f"{stored_bytes / 1024**3:.2f} GiB on disk",
            flush=True,
        )
    else:
        stored_messages = kafka_topic_state()
        print(f"topic `{KAFKA_TOPIC}`: {stored_messages} messages", flush=True)
    if stored_messages != NUM_MESSAGES:
        sys.exit(
            f"broker holds {stored_messages} messages, expected {NUM_MESSAGES} "
            f"({NUM_MESSAGES - stored_messages} lost)"
        )


if __name__ == "__main__":
    main()
