#!/usr/bin/env python3
"""Runs one Quickwit indexing benchmark over the pre-injected NATS stream or Kafka topic.

A run is a full node lifecycle: a fresh data directory (so a fresh file-backed
metastore and split store), a fresh `quickwit run` process (so cooperative
indexing restarts its cycle from a clean origin of time), then the index and the
source(s) are created through the REST API and the run is over once every
document of the corpus is published.

Sources are created with `enable_backfill_mode`, so a source exits as soon as it
reaches the end of the stream/topic. That forces the pipeline to commit its last
split immediately instead of waiting up to `commit_timeout_secs`, which keeps the
tail of the measurement from being dominated by the commit interval.

Two throughput numbers come out of a run, both needed to read the results:
  * end-to-end: from the creation of the sources to the last published document,
    which includes pipeline startup (with cooperative indexing, a pipeline sleeps
    up to one commit timeout before its first split);
  * steady-state: the slope of the published/processed byte counters over the
    window where all pipelines are active.
"""

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request

BENCH_DIR = os.path.dirname(os.path.abspath(__file__))
CLOCK_TICKS = os.sysconf("SC_CLK_TCK")

INDEX_ID = "bench-logs"
NATS_STREAM = "logs"
KAFKA_TOPIC = "logs"

INDEX_CONFIG = """\
version: 0.9
index_id: {index_id}
doc_mapping:
  mode: strict
  field_mappings:
    - name: timestamp
      type: datetime
      input_formats: [rfc3339]
      fast: true
      fast_precision: seconds
    - name: level
      type: text
      tokenizer: raw
      fast: true
    - name: service
      type: text
      tokenizer: raw
      fast: true
    - name: tenant
      type: text
      tokenizer: raw
      fast: true
    - name: seq
      type: u64
      indexed: false
      fast: true
    - name: trace_id
      type: text
      tokenizer: raw
    - name: message
      type: text
      tokenizer: default
  timestamp_field: timestamp
indexing_settings:
  commit_timeout_secs: {commit_timeout_secs}
  resources:
    heap_size: {heap_size}
search_settings:
  default_search_fields: [message]
"""

NODE_CONFIG = """\
version: 0.8
node_id: bench-node
listen_address: 127.0.0.1
rest:
  listen_port: {rest_port}
data_dir: {data_dir}
indexer:
  enable_cooperative_indexing: {enable_cooperative_indexing}
  enable_otlp_endpoint: false
"""


def rest(port: int, path: str) -> str:
    return f"http://127.0.0.1:{port}/api/v1{path}"


def http_json(url: str, method: str = "GET", body: bytes = None, content_type: str = None,
              timeout: float = 30.0):
    request = urllib.request.Request(url, data=body, method=method)
    if content_type:
        request.add_header("Content-Type", content_type)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = response.read()
    return json.loads(payload) if payload else None


def http_text(url: str, timeout: float = 30.0) -> str:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return response.read().decode()


# ---------------------------------------------------------------------------
# Source configurations
# ---------------------------------------------------------------------------


def source_configs(
    source_type: str, num_pipelines: int, num_tenants: int, backfill: bool = True
) -> list[dict]:
    """One source per pipeline for NATS, one source with N pipelines for Kafka.

    The NATS source rejects `num_pipelines > 1` (a second pipeline would consume
    the same messages and corrupt the shared checkpoint), so NATS parallelism is
    expressed as several sources, each filtering a disjoint set of subjects. That
    is the sharding pattern the source is designed around, and with one tenant
    per source the split of the corpus matches the Kafka one exactly: a Kafka
    partition holds the messages of one NATS subject.
    """
    if source_type == "kafka":
        return [
            {
                "version": "0.9",
                "source_id": "bench-kafka",
                "source_type": "kafka",
                "num_pipelines": num_pipelines,
                "params": {
                    "topic": KAFKA_TOPIC,
                    "client_log_level": "warn",
                    "client_params": {
                        "bootstrap.servers": "localhost:9092",
                        "auto.offset.reset": "earliest",
                    },
                    "enable_backfill_mode": backfill,
                },
            }
        ]
    if num_pipelines == 1:
        return [
            {
                "version": "0.9",
                "source_id": "bench-nats",
                "source_type": "nats",
                "num_pipelines": 1,
                "params": {
                    "uris": ["nats://localhost:4222"],
                    "stream": NATS_STREAM,
                    "deliver_policy": "all",
                    "enable_backfill_mode": backfill,
                },
            }
        ]
    if num_pipelines != num_tenants:
        sys.exit(
            f"NATS runs shard by tenant subject: --pipelines must be 1 or "
            f"{num_tenants}, got {num_pipelines}"
        )
    return [
        {
            "version": "0.9",
            "source_id": f"bench-nats-{tenant:02d}",
            "source_type": "nats",
            "num_pipelines": 1,
            "params": {
                "uris": ["nats://localhost:4222"],
                "stream": NATS_STREAM,
                "subjects": [f"{NATS_STREAM}.tenant-{tenant}"],
                "deliver_policy": "all",
                "enable_backfill_mode": backfill,
            },
        }
        for tenant in range(num_tenants)
    ]


# ---------------------------------------------------------------------------
# Sampling
# ---------------------------------------------------------------------------


def parse_prometheus(text: str) -> dict[str, float]:
    """Sums the samples of the metric families the bench cares about."""
    wanted = {
        "quickwit_indexing_processed_docs_total": 0.0,
        "quickwit_indexing_processed_bytes": 0.0,
        "quickwit_indexing_indexing_pipelines": 0.0,
        "quickwit_indexing_nats_source_pending_messages": 0.0,
        "quickwit_indexing_ongoing_merge_operations": 0.0,
        "quickwit_indexing_pending_merge_operations": 0.0,
        "quickwit_indexing_published_split_uncompressed_bytes_total": 0.0,
        "quickwit_indexing_split_builders": 0.0,
    }
    # Only valid documents count towards the corpus: a parse or schema error
    # would silently shrink it. Only splits that were never merged
    # (`merge_ops="0"`) account for freshly indexed bytes; counting merged
    # splits too would double-count the same documents.
    label_filters = {
        "quickwit_indexing_processed_docs_total": 'docs_processed_status="valid"',
        "quickwit_indexing_processed_bytes": 'docs_processed_status="valid"',
        "quickwit_indexing_published_split_uncompressed_bytes_total": 'merge_ops="0"',
    }
    for line in text.splitlines():
        if line.startswith("#") or not line:
            continue
        name_and_labels, _, value = line.rpartition(" ")
        name = name_and_labels.split("{", 1)[0]
        if name not in wanted:
            continue
        label_filter = label_filters.get(name)
        if label_filter is not None and label_filter not in name_and_labels:
            continue
        try:
            wanted[name] += float(value)
        except ValueError:
            continue
    return wanted


def read_process_cpu(pid: int) -> tuple[float, float]:
    """(cpu seconds, resident bytes) of a process, or (nan, nan) once it is gone."""
    try:
        with open(f"/proc/{pid}/stat") as stat_file:
            fields = stat_file.read().rsplit(") ", 1)[1].split()
        cpu_seconds = (int(fields[11]) + int(fields[12])) / CLOCK_TICKS
        with open(f"/proc/{pid}/statm") as statm_file:
            resident_pages = int(statm_file.read().split()[1])
        return cpu_seconds, resident_pages * os.sysconf("SC_PAGE_SIZE")
    except (FileNotFoundError, IndexError, ProcessLookupError):
        return float("nan"), float("nan")


def read_disk_bytes(device: str = "vda1") -> tuple[float, float]:
    """(bytes read, bytes written) on the block device since boot.

    Sampled to tell an indexing-bound run from an IO-bound one: the block volume
    of this VM is capped around 10-12 MB/s, which is why the corpus and the split
    store live on tmpfs. Any significant traffic here means something escaped it.
    """
    with open("/proc/diskstats") as diskstats:
        for line in diskstats:
            fields = line.split()
            if len(fields) > 9 and fields[2] == device:
                return int(fields[5]) * 512.0, int(fields[9]) * 512.0
    return float("nan"), float("nan")


def read_host_memory() -> tuple[float, float]:
    """(bytes used excluding reclaimable cache, bytes held by tmpfs)."""
    values = {}
    with open("/proc/meminfo") as meminfo:
        for line in meminfo:
            key, _, rest = line.partition(":")
            values[key] = float(rest.strip().split()[0]) * 1024
    used = values["MemTotal"] - values["MemAvailable"]
    return used, values.get("Shmem", 0.0)


def read_system_cpu() -> float:
    """Busy CPU seconds across the whole machine (brokers included)."""
    with open("/proc/stat") as stat_file:
        fields = stat_file.readline().split()[1:]
    values = [int(field) for field in fields]
    idle = values[3] + values[4]  # idle + iowait
    return (sum(values) - idle) / CLOCK_TICKS


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------


def wait_until_ready(port: int, deadline: float) -> None:
    while time.monotonic() < deadline:
        try:
            if http_text(f"http://127.0.0.1:{port}/health/readyz", timeout=2).strip() == "true":
                return
        except (urllib.error.URLError, OSError, TimeoutError):
            pass
        time.sleep(0.2)
    sys.exit("node did not become ready in time")


# Deliberately no page-cache dropping between runs: the corpus and the split
# store both live on tmpfs, so there is no disk read to warm or cool, and
# dropping caches would only evict tmpfs-adjacent metadata for no gain.


def run(args: argparse.Namespace) -> dict:
    run_dir = os.path.join(args.out_dir, args.name)
    # The split store must not land on the (capped) block volume, or the run
    # measures the disk. It is wiped per run so every run starts with an empty
    # file-backed metastore and split store.
    data_dir = os.path.join(args.data_root, args.name)
    shutil.rmtree(run_dir, ignore_errors=True)
    shutil.rmtree(data_dir, ignore_errors=True)
    os.makedirs(run_dir)
    os.makedirs(data_dir)

    node_config_path = os.path.join(run_dir, "node-config.yaml")
    with open(node_config_path, "w") as config_file:
        config_file.write(
            NODE_CONFIG.format(
                rest_port=args.rest_port,
                data_dir=os.path.abspath(data_dir),
                enable_cooperative_indexing="true" if args.coop else "false",
            )
        )
    sources = source_configs(
        args.source, args.pipelines, args.tenants, backfill=not args.no_backfill
    )
    # `--per-source-index` reproduces the per-tenant-index topology rather than
    # funnelling every source into one index: same pipelines, but each has its
    # own doc mapper, split store and merge pipeline.
    if args.per_source_index and len(sources) > 1:
        index_ids = [f"{INDEX_ID}-{position:02d}" for position in range(len(sources))]
    else:
        index_ids = [INDEX_ID]
    index_configs = {
        index_id: INDEX_CONFIG.format(
            index_id=index_id,
            commit_timeout_secs=args.commit_timeout_secs,
            heap_size=args.heap_size,
        )
        for index_id in index_ids
    }
    with open(os.path.join(run_dir, "index-config.yaml"), "w") as config_file:
        config_file.write(index_configs[index_ids[0]])


    node_log_path = os.path.join(run_dir, "node.log")
    node_log = open(node_log_path, "w")
    environment = dict(
        os.environ,
        QW_DISABLE_TELEMETRY="1",
        # Without this, every index also gets an idle `_ingest-api-source`
        # pipeline, which makes the pipeline gauge unreadable.
        QW_DISABLE_INGEST_V1="true",
        RUST_LOG=args.rust_log,
    )
    node = subprocess.Popen(
        [args.binary, "run", "--config", node_config_path, "--no-color"],
        stdout=node_log,
        stderr=subprocess.STDOUT,
        env=environment,
        start_new_session=True,
    )
    samples: list[dict] = []
    result: dict = {}
    try:
        wait_until_ready(args.rest_port, time.monotonic() + 120)
        for index_config in index_configs.values():
            http_json(
                rest(args.rest_port, "/indexes"),
                method="POST",
                body=index_config.encode(),
                content_type="application/yaml",
            )
        cpu_before, _ = read_process_cpu(node.pid)
        system_cpu_before = read_system_cpu()
        disk_read_before, disk_write_before = read_disk_bytes()
        started = time.monotonic()
        for position, source in enumerate(sources):
            index_id = index_ids[position] if len(index_ids) > 1 else index_ids[0]
            http_json(
                rest(args.rest_port, f"/indexes/{index_id}/sources"),
                method="POST",
                body=json.dumps(source).encode(),
                content_type="application/json",
            )
        sources_created = time.monotonic()

        deadline = started + args.timeout
        first_doc_at = None
        published_docs = 0
        max_resident_bytes = 0.0
        while time.monotonic() < deadline:
            time.sleep(args.sample_interval)
            now = time.monotonic()
            try:
                metrics = parse_prometheus(
                    http_text(f"http://127.0.0.1:{args.rest_port}/metrics", timeout=10)
                )
                stats = {"num_published_docs": 0, "num_published_splits": 0,
                         "size_published_splits": 0}
                for index_id in index_ids:
                    index_stats = http_json(
                        rest(args.rest_port, f"/indexes/{index_id}/describe"), timeout=10
                    )
                    for key in stats:
                        stats[key] += index_stats[key]
            except (urllib.error.URLError, OSError, TimeoutError, json.JSONDecodeError) as error:
                print(f"  sampling error: {error}", flush=True)
                continue
            cpu_seconds, resident_bytes = read_process_cpu(node.pid)
            disk_read, disk_write = read_disk_bytes()
            host_memory_used, tmpfs_bytes = read_host_memory()
            if resident_bytes == resident_bytes:  # not NaN
                max_resident_bytes = max(max_resident_bytes, resident_bytes)
            published_docs = stats["num_published_docs"]
            processed_docs = metrics["quickwit_indexing_processed_docs_total"]
            if first_doc_at is None and processed_docs > 0:
                first_doc_at = now
            samples.append(
                {
                    "elapsed": round(now - started, 2),
                    "processed_docs": processed_docs,
                    "processed_bytes": metrics["quickwit_indexing_processed_bytes"],
                    "published_docs": published_docs,
                    "published_splits": stats["num_published_splits"],
                    "published_bytes": stats["size_published_splits"],
                    "pipelines": metrics["quickwit_indexing_indexing_pipelines"],
                    "nats_pending": metrics["quickwit_indexing_nats_source_pending_messages"],
                    "ongoing_merges": metrics["quickwit_indexing_ongoing_merge_operations"],
                    "pending_merges": metrics["quickwit_indexing_pending_merge_operations"],
                    "fresh_split_bytes": metrics[
                        "quickwit_indexing_published_split_uncompressed_bytes_total"
                    ],
                    "split_builders": metrics["quickwit_indexing_split_builders"],
                    "cpu_seconds": round(cpu_seconds - cpu_before, 2),
                    "system_cpu_seconds": round(read_system_cpu() - system_cpu_before, 2),
                    "resident_bytes": resident_bytes,
                    "disk_read_bytes": disk_read - disk_read_before,
                    "disk_write_bytes": disk_write - disk_write_before,
                    "host_memory_used_bytes": host_memory_used,
                    "tmpfs_bytes": tmpfs_bytes,
                }
            )
            sample = samples[-1]
            print(
                f"  t={sample['elapsed']:7.1f}s "
                f"published={published_docs:>9}/{args.messages} "
                f"processed={int(processed_docs):>9} "
                f"pipelines={int(sample['pipelines']):>2} "
                f"merges={int(sample['ongoing_merges']):>2} "
                f"cpu={sample['cpu_seconds']:>7.1f}s "
                f"rss={resident_bytes / 1024 ** 3:.1f}GiB",
                flush=True,
            )
            if published_docs >= args.messages:
                break
            if node.poll() is not None:
                raise RuntimeError(f"node exited early with code {node.returncode}")
        ended = time.monotonic()
        cpu_seconds, _ = read_process_cpu(node.pid)
        result = {
            "name": args.name,
            "source": args.source,
            "pipelines": args.pipelines,
            "cooperative_indexing": args.coop,
            "num_sources": len(sources),
            "num_indexes": len(index_ids),
            "expected_docs": args.messages,
            "published_docs": published_docs,
            "complete": published_docs >= args.messages,
            "data_root": args.data_root,
            "commit_timeout_secs": args.commit_timeout_secs,
            "sample_interval": args.sample_interval,
            "backfill_mode": not args.no_backfill,
            "heap_size": args.heap_size,
            "elapsed_seconds": round(ended - started, 2),
            "source_creation_seconds": round(sources_created - started, 2),
            "first_doc_seconds": round(first_doc_at - started, 2) if first_doc_at else None,
            "process_cpu_seconds": round(cpu_seconds - cpu_before, 2),
            "system_cpu_seconds": round(read_system_cpu() - system_cpu_before, 2),
            "max_resident_bytes": max_resident_bytes,
            "disk_read_bytes": read_disk_bytes()[0] - disk_read_before,
            "disk_write_bytes": read_disk_bytes()[1] - disk_write_before,
            "peak_host_memory_used_bytes": max(
                (sample["host_memory_used_bytes"] for sample in samples), default=0.0
            ),
        }
    finally:
        if node.poll() is None:
            node.send_signal(signal.SIGTERM)
            try:
                node.wait(timeout=180)
            except subprocess.TimeoutExpired:
                node.kill()
                node.wait()
        node_log.close()

    with open(os.path.join(run_dir, "samples.json"), "w") as samples_file:
        json.dump(samples, samples_file, indent=1)
    with open(os.path.join(run_dir, "result.json"), "w") as result_file:
        json.dump(result, result_file, indent=1)
    # librdkafka reports end-of-partition at error level, and backfill mode
    # turns that on deliberately, so those lines are not failures.
    with open(node_log_path) as log_file:
        result["node_log_errors"] = sum(
            1
            for line in log_file
            if "ERROR" in line and "PartitionEOF" not in line
        )
    if args.purge_data:
        shutil.rmtree(data_dir, ignore_errors=True)
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    parser.add_argument("--source", required=True, choices=["nats", "kafka"])
    parser.add_argument("--pipelines", type=int, default=1)
    parser.add_argument("--coop", action="store_true")
    parser.add_argument("--tenants", type=int, default=16)
    parser.add_argument("--messages", type=int, default=5 * 1024**2)
    parser.add_argument("--commit-timeout-secs", type=int, default=60)
    parser.add_argument("--heap-size", default="250 MB")
    parser.add_argument("--timeout", type=float, default=2400)
    parser.add_argument("--rest-port", type=int, default=7380)
    parser.add_argument("--rust-log", default="info")
    parser.add_argument(
        "--binary", default=os.path.join(BENCH_DIR, "..", "..", "quickwit", "target", "release", "quickwit")
    )
    parser.add_argument("--out-dir", default=os.path.join(BENCH_DIR, "runs"))
    parser.add_argument("--purge-data", action="store_true")
    parser.add_argument("--per-source-index", action="store_true")
    parser.add_argument("--data-root", default="/mnt/bench/qw")
    # 16 pipelines drain a 5 GiB corpus in a handful of seconds, which leaves a
    # 1 Hz series too coarse to fit a rate to.
    parser.add_argument("--sample-interval", type=float, default=1.0)
    # Backfill mode makes a source exit at the end of the stream/topic, which
    # avoids a commit-timeout-long tail. For Kafka it also makes every exit a
    # consumer-group rebalance, so `--no-backfill` is the control that separates
    # rebalance churn caused by exits from churn caused by startup.
    parser.add_argument("--no-backfill", action="store_true")
    args = parser.parse_args()
    args.binary = os.path.abspath(args.binary)

    print(
        f"=== run `{args.name}`: {args.source}, {args.pipelines} pipeline(s), "
        f"cooperative indexing {'on' if args.coop else 'off'} ===",
        flush=True,
    )
    result = run(args)
    print(json.dumps(result, indent=1), flush=True)
    if not result.get("complete"):
        sys.exit(f"run `{args.name}` did not index the whole corpus")


if __name__ == "__main__":
    main()
