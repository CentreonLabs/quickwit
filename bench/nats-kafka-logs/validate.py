#!/usr/bin/env python3
"""Checks that a NATS-sourced index holds the corpus and is searchable.

The throughput bench only proves documents were *published*. This proves they
were indexed correctly: exact total, exact per-tenant and per-level splits (both
derivable from the generator, since a document is a pure function of its
sequence number), and that the tokenized text field answers term queries.

Assumes the NATS stream already holds NUM_MESSAGES messages.

Usage: python3 validate.py [--messages 200000]
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

BENCH_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BENCH_DIR)
import bench

LEVELS = ["DEBUG", "INFO", "INFO", "INFO", "WARN", "ERROR"]
NUM_TENANTS = 16


def expected_counts(num_messages: int) -> dict:
    """Ground truth straight from the generator's rules."""
    counts = {"total": num_messages}
    counts["tenant-3"] = sum(1 for seq in range(num_messages) if seq % NUM_TENANTS == 3)
    counts["level-ERROR"] = sum(
        1 for seq in range(num_messages) if LEVELS[seq % len(LEVELS)] == "ERROR"
    )
    counts["level-INFO"] = sum(
        1 for seq in range(num_messages) if LEVELS[seq % len(LEVELS)] == "INFO"
    )
    return counts


def search(port: int, index: str, query: str, deadline: float = 0.0) -> int:
    """Runs one count query, waiting out the searcher's cluster registration.

    The REST server reports ready before the node has gossiped itself in as a
    searcher, and until it has, every search fails with `no available searcher
    nodes in the cluster`. That is transient, so it is retried rather than
    treated as a result.
    """
    url = (
        f"http://127.0.0.1:{port}/api/v1/{index}/search"
        f"?query={urllib.parse.quote(query)}&max_hits=0"
    )
    deadline = deadline or time.monotonic() + 90
    while True:
        try:
            with urllib.request.urlopen(url, timeout=60) as response:
                return json.load(response)["num_hits"]
        except urllib.error.HTTPError as error:
            if error.code != 500 or time.monotonic() > deadline:
                raise
            time.sleep(1.0)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--messages", type=int, default=200000)
    parser.add_argument("--rest-port", type=int, default=7380)
    parser.add_argument("--data-root", default="/mnt/bench/qw")
    parser.add_argument(
        "--binary",
        default=os.path.join(BENCH_DIR, "..", "..", "quickwit", "target", "release", "quickwit"),
    )
    args = parser.parse_args()

    run_args = argparse.Namespace(
        name="validate",
        source="nats",
        pipelines=1,
        coop=False,
        tenants=NUM_TENANTS,
        messages=args.messages,
        commit_timeout_secs=10,
        heap_size="250 MB",
        timeout=900,
        rest_port=args.rest_port,
        rust_log="info",
        binary=os.path.abspath(args.binary),
        out_dir=os.path.join(BENCH_DIR, "runs-validate"),
        purge_data=False,
        per_source_index=False,
        data_root=args.data_root,
        sample_interval=1.0,
        no_backfill=False,
    )
    print(f"indexing {args.messages} messages from NATS, then querying", flush=True)
    result = bench.run(run_args)
    if not result["complete"]:
        sys.exit(f"indexing did not complete: {result['published_docs']} published")

    # The node is stopped by `bench.run`; bring one back up on the same data dir
    # so the splits can be searched.
    data_dir = os.path.join(args.data_root, "validate")
    config_path = os.path.join(run_args.out_dir, "validate", "node-config.yaml")
    # Searcher and metastore only: an indexer would try to respawn the still-enabled
    # NATS source, and its ephemeral consumer from the indexing run is still alive on
    # the server, so JetStream rejects the re-create with `deliver policy can not be
    # updated`. Nothing in this phase needs to index.
    node = subprocess.Popen(
        [
            run_args.binary, "run", "--config", config_path, "--no-color",
            "--service", "searcher", "--service", "metastore",
        ],
        stdout=open(os.path.join(run_args.out_dir, "validate", "search.log"), "w"),
        stderr=subprocess.STDOUT,
        env=dict(os.environ, QW_DISABLE_TELEMETRY="1", QW_DISABLE_INGEST_V1="true"),
        start_new_session=True,
    )
    failures = []
    try:
        bench.wait_until_ready(args.rest_port, time.monotonic() + 120)
        expected = expected_counts(args.messages)
        checks = [
            ("total", "*", expected["total"]),
            ("tenant-3", "tenant:tenant-3", expected["tenant-3"]),
            ("level-ERROR", "level:ERROR", expected["level-ERROR"]),
            ("level-INFO", "level:INFO", expected["level-INFO"]),
        ]
        for label, query, want in checks:
            got = search(args.rest_port, bench.INDEX_ID, query)
            verdict = "ok" if got == want else "MISMATCH"
            if got != want:
                failures.append(f"{label}: got {got}, want {want}")
            print(f"  {label:14} {query:24} {got:>9} (want {want:>9}) {verdict}", flush=True)
        # The tokenized field only has to answer, not match an exact count: the
        # bodies are drawn from a shuffled pool.
        hits = search(args.rest_port, bench.INDEX_ID, "message:timeout")
        print(f"  {'message term':14} {'message:timeout':24} {hits:>9} (want > 0)", flush=True)
        if hits <= 0:
            failures.append("message:timeout returned no hits")
    finally:
        if node.poll() is None:
            node.send_signal(signal.SIGTERM)
            try:
                node.wait(timeout=120)
            except subprocess.TimeoutExpired:
                node.kill()

    if failures:
        sys.exit("VALIDATION FAILED\n  " + "\n  ".join(failures))
    print("VALIDATION OK: corpus is complete and searchable", flush=True)


if __name__ == "__main__":
    main()
