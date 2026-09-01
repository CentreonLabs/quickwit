#!/usr/bin/env python3
"""Turns the per-run artefacts written by `bench.py` into the report tables.

Two rates are reported per run, because they answer different questions:

  * end-to-end: the whole corpus divided by the wall clock from the creation of
    the sources. It includes pipeline startup, and with cooperative indexing a
    pipeline sleeps up to one commit timeout before it starts, so on a corpus
    this small the startup cost is visible in this number.
  * steady-state: the slope of the processed-bytes counter between the moments
    it crosses 20 % and 80 % of the corpus. That window excludes both startup
    and the final flush, so it is the rate the node sustains once every pipeline
    is running.
"""

import argparse
import json
import os

MIB = 1024**2
GIB = 1024**3


def steady_state(samples: list[dict], total_bytes: float) -> tuple[float, float]:
    """(MiB/s while the corpus goes from 20 % to 80 % processed, window length).

    A least-squares fit over every sample in the window rather than the slope
    between its two endpoints: at 16 pipelines the corpus drains in seconds, so
    the window holds few samples and the endpoints alone are noisy.
    """
    if total_bytes <= 0 or len(samples) < 3:
        return float("nan"), float("nan")
    low, high = 0.2 * total_bytes, 0.8 * total_bytes
    window = [
        sample for sample in samples if low <= sample["processed_bytes"] <= high
    ]
    if len(window) < 2:
        return float("nan"), float("nan")
    count = len(window)
    mean_time = sum(sample["elapsed"] for sample in window) / count
    mean_bytes = sum(sample["processed_bytes"] for sample in window) / count
    covariance = sum(
        (sample["elapsed"] - mean_time) * (sample["processed_bytes"] - mean_bytes)
        for sample in window
    )
    variance = sum((sample["elapsed"] - mean_time) ** 2 for sample in window)
    if variance <= 0:
        return float("nan"), float("nan")
    slope = covariance / variance
    return slope / MIB, window[-1]["elapsed"] - window[0]["elapsed"]


def load(out_dir: str) -> list[dict]:
    runs = []
    for name in sorted(os.listdir(out_dir)):
        result_path = os.path.join(out_dir, name, "result.json")
        samples_path = os.path.join(out_dir, name, "samples.json")
        if not os.path.exists(result_path):
            continue
        with open(result_path) as result_file:
            result = json.load(result_file)
        with open(samples_path) as samples_file:
            samples = json.load(samples_file)
        processed_bytes = samples[-1]["processed_bytes"] if samples else 0.0
        # Re-read documents inflate the processed counters, so the corpus size is
        # derived from the published documents instead.
        published_at_end = samples[-1]["published_docs"] if samples else 0
        total_bytes = (
            processed_bytes * published_at_end / samples[-1]["processed_docs"]
            if samples and samples[-1]["processed_docs"]
            else processed_bytes
        )
        processed_docs = samples[-1]["processed_docs"] if samples else 0
        rate, window = steady_state(samples, processed_bytes)
        # Time to the first published split, not to the first processed document:
        # cooperative indexing gates the *indexer*, while the source and doc
        # processor keep running, so `first_doc_seconds` is blind to it.
        first_publish = next(
            (sample["elapsed"] for sample in samples if sample["published_docs"] > 0),
            None,
        )
        peak_pipelines = max((s["pipelines"] for s in samples), default=0)
        window_samples = sum(
            1
            for sample in samples
            if 0.2 * processed_bytes <= sample["processed_bytes"] <= 0.8 * processed_bytes
        )
        peak_split_builders = max((s.get("split_builders", 0) for s in samples), default=0)
        # Documents processed more than once: a Kafka consumer-group rebalance can
        # hand a partition to a second pipeline, whose checkpoint delta the
        # metastore then rejects, faulting the pipeline into re-reading. Published
        # counts stay exact, but the CPU was really spent.
        published_docs = published_at_end
        processed_docs = samples[-1]["processed_docs"] if samples else 0
        result.update(
            {
                "corpus_bytes": total_bytes,
                "duplicate_work": (processed_docs / published_docs)
                if published_docs
                else float("nan"),
                "end_to_end_mib_s": total_bytes / MIB / result["elapsed_seconds"],
                "steady_state_mib_s": rate,
                # `steady_state_mib_s` counts every pass over a document. Dividing
                # by the duplication factor gives the rate at which the corpus
                # actually advanced, which is what a catch-up takes.
                "steady_unique_mib_s": rate
                / (processed_docs / published_at_end if published_at_end else 1),
                "steady_state_window_s": window,
                "steady_state_samples": window_samples,
                "cpu_seconds_per_gib": result["process_cpu_seconds"] / (total_bytes / GIB)
                if total_bytes
                else float("nan"),
                "mean_cores_used": result["process_cpu_seconds"] / result["elapsed_seconds"],
                # Everything busy on the machine that is not the Quickwit process is
                # essentially the broker, which is the difference between an
                # indexer-bound run and a broker-bound one.
                "broker_cores": (
                    result["system_cpu_seconds"] - result["process_cpu_seconds"]
                )
                / result["elapsed_seconds"],
                "first_publish_seconds": first_publish,
                "peak_pipelines": peak_pipelines,
                "peak_split_builders": peak_split_builders,
                "peak_rss_gib": result["max_resident_bytes"] / GIB,
                "final_splits": samples[-1]["published_splits"] if samples else 0,
            }
        )
        runs.append(result)
    return runs


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()
    runs = load(args.out_dir)
    if not runs:
        raise SystemExit(f"no run results under {args.out_dir}")

    header = (
        f"{'run':22} {'src':6} {'pipe':>5} {'coop':>5} {'corpus':>8} {'wall':>8} "
        f"{'e2e':>9} {'gross':>9} {'uniq':>9} {'1st pub':>8} {'cpu/GiB':>8} "
        f"{'cores':>6} {'rss':>6} {'splt':>5} {'dup':>5} {'n':>4} {'ok':>4}"
    )
    print(header)
    print("-" * len(header))
    for run in runs:
        print(
            f"{run['name']:22} {run['source']:6} {run['pipelines']:>5} "
            f"{('on' if run['cooperative_indexing'] else 'off'):>5} "
            f"{run['corpus_bytes'] / GIB:>7.2f}G {run['elapsed_seconds']:>7.1f}s "
            f"{run['end_to_end_mib_s']:>7.1f}MB {run['steady_state_mib_s']:>7.1f}MB "
            f"{run['steady_unique_mib_s']:>7.1f}MB "
            f"{(run['first_publish_seconds'] or 0):>7.1f}s {run['cpu_seconds_per_gib']:>7.1f}s "
            f"{run['mean_cores_used']:>6.1f} {run['peak_rss_gib']:>5.1f}G "
            f"{run['final_splits']:>5} {run['duplicate_work']:>5.2f} "
            f"{run['steady_state_samples']:>4} "
            f"{'yes' if run['complete'] else 'NO':>4}"
        )
    print()
    print("e2e/gross/uniq are MiB/s of raw (uncompressed) log bytes. gross counts every")
    print("pass over a document; uniq = gross / dup is how fast the corpus itself advanced.")
    print("cpu/GiB is quickwit process CPU seconds per GiB of raw logs indexed.")
    print("n is how many samples the steady-state fit had; below ~4 read the rate as indicative.")
    print("dup is processed docs / published docs: 1.00 means every document was indexed once.")
    print("1st pub is time to the first published split, which is what cooperative indexing delays.")
    with open(os.path.join(args.out_dir, "summary.json"), "w") as summary_file:
        json.dump(runs, summary_file, indent=1)


if __name__ == "__main__":
    main()
