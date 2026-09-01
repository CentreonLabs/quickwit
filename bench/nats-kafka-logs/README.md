# NATS vs Kafka indexing bench

Compares the Quickwit NATS source against the Kafka source on an identical
corpus of synthetic JSON logs, across the two axes that matter for placing many
tenants on one indexer: **number of indexing pipelines** (1 vs 16) and
**cooperative indexing** (off vs on).

The corpus is a fixed number of ~1 KiB JSON log documents spread over 16
tenants. A document is a pure function of its sequence number, so the NATS
stream and the Kafka topic hold byte-for-byte the same documents:

| | NATS | Kafka |
|---|---|---|
| container | stream `logs`, subjects `logs.tenant-{0..15}` | topic `logs`, 16 partitions |
| shard of tenant *n* | subject `logs.tenant-n` | partition *n* (explicit, not hash-based) |
| 1-pipeline run | 1 source over the whole stream | 1 source, `num_pipelines: 1`, all 16 partitions |
| 16-pipeline run | 16 sources, one subject each | 1 source, `num_pipelines: 16` |

The NATS source rejects `num_pipelines > 1` — a second pipeline would consume
the same messages and corrupt the shared checkpoint — so NATS parallelism is
expressed as several sources with disjoint subject filters. Pairing one tenant
per source makes the two 16-way splits exactly equivalent.

## The bench VM's disk is not usable in the data path

The block volume of the bench VM sustains only **~10-12 MB/s in both
directions** (it bursts to ~180 MB/s into the page cache, then collapses — a
burst-credit volume with the credits gone). That is an order of magnitude below
what a single indexing pipeline consumes, so anything left on it measures the
disk instead of Quickwit.

Both broker stores and the Quickwit split store therefore live on tmpfs:

```bash
sudo mkdir -p /mnt/bench/{nats,kafka,qw}
sudo mount -t tmpfs -o size=6G,mode=0777 tmpfs /mnt/bench/nats
sudo mount -t tmpfs -o size=6G,mode=0777 tmpfs /mnt/bench/kafka
sudo mount -t tmpfs -o size=8G,mode=0777 tmpfs /mnt/bench/qw
```

The two corpora plus the split store plus the node do not fit in RAM at once, so
`run-matrix.sh` runs the two source types in separate phases: inject one broker,
run its matrix, release it, move to the other.

NATS needs one more thing: JetStream sizes its own storage limit from the store
filesystem and keeps headroom, so on a tmpfs sized near the corpus it refuses
writes with `resource limits exceeded` well before the tmpfs is full, and
nats-server 2.10 has no CLI flag for the limit. `nats/nats.conf` states it
(`max_file: 7GB`) and is mounted into the container. `bench.py` samples
`/proc/diskstats` on every tick, so a run that did touch the volume is visible
in `disk_read_bytes`/`disk_write_bytes` of its `result.json`.

## Usage

```bash
cd bench/nats-kafka-logs

# Whole matrix: injection included. Budget ~80 min for the default 5 GiB
# corpus -- the 16-pipeline cooperative-indexing cells run for tens of minutes
# by design, see "Reading the numbers".
./run-matrix.sh           # or: ./run-matrix.sh kafka / ./run-matrix.sh nats

# One run at a time (broker must already hold the corpus).
python3 bench.py --name kafka-16-coop-on --source kafka --pipelines 16 --coop

# Re-print the tables from artefacts already on disk.
python3 summarize.py --out-dir runs
```

Injection tunables (env vars): `NUM_MESSAGES` (default 5242880, i.e. ~5 GiB),
`MSG_BYTES` (1024), `NUM_TENANTS` (16), `WORKERS` (6 from `run-all.sh`),
`KAFKA_PARTITIONS` (16), `RATE_MIB_S` (0 = uncapped; set it when the broker
store is *not* tmpfs, otherwise a burst stalls the broker).

Bench tunables: `--pipelines`, `--coop`, `--commit-timeout-secs` (60),
`--heap-size` (250 MB), `--per-source-index`, `--rest-port` (7380),
`--data-root` (/mnt/bench/qw).

`--heap-size` is deliberately below Quickwit's 2 GB default: 16 pipelines at
2 GB each would not fit in this VM's RAM. It is the same for every run, so the
comparison holds, but a production single-pipeline indexer with a larger budget
cuts fewer, bigger splits and does less merge work.

The message-body pool size is a prime (4093) so that it stays coprime with the
tenant count. With the earlier 4096, tenant *n* only ever drew bodies from pool
indices congruent to *n* mod 16, i.e. 256 of 4096. Kafka delivers per-partition
batches and NATS delivers in stream-sequence order, so that correlation gave
Kafka's docstore blocks far better locality than NATS's on the same corpus
(splits compressed to 0.13 of raw against 0.19) and put the two sources on
measurably different work per byte at one pipeline.

## What a run does

Fresh data directory (fresh file-backed metastore and split store), fresh
`quickwit run` process (so cooperative indexing restarts its cycle from a clean
origin of time), then the index and the source(s) are created over the REST API
and the run ends when every document of the corpus is published.

Sources use `enable_backfill_mode`, so a source exits on reaching the end of the
stream/topic. That makes the pipeline commit its last split immediately instead
of waiting up to `commit_timeout_secs`, which keeps the tail of the measurement
from being dominated by the commit interval.

Each run leaves `runs/<name>/`: `result.json` (totals), `samples.json` (1 Hz
time series of processed/published docs and bytes, running pipelines, split
builders, merges, CPU, RSS, disk), `node.log`, and the exact configs used.

## Reading the numbers

`summarize.py` prints two rates, which answer different questions:

* **end-to-end** — corpus divided by wall clock from source creation. Includes
  pipeline startup. Cooperative indexing sleeps each pipeline up to one
  `commit_timeout` before its first split, so on a corpus this small that
  startup cost is a visible part of this number, by design.
* **steady-state** — slope of the processed-bytes counter between 20 % and 80 %
  of the corpus. Excludes startup and the final flush: the rate the node
  sustains with every pipeline running.

`dup` is processed documents over published documents. It must be 1.00; above
that the pipeline is redoing work (a Kafka consumer-group rebalance can hand a
partition to a second pipeline, whose checkpoint delta the metastore then
rejects, faulting the pipeline into re-reading), and `gross` is inflated by
exactly that factor. `uniq` = `gross` / `dup` is how fast the corpus itself
advanced, which is what a catch-up actually takes.

`cpu/GiB` (process CPU seconds per GiB of raw logs) is the number to compare
across configurations that ran for different durations, and `cores` shows how
much of the 16 vCPUs a configuration actually managed to use. It only counts the
Quickwit process: `result.json` also carries `system_cpu_seconds` for the whole
machine, and the difference is the broker, which matters -- with 16
subject-filtered NATS consumers replaying one stream, nats-server is the
bottleneck rather than the indexer.

## Tear down

```bash
docker compose down
sudo rm -rf /mnt/bench/nats/* /mnt/bench/kafka/* && rm -rf /mnt/bench/qw/*
```
