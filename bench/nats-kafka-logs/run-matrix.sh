#!/usr/bin/env bash
# Full NATS-vs-Kafka indexing matrix over an identical synthetic log corpus.
#
# Axes asked of the bench: 1 vs 16 indexing pipelines, cooperative indexing off
# vs on, NATS source vs Kafka source. Plus, per source, two controls that
# separate effects the main cells conflate at 16 pipelines:
#
#   nobf  - sources without `enable_backfill_mode` and a 10 s commit timeout.
#           Backfill mode makes a source exit at the end of the topic, which for
#           Kafka turns every exit into a consumer-group rebalance; the pilot run
#           indexed documents 3.8x over because partitions moved between
#           pipelines and the metastore rejected the losing checkpoint deltas.
#           A shorter commit timeout also shrinks cooperative indexing's cycle,
#           which is what throttles it, and matches Quickwit's own OTLP logs
#           index (5 s) more closely than the 60 s default.
#   idx   - NATS only: one index per tenant rather than one shared index, the
#           topology Pulse would deploy, so each pipeline also gets its own doc
#           mapper, split store and merge pipeline.
#
# The two brokers cannot both hold the corpus in RAM alongside the split store,
# so each source type runs as its own phase: inject, run, release.
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="${OUT_DIR:-$BENCH_DIR/runs}"
NUM_MESSAGES="${NUM_MESSAGES:-5242880}"
PIPELINES="${PIPELINES:-16}"
PHASES="${1:-all}"

mkdir -p "$OUT_DIR"

for mount_point in /mnt/bench/nats /mnt/bench/kafka /mnt/bench/qw; do
  findmnt -t tmpfs "$mount_point" >/dev/null \
    || { echo "$mount_point is not a tmpfs mount, see the README"; exit 1; }
done

# A run that cannot index the whole corpus must not abort the matrix: it is
# reported as incomplete by `summarize.py` and the remaining runs still happen.
run() {
  local name="$1" source="$2" pipelines="$3" coop="$4"
  shift 4
  local args=(
    "--name" "$name" "--source" "$source" "--pipelines" "$pipelines"
    "--messages" "$NUM_MESSAGES" "--out-dir" "$OUT_DIR" "--purge-data" "$@"
  )
  if [[ "$coop" == "on" ]]; then args+=("--coop"); fi
  echo
  if ! python3 "$BENCH_DIR/bench.py" "${args[@]}" 2>&1 | tee "$OUT_DIR/$name.log"; then
    echo "!!! run $name failed or timed out, continuing"
  fi
}

inject() {
  local source="$1"
  echo "=== phase $source: injecting $NUM_MESSAGES messages ==="
  docker compose up -d "$source"
  NUM_MESSAGES="$NUM_MESSAGES" WORKERS="${INJECT_WORKERS:-6}" \
    docker compose run --rm "inject-$source"
}

release() {
  local source="$1"
  echo "=== phase $source: releasing the corpus ==="
  docker compose stop "$source" && docker compose rm -f "$source"
  sudo -n rm -rf "/mnt/bench/$source/..?*" "/mnt/bench/$source/.[!.]*" \
    "/mnt/bench/$source/"* || true
}

# Cooperative indexing at 16 pipelines runs for tens of minutes by design (each
# pipeline cuts a split and sleeps out the rest of its commit-timeout cycle), so
# those cells get a wide deadline rather than an open-ended one.
COOP16=(--timeout 1800)
# Controls sample at 4 Hz: without cooperative indexing 16 pipelines drain the
# corpus in seconds, and a 1 Hz series is too coarse to fit a rate to.
CONTROL=(--commit-timeout-secs 10 --sample-interval 0.25 --no-backfill)

phase_kafka() {
  inject kafka
  run kafka-1-coop-off        kafka 1            off
  run kafka-1-coop-on         kafka 1            on
  run kafka-16-coop-off       kafka "$PIPELINES" off
  run kafka-16-nobf-coop-off  kafka "$PIPELINES" off "${CONTROL[@]}"
  run kafka-16-nobf-coop-on   kafka "$PIPELINES" on  "${CONTROL[@]}" --timeout 1800
  run kafka-16-coop-on        kafka "$PIPELINES" on  "${COOP16[@]}"
  release kafka
}

phase_nats() {
  inject nats
  run nats-1-coop-off         nats 1            off
  run nats-1-coop-on          nats 1            on
  run nats-16-coop-off        nats "$PIPELINES" off --timeout 900
  run nats-16-idx-coop-off    nats "$PIPELINES" off --per-source-index --timeout 900
  run nats-16-nobf-coop-off   nats "$PIPELINES" off "${CONTROL[@]}" --timeout 900
  run nats-16-nobf-coop-on    nats "$PIPELINES" on  "${CONTROL[@]}" --timeout 1800
  run nats-16-coop-on         nats "$PIPELINES" on  "${COOP16[@]}"
  release nats
}

cd "$BENCH_DIR"
case "$PHASES" in
  kafka) phase_kafka ;;
  nats) phase_nats ;;
  all) phase_kafka; phase_nats ;;
  *) echo "usage: $0 [kafka|nats|all]"; exit 1 ;;
esac

echo "MATRIX DONE"
python3 "$BENCH_DIR/summarize.py" --out-dir "$OUT_DIR"
