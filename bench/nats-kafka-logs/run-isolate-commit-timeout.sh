#!/usr/bin/env bash
# Isolates commit timeout from backfill mode for the cooperative-indexing cells.
#
# In the main matrix the healthy 16-pipeline cooperative run differed from the
# pathological one in two ways at once: no backfill mode *and* a 10 s commit
# timeout instead of 60 s. This runs the missing corner -- no backfill, 60 s
# commit timeout, cooperative indexing on -- so the two can be told apart. If it
# is slow, the commit timeout is what throttles cooperative indexing; if it is
# fast, backfill mode was the whole story.
#
# Capped at 900 s: the point is to establish whether the cell is in the
# "seconds" regime or the "many minutes" regime, and an incomplete run still
# yields the rate from its samples. The mechanism is source-independent (the
# indexer sleeps when its mailbox drains, whatever fed it), so Kafka alone
# settles it.
#
# Usage: ./run-isolate-commit-timeout.sh <kafka|nats> [noinject]
set -euo pipefail
BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="${OUT_DIR:-$BENCH_DIR/runs}"
NUM_MESSAGES="${NUM_MESSAGES:-5242880}"
PIPELINES="${PIPELINES:-16}"
SOURCE="${1:?usage: $0 <kafka|nats> [noinject]}"
INJECT="${2:-inject}"

cd "$BENCH_DIR"
docker compose up -d "$SOURCE"
if [[ "$INJECT" != "noinject" ]]; then
  NUM_MESSAGES="$NUM_MESSAGES" WORKERS="${INJECT_WORKERS:-6}" \
    docker compose run --rm "inject-$SOURCE"
fi

name="$SOURCE-16-nobf-ct60-coop-on"
python3 "$BENCH_DIR/bench.py" \
  --name "$name" --source "$SOURCE" --pipelines "$PIPELINES" --coop \
  --no-backfill --commit-timeout-secs 60 --sample-interval 0.5 \
  --messages "$NUM_MESSAGES" --out-dir "$OUT_DIR" --purge-data --timeout 900 \
  2>&1 | tee "$OUT_DIR/$name.log" || echo "!!! run $name failed or timed out"

docker compose stop "$SOURCE" && docker compose rm -f "$SOURCE"
sudo -n rm -rf "/mnt/bench/$SOURCE/..?*" "/mnt/bench/$SOURCE/.[!.]*" "/mnt/bench/$SOURCE/"* || true
python3 "$BENCH_DIR/summarize.py" --out-dir "$OUT_DIR"
