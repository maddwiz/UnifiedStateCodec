#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RAW_DIR="results/raw_real_suite16_200k"
OUT_DIR="results/suite16_200k_competitors"
mkdir -p "$OUT_DIR"

DATASETS=(Android_v2 Apache BGL HDFS_v2 HPC Hadoop HealthApp Linux Mac OpenStack Proxifier SSH Spark Thunderbird Windows Zookeeper)

have_cmd() { command -v "$1" >/dev/null 2>&1; }

echo "============================================"
echo "Suite16 Competitor Bench @ 200k lines"
echo "RAW_DIR: $RAW_DIR"
echo "OUT_DIR: $OUT_DIR"
echo "============================================"

for name in "${DATASETS[@]}"; do
  RAW="$RAW_DIR/${name}_200000.log"
  [[ -f "$RAW" ]] || { echo "SKIP $name (missing raw)"; continue; }

  echo
  echo "==================== $name ===================="

  # --- USC modes ---
  python3 -m usc.cli.app encode --mode hot           --log "$RAW" --lines 200000 --out "$OUT_DIR/${name}_hot.bin"
  python3 -m usc.cli.app encode --mode hot-lite-full --log "$RAW" --lines 200000 --out "$OUT_DIR/${name}_hot-lite-full.bin"
  python3 -m usc.cli.app encode --mode cold          --log "$RAW" --lines 200000 --out "$OUT_DIR/${name}_cold.bin"

  # --- gzip ---
  if have_cmd gzip; then
    gzip -9 -c "$RAW" > "$OUT_DIR/${name}.gz"
  fi

  # --- zstd ---
  if have_cmd zstd; then
    zstd -19 -q -f "$RAW" -o "$OUT_DIR/${name}.zst"

    # --- zstd+dict (train using 200k lines sample) ---
    head -n 200000 "$RAW" > "$OUT_DIR/${name}_train_200k.txt"

    # zstd trainer sometimes fails if sample set is too small/weird; don't crash suite
    zstd --train -q "$OUT_DIR/${name}_train_200k.txt" -o "$OUT_DIR/${name}.dict" || true

    if [[ -f "$OUT_DIR/${name}.dict" ]]; then
      zstd -19 -q -f --dict="$OUT_DIR/${name}.dict" "$RAW" -o "$OUT_DIR/${name}.zst_dict" || true
    fi
  fi
done

echo
echo "✅ competitor artifacts written to: $OUT_DIR"
