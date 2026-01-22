#!/usr/bin/env bash
set -euo pipefail

N_LINES=200000
OUTDIR="results"

declare -a DATASETS=("Android" "Apache" "BGL" "HDFS" "Zookeeper")

echo "=== building raw ${N_LINES} logs into ${OUTDIR} ==="

for ds in "${DATASETS[@]}"; do
  SRC=""

  # Prefer the exact CLP 200k file if present (best + stable)
  if [[ -f "./results/clp/logs/${ds}_200k.log" ]]; then
    SRC="./results/clp/logs/${ds}_200k.log"
  # Else fall back to LogHub original
  elif [[ -f "./data/loghub/${ds}.log" ]]; then
    SRC="./data/loghub/${ds}.log"
  # Else as a last resort try any matching .log
  else
    SRC="$(find . -maxdepth 8 -type f | rg -i "${ds}.*\.log$" | head -n 1 || true)"
  fi

  if [[ -z "${SRC}" ]]; then
    echo "❌ ${ds}: could not find source log"
    continue
  fi

  OUT="${OUTDIR}/__raw_${ds}_${N_LINES}.log"
  echo "✅ ${ds}: source=${SRC}"
  echo "   -> writing ${OUT}"

  head -n "${N_LINES}" "${SRC}" > "${OUT}"
  ls -lh "${OUT}"
done

echo "=== done ==="
