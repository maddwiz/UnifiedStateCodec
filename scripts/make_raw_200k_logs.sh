#!/usr/bin/env bash
set -euo pipefail

LINES=200000
OUTDIR="results"

declare -a DATASETS=("Android" "Apache" "BGL" "HDFS" "Zookeeper")

echo "=== building raw 200k logs into $OUTDIR ==="

for ds in "${DATASETS[@]}"; do
  # Find a source .log file anywhere in repo
  SRC="$(find . -maxdepth 8 -type f | rg -i "${ds}.*\.log$" | head -n 1 || true)"

  if [[ -z "${SRC}" ]]; then
    echo "❌ ${ds}: could not find source .log file"
    continue
  fi

  OUT="${OUTDIR}/__raw_${ds}_${LINES}.log"
  echo "✅ ${ds}: source=${SRC}"
  echo "   -> writing ${OUT}"

  # make 200k exact
  head -n "${LINES}" "${SRC}" > "${OUT}"

  # show size
  ls -lh "${OUT}"
done

echo "=== done ==="
