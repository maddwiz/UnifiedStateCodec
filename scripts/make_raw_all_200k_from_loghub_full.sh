#!/usr/bin/env bash
set -euo pipefail

N_LINES=200000
SRCROOT="data/loghub_full"
OUTDIR="results/raw_loghub_full_200k"

mkdir -p "${OUTDIR}"

echo "=== scanning ${SRCROOT} for .log files ==="
mapfile -t LOGS < <(find "${SRCROOT}" -type f -iname "*.log" | sort)

if [[ "${#LOGS[@]}" -eq 0 ]]; then
  echo "❌ no .log files found under ${SRCROOT}"
  exit 1
fi

echo "✅ found ${#LOGS[@]} .log files"

# Build 200k heads (or smaller if dataset < 200k)
n_out=0
for f in "${LOGS[@]}"; do
  base="$(basename "${f}")"
  # dataset name guess: folder name (parent dir)
  ds="$(basename "$(dirname "${f}")")"
  out="${OUTDIR}/${ds}_${N_LINES}.log"

  # Avoid overwriting if duplicates exist
  if [[ -f "${out}" ]]; then
    continue
  fi

  # Count lines quickly (wc -l)
  total="$(wc -l < "${f}" | tr -d ' ')"
  take="${N_LINES}"
  if [[ "${total}" -lt "${N_LINES}" ]]; then
    take="${total}"
  fi

  echo "✅ ${ds}: total=${total} -> take=${take}  src=${f}"
  head -n "${take}" "${f}" > "${out}"

  # sanity size
  ls -lh "${out}" | awk '{print "   -> " $5 " " $9}'
  n_out=$((n_out+1))
done

echo "=== done ==="
echo "✅ wrote ${n_out} datasets into ${OUTDIR}"
