#!/usr/bin/env bash
set -euo pipefail

SRC="data/loghub_real_zenodo_v8"
DST="data/loghub_real_v8_extracted"
mkdir -p "${DST}"

echo "=== extracting into ${DST} ==="

for f in "${SRC}"/*; do
  bn="$(basename "$f")"
  echo "📦 ${bn}"

  if [[ "${bn}" == *.tar.gz ]]; then
    d="${DST}/${bn%.tar.gz}"
    mkdir -p "${d}"
    tar -xzf "${f}" -C "${d}"
  elif [[ "${bn}" == *.zip ]]; then
    d="${DST}/${bn%.zip}"
    mkdir -p "${d}"
    unzip -q "${f}" -d "${d}"
  else
    echo "⚠️ unknown archive type: ${bn}"
  fi
done

echo "✅ extraction complete"
echo "Top-level folders:"
ls -1 "${DST}"
