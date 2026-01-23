#!/usr/bin/env bash
set -euo pipefail

REC="8196385"
OUTDIR="data/loghub_real_zenodo_v8"
mkdir -p "${OUTDIR}"
cd "${OUTDIR}"

FILES=(
  "Android_v2.zip"
  "Apache.tar.gz"
  "BGL.zip"
  "Hadoop.zip"
  "HDFS_v2.zip"
  "HealthApp.tar.gz"
  "HPC.zip"
  "Linux.tar.gz"
  "Mac.tar.gz"
  "OpenStack.tar.gz"
  "Proxifier.tar.gz"
  "Spark.tar.gz"
  "SSH.tar.gz"
  "Thunderbird.tar.gz"
  "Windows.tar.gz"
  "Zookeeper.tar.gz"
)

echo "=== downloading LogHub REAL v8 from Zenodo record ${REC} ==="

for f in "${FILES[@]}"; do
  if [[ -f "${f}" ]]; then
    echo "✅ already have ${f}"
    continue
  fi
  url="https://zenodo.org/records/${REC}/files/${f}?download=1"
  echo "⬇️  ${f}"
  wget -q --show-progress -O "${f}" "${url}"
done

echo "✅ done downloading into: ${OUTDIR}"
ls -lh
