#!/usr/bin/env bash
set -euo pipefail

mkdir -p data

if [[ -d data/loghub_full/.git ]]; then
  echo "✅ data/loghub_full already exists"
  exit 0
fi

echo "=== cloning LogHub full dataset repo ==="
git clone --depth 1 https://github.com/logpai/loghub.git data/loghub_full
echo "✅ cloned into data/loghub_full"
