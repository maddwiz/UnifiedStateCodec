# Scoreboard — 200k lines (USC vs CLP vs gzip/zstd)

Single-table comparison on the *same exact* 200k raw logs.

**Queryable** = supports keyword search without full decompression.

## Compression ratios (higher is better)

| Dataset | gzip-9 | zstd-19 | CLP (Queryable) | USC-hot-lite-full (Queryable) | USC-cold (Max) | Queryable Winner | Max Winner |
|---|---:|---:|---:|---:|---:|---|---|
| Android | 7.43× | 21.01× | 15.96× | 11.78× | 21.15× | CLP | USC |
| Apache | 21.33× | 28.81× | 15.33× | 23.48× | 35.82× | USC | USC |
| BGL | 10.84× | 18.78× | 9.12× | 12.73× | 83.30× | USC | USC |
| HDFS | 11.61× | 16.80× | 13.92× | 12.98× | 56.66× | CLP | USC |
| Zookeeper | 25.99× | 38.98× | 34.55× | 32.27× | 80.76× | CLP | USC |

## Query speed (lower is better)

Average time over 4 queries: `ERROR`, `Exception`, `WARN`, `INFO`.

| Dataset | CLP avg search time | USC avg search time | Speed Winner |
|---|---:|---:|---|
| Android | 0.627s | 0.038s | USC |
| Apache | 0.473s | 0.037s | USC |
| BGL | 0.875s | 0.037s | USC |
| HDFS | 0.642s | 0.034s | USC |
| Zookeeper | 0.541s | 0.030s | USC |
