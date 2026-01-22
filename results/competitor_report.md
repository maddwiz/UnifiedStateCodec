# USC vs CLP — Competitor Report (200k lines)

This report compares USC to CLP on the same 5 LogHub datasets (200,000 lines each).

## Compression ratio (higher is better)

| Dataset | CLP | USC-hot-lite-full | USC-cold | Winner (Queryable) | Winner (Max) |
|---|---:|---:|---:|---|---|
| Android | 15.96× | 11.78× | 21.15× | CLP | USC |
| Apache | 15.33× | 23.48× | 35.82× | USC | USC |
| BGL | 9.12× | 12.73× | 83.30× | USC | USC |
| HDFS | 13.92× | 12.98× | 56.66× | CLP | USC |
| Zookeeper | 34.55× | 32.27× | 80.76× | CLP | USC |

## CLP search time (lower is better)

Average time over 4 queries: `ERROR`, `Exception`, `WARN`, `INFO`.

| Dataset | Avg CLP search time |
|---|---:|
| Android | 0.627s |
| Apache | 0.473s |
| BGL | 0.875s |
| HDFS | 0.642s |
| Zookeeper | 0.541s |
## Baseline compression ratios (gzip-9 / zstd-19)
These are general-purpose baselines on the *same exact 200k raw logs*.

| Dataset | Raw size | gzip-9 | zstd-19 |
|---|---:|---:|---:|
| Android | 24,829,629 B | 7.43× | 21.01× |
| Apache | 5,135,876 B | 21.33× | 28.81× |
| BGL | 26,817,462 B | 10.84× | 18.78× |
| HDFS | 28,208,761 B | 11.61× | 16.80× |
| Zookeeper | 10,426,438 B | 25.99× | 38.98× |

