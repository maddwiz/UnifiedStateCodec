# MASTER HANDOFF — Unified State Codec (USC)

## What USC is
USC is a domain-aware compression system for structured logs / agent traces.

It supports 3 working tiers:

### 1) HOT-LITE (PF1-only)
- Fast encode (~0.5s on HDFS 200k)
- FAST query (~86ms)
- ~22.65x compression on HDFS 200k lines
- No universal fallback

### 2) HOT (PF1 + PFQ1)
- FAST query first, then PFQ1 fallback
- Build time higher (~8–11s for PFQ1)
- ~11.29x compression on HDFS 200k lines
- Universal keyword search fallback

### 3) COLD (Bundle archive)
- Max compression archive mode
- ~55.64x on HDFS 200k lines
- Not query-focused

## HOT-LAZY (Premium UX)
HOT-LAZY starts as HOT-LITE and can upgrade itself to full HOT on demand:

- encode PF1-only into .usch
- query FAST
- if FAST hits=0 and --upgrade is used:
  - build PFQ1 once
  - rewrite the same .usch file with PFQ1 included
  - future queries use PFQ1 automatically

## Key bench result (HDFS 200k)
- USC-COLD: 503 KB (55.64x)
- USC-HOT-LITE: 1.24 MB (22.65x)
- zstd-10: 2.07 MB (13.52x)
- gzip-9: 2.43 MB (11.53x)
- USC-HOT: 2.48 MB (11.29x)

## Next immediate TODO
1) Pick a “value-only” query (block id) that FAST misses
2) Confirm PFQ1 hits it after upgrade
3) Commit HOT-LAZY + docs + milestone files
