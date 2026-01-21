# USC Modes (Hot vs Cold)

USC supports two primary storage modes:

## 1) HOT (USCH) — Queryable Storage
HOT mode stores two payloads in one file:

- PF1: fast selective recall by template EventID (FAST mode)
- PFQ1: bloom-index fallback query (slow build, universal search)

This makes HOT storage usable for agent memory:
- fast queries
- selective decode
- still lossless

### Build
- PF1 is fast to build
- PFQ1 is slower to build (more indexing work)

### Query
A router is used:
FAST → PFQ1 fallback


## 2) COLD (USCC) — Maximum Compression Archive
COLD mode uses TPLv1M bundle:
- extreme compression ratio
- fully self-contained (includes templates inside blob)
- best for saving huge archives

Downside:
- not optimized for fast searching (you decode when needed)


## Recommended Use
- HOT for active agent memory / searchable history
- COLD for long-term storage snapshots
