# USC HOT-LAZY Mode

HOT-LAZY is the premium workflow:

- Encode FAST like HOT-LITE (PF1-only)
- Query FAST immediately (PF1 recall)
- If FAST returns 0 hits and `--upgrade` is used:
  - build PFQ1 fallback once
  - upgrade the same .usch file in-place
  - future queries use PFQ1 automatically

## Encode (HOT-LAZY)

PYTHONPATH=src python -m usc encode \
  --mode hot-lazy \
  --out hdfs_hot_lazy.usch \
  --lines 200000 \
  --packet_events 32768

Expected:
- ~22x compression
- ~0.5s build time

## Query (FAST-first)

PYTHONPATH=src python -m usc query \
  --hot hdfs_hot_lazy.usch \
  --q "IOException receiveBlock" \
  --limit 25

## Upgrade-on-demand (build PFQ1 once)

If FAST returns 0 hits, upgrade the file:

PYTHONPATH=src python -m usc query \
  --hot hdfs_hot_lazy.usch \
  --q "some keyword" \
  --limit 25 \
  --upgrade \
  --log data/loghub/HDFS.log \
  --tpl data/loghub/preprocessed/HDFS.log_templates.csv \
  --lines 200000 \
  --packet_events 32768

After upgrade:
- the .usch file becomes full HOT permanently
- future queries use PFQ1 fallback automatically
