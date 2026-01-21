# USC CLI

## Encode

### HOT (queryable)
Creates a `.usch` file containing:
- PF1 (fast EventID recall)
- PFQ1 (bloom-index fallback search)

Example:
PYTHONPATH=src python -m usc encode --mode hot --out hdfs_hot.usch --lines 200000 --packet_events 32768

Query:
PYTHONPATH=src python -m usc query --hot hdfs_hot.usch --q "IOException receiveBlock" --limit 25


### COLD (max compression archive)
Creates a `.uscc` file containing:
- TPLv1M bundle (self-contained, extreme compression)

Example:
PYTHONPATH=src python -m usc encode --mode cold --out hdfs_cold.uscc --lines 200000


## Bench (scoreboard)
Runs gzip/zstd baselines and USC modes and prints a size/ratio table.

Example:
PYTHONPATH=src python -m usc bench --lines 200000 --packet_events 32768 --out_json results_hdfs_200k.json
