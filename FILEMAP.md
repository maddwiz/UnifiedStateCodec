# FILEMAP — Unified State Codec (USC)

## Core Modules

### Packetization (USC v3b)
- `src/usc/mem/stream_proto_canz_v3b.py`
  - Builds dict packet + data packets
  - Encodes structured stream

### ZSTD Dictionary Support
- `src/usc/mem/zstd_trained_dict.py`
  - Safe dictionary training with conservative sizing
  - Plain zstd helpers

### Outer Stream Wrapper
- `src/usc/mem/outerstream_zstd.py`
  - Frames packets into one byte stream
  - Compress/decompress outer layer (plain zstd)

### ODC (Outer Dictionary Codec)
- `src/usc/api/codec_odc.py`
  - ODC encode/decode API
  - Text -> packets -> ODC blob
  - Blob -> packets

## Benchmarks
- `src/usc/bench/stream_bench19_outerstream.py`
  - USC packets + outer zstd pass
- `src/usc/bench/stream_bench20_outerstream_dict.py`
  - OuterStream framed + trained dict zstd
- `src/usc/bench/stream_bench21_odc_roundtrip.py`
  - ODC encode/decode roundtrip test

## Datasets
- `src/usc/bench/datasets_real_agent_trace.py`
  - Synthetic “real-ish” agent trace generator
