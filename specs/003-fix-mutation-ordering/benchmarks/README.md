# Mutation Ordering Benchmark Artifacts

These files are the immutable pre-feature measurements for issue #3. They were
captured after adding only test infrastructure and before changing any release
mutation path.

## Commands

```bash
cargo test --release --test mutation_ordering performance::paired_baseline -- --exact --ignored --nocapture --test-threads=1
```

The harness runs five untimed warmups and 11 measured samples per cell. Inputs
use 32-byte keys/values, setup occurs before timing, and eight-worker samples
start behind a barrier. Throughput is the median sample throughput; latency is
the p95 across measured public calls.

## Performance Schema

`pre-feature.csv` contains exactly 36 unique rows:

```text
store,mode,profile,workers,samples,ops_per_sample,median_throughput,p95_latency_ns
```

The Cartesian matrix is three stores, two storage modes, three profiles, and
one or eight workers. Candidate rows must be paired by those first four fields.

## Memory Schema

`pre-feature-memory.csv` contains the 1,000 and 1,000,000 unique-key
create/delete samples:

```text
store,mode,profile,cycles,rss_before_bytes,rss_after_bytes,rss_delta_bytes,retained_keys
```

RSS is read from Linux `/proc/self/status`; `retained_keys` is the public store
size after every create/delete pair. Candidate comparisons must run on the same
host/session and distinguish pre-existing vector-WAL and allocator retention
from ordering-specific retained state.
