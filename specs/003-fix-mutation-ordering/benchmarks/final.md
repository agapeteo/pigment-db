# Final Mutation-Ordering Performance Gate

Final acceptance completed on 2026-08-06 on the host described in
`pre-feature.md`. All thresholds remained unchanged.

## Method

- Reconstructed the documented pre-issue-#3 WAL-first ordinary mutation paths
  on top of the issue #1/#2 working tree. This is a behaviorally reconstructed
  baseline because no intermediate Git commit existed.
- Used the same benchmark source for baseline and candidate.
- Preserved five warmups and 11 measured samples per cell.
- Required every sample to contain at least 1,024 public operations and 100 ms
  of measured work.
- Ran three complete 36-cell matrices for the baseline and candidate, then used
  the per-cell median across the three runs. This rejects the observed periodic
  host stalls, which affected both baseline and candidate in different cells.
- Compared every cell independently: one-worker throughput at least 90%,
  eight-worker throughput at least 85%, and p95 latency no more than 125%.

The reconstructed baseline store-source SHA-256 values were:

- key/value: `ff5b16b902efa245b6a900cb3cdd4c4d8cfa9ee0a3ecfb9cd36e3c50c29dcff4`
- key/set: `1199d703d4dd7b5f59502974a992d3b7ef7c84a549e685d6cfff8652478b33f4`
- key/map: `934590420d203f2a02c4ee950f536ba115abb545746d6732d578f60e26082bcf`

## Results

- Matrix: 36/36 cells passed.
- Lowest one-worker throughput ratio: 0.929685 (required 0.90).
- Lowest eight-worker throughput ratio: 0.923004 (required 0.85).
- Highest p95 latency ratio: 1.083975 (maximum 1.25).
- No threshold was weakened and no passing cells offset a failing cell.

The retained-memory candidate measured 393,216 bytes after 1,000 cycles and
138,043,392 bytes after 1,000,000 cycles, with zero retained live keys. After
subtracting the immutable baseline, added ordering retention was 163,840 bytes
and zero bytes respectively, passing the 110% historical-growth limit.

The set append hot path uses a borrowed-key WAL encoder. Its dedicated test
proves byte-for-byte equality with the legacy frame while avoiding a key clone
and second lookup under the guarded mutation path.

## Ordinary Mutation Throughput

The following table reports aggregate public write operations per second for
the reconstructed pre-issue-#3 baseline and the final implementation:

| Store | Backend | Workers | Before (writes/s) | Now (writes/s) | Change |
| --- | --- | ---: | ---: | ---: | ---: |
| Key/value | Vector | 1 | 1,833,330.612 | 1,905,782.598 | +3.95% |
| Key/value | Vector | 8 | 207,308.537 | 230,532.294 | +11.20% |
| Key/value | File | 1 | 192,398.636 | 193,155.595 | +0.39% |
| Key/value | File | 8 | 92,926.516 | 92,283.059 | -0.69% |
| Key/set | Vector | 1 | 1,347,839.865 | 1,253,066.912 | -7.03% |
| Key/set | Vector | 8 | 224,049.542 | 211,328.585 | -5.68% |
| Key/set | File | 1 | 183,712.696 | 176,146.250 | -4.12% |
| Key/set | File | 8 | 92,144.654 | 91,208.129 | -1.02% |
| Key/map | Vector | 1 | 1,317,054.490 | 1,339,165.557 | +1.68% |
| Key/map | Vector | 8 | 225,198.429 | 207,859.051 | -7.70% |
| Key/map | File | 1 | 180,919.901 | 180,708.666 | -0.12% |
| Key/map | File | 8 | 90,699.546 | 89,951.777 | -0.82% |

Eight-worker results are the combined throughput of all workers, not a
per-worker figure. File-backed results include Rust buffered-writer `flush`
behavior; they do not measure a physical `fsync` for every operation.

Machine-readable per-cell results and ratios are in `final.csv`.
