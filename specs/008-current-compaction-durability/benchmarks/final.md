# Final Performance Gate

Status: **PASS — 36/36 cells GREEN**.

Release decision: **GO** for the inactive-compaction performance requirement.
The user approved the quiet-machine window from
`2026-08-21T10:21:14-05:00` through `2026-08-21T10:43:59-05:00`. The final
protocol-complete matrices finished between `10:34:43` and `10:43:53` on CPUs
`12-19`.

## Capture validity and retry history

- Attempt 1 compared the pre-optimization candidate with the prior-day frozen
  baseline and failed six one-worker throughput cells (30/36 passed). Its three
  complete matrices remain preserved as `candidate-1` through `candidate-3`.
- The optimized ordinal retry also compared against the prior-day timings. It
  passed 33/36 cells but failed key/set file ordinary write (`0.897739`),
  key/set file callback (`0.855966`), and key/map vector remove (`0.847392`).
  All eight-worker and p95 cells passed. Its complete matrices remain preserved
  as `candidate-retry-1` through `candidate-retry-3`.
- A non-acceptance reconstructed-baseline diagnostic showed that all three
  ordinal-retry failures passed against the same pre-feature binary on the
  current host, while unrelated one-worker cells moved with run order. This
  demonstrated temporal CPU-frequency/host drift rather than a stable defect.
- The governing contract requires counterbalancing on the same quiet pinned
  host where practical. The final retry therefore reconstructed the frozen
  pre-feature commit and ran six sequential matrices in `B1-C1 / C2-B2 /
  B3-C3` order. No processes overlapped, and no observed run or cell was
  dropped or selectively recaptured.

All final CSVs contain exactly 36 unique cells. Baseline commit
`a7c8281f72e25c177a142be99285faead7335e01` and candidate commit
`180e16a965285dd3edfc494bfdeff2b1fe7dcd3c` used the same Rust/Cargo
toolchain, Btrfs data placement, release flags, CPU affinity, payload, warmups,
sample rules, and frozen harness SHA-256
`c8ca6c94e6f38d54e456462bce2e8fad0d3cffa2b2457f4c2818129b1c62006c`.
The candidate's production optimization is commit
`19ecb1d7efaceecf2199f0458b50d3faebc0b3da`; the later commit changes only
benchmark documentation.

## Final result

- All 36/36 independent cells pass.
- Lowest one-worker throughput ratio: `0.913449` (required `>= 0.90`).
- Lowest eight-worker throughput ratio: `0.900328` (required `>= 0.85`).
- Highest p95-latency ratio: `1.098007` (required `<= 1.25`).
- Retained-memory checks completed successfully for all six final runs.

## Per-cell medians

| Family | Backing | Profile | Workers | Baseline ops/s | Candidate ops/s | Throughput | Baseline p95 ns | Candidate p95 ns | p95 ratio | Verdict |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| key_value | vector | ordinary_write | 1 | 3738487.174 | 3736902.601 | 0.999576 | 228 | 233 | 1.021930 | PASS |
| key_value | vector | ordinary_write | 8 | 469265.821 | 487867.001 | 1.039639 | 7872 | 8509 | 1.080920 | PASS |
| key_value | vector | successful_remove | 1 | 3379227.499 | 3359446.439 | 0.994146 | 265 | 269 | 1.015094 | PASS |
| key_value | vector | successful_remove | 8 | 68120.477 | 71338.269 | 1.047237 | 6433 | 6732 | 1.046479 | PASS |
| key_value | vector | minimal_callback | 1 | 3733929.158 | 3433508.395 | 0.919543 | 249 | 253 | 1.016064 | PASS |
| key_value | vector | minimal_callback | 8 | 276521.170 | 274356.364 | 0.992171 | 8192 | 7723 | 0.942749 | PASS |
| key_value | file | ordinary_write | 1 | 550119.555 | 530399.358 | 0.964153 | 2885 | 2867 | 0.993761 | PASS |
| key_value | file | ordinary_write | 8 | 178316.959 | 184972.874 | 1.037326 | 76094 | 75606 | 0.993587 | PASS |
| key_value | file | successful_remove | 1 | 519008.163 | 538836.568 | 1.038204 | 2631 | 2583 | 0.981756 | PASS |
| key_value | file | successful_remove | 8 | 60551.587 | 65404.411 | 1.080144 | 66100 | 65278 | 0.987564 | PASS |
| key_value | file | minimal_callback | 1 | 565339.744 | 543366.094 | 0.961132 | 2795 | 2844 | 1.017531 | PASS |
| key_value | file | minimal_callback | 8 | 129720.750 | 135411.503 | 1.043869 | 85340 | 84120 | 0.985704 | PASS |
| key_set | vector | ordinary_write | 1 | 1893267.826 | 1948445.298 | 1.029144 | 527 | 488 | 0.925996 | PASS |
| key_set | vector | ordinary_write | 8 | 505124.587 | 472252.255 | 0.934922 | 7743 | 8084 | 1.044040 | PASS |
| key_set | vector | successful_remove | 1 | 2884634.981 | 2768526.401 | 0.959749 | 279 | 288 | 1.032258 | PASS |
| key_set | vector | successful_remove | 8 | 72553.622 | 72739.346 | 1.002560 | 5927 | 6431 | 1.085035 | PASS |
| key_set | vector | minimal_callback | 1 | 1904775.105 | 1739915.511 | 0.913449 | 602 | 661 | 1.098007 | PASS |
| key_set | vector | minimal_callback | 8 | 215993.468 | 227747.878 | 1.054420 | 35031 | 36903 | 1.053438 | PASS |
| key_set | file | ordinary_write | 1 | 515386.807 | 471694.392 | 0.915224 | 2960 | 3058 | 1.033108 | PASS |
| key_set | file | ordinary_write | 8 | 181351.033 | 179583.196 | 0.990252 | 83259 | 83987 | 1.008744 | PASS |
| key_set | file | successful_remove | 1 | 529937.329 | 489788.790 | 0.924239 | 3058 | 3025 | 0.989209 | PASS |
| key_set | file | successful_remove | 8 | 65481.461 | 60030.360 | 0.916754 | 66396 | 66317 | 0.998810 | PASS |
| key_set | file | minimal_callback | 1 | 398511.635 | 402397.465 | 1.009751 | 3630 | 3462 | 0.953719 | PASS |
| key_set | file | minimal_callback | 8 | 120828.337 | 123877.078 | 1.025232 | 106541 | 106826 | 1.002675 | PASS |
| key_map | vector | ordinary_write | 1 | 2629830.664 | 2641346.635 | 1.004379 | 353 | 341 | 0.966006 | PASS |
| key_map | vector | ordinary_write | 8 | 507824.156 | 495851.869 | 0.976424 | 9087 | 9827 | 1.081435 | PASS |
| key_map | vector | successful_remove | 1 | 1945303.833 | 2523746.012 | 1.297353 | 565 | 416 | 0.736283 | PASS |
| key_map | vector | successful_remove | 8 | 74513.304 | 70352.676 | 0.944163 | 9499 | 9267 | 0.975576 | PASS |
| key_map | vector | minimal_callback | 1 | 1355254.819 | 1449159.510 | 1.069289 | 826 | 820 | 0.992736 | PASS |
| key_map | vector | minimal_callback | 8 | 215965.156 | 194439.448 | 0.900328 | 38608 | 37584 | 0.973477 | PASS |
| key_map | file | ordinary_write | 1 | 537675.940 | 550105.292 | 1.023117 | 3158 | 2992 | 0.947435 | PASS |
| key_map | file | ordinary_write | 8 | 176135.544 | 182148.517 | 1.034138 | 80564 | 84663 | 1.050879 | PASS |
| key_map | file | successful_remove | 1 | 454342.299 | 442344.325 | 0.973593 | 3181 | 3284 | 1.032380 | PASS |
| key_map | file | successful_remove | 8 | 60824.766 | 64286.460 | 1.056913 | 68702 | 68173 | 0.992300 | PASS |
| key_map | file | minimal_callback | 1 | 358538.975 | 355874.751 | 0.992569 | 4043 | 4165 | 1.030176 | PASS |
| key_map | file | minimal_callback | 8 | 108997.174 | 111860.619 | 1.026271 | 123829 | 135092 | 1.090956 | PASS |

## Final artifact checksums

| Artifact | SHA-256 |
|---|---|
| `baseline/baseline-retry-p2-1.csv` | `7bf93e03b7bf095c4d2d1f37cd29b1bda0bae00c26b1d289cdaed94656a619fc` |
| `baseline/baseline-retry-p2-1.csv.metadata` | `09ff25cd9cb116ec28b41c31eaa0d8af0ea33840910a224131c2feccae9b1550` |
| `baseline/baseline-retry-p2-2.csv` | `ebd8f35c5d44b9380337e25b3111fa78fb69889b75b1a97823592ecdd4901cf0` |
| `baseline/baseline-retry-p2-2.csv.metadata` | `9972736b7321dbfcd1c528ffc6c99ff64bbed90102245279bbc06c33e05083d9` |
| `baseline/baseline-retry-p2-3.csv` | `04c53c3f97a79ede8cac9b2c8cef0ed35880d8ddc162ff8657407a62ce75db8c` |
| `baseline/baseline-retry-p2-3.csv.metadata` | `61a834283310448da492ddf282afa9cefca8625928d670cc111d472a7c8d8bd8` |
| `candidate/candidate-retry-p2-1.csv` | `7af6d5e5afdff52fd489992a8cb24757d12902e869371fae2ecbc4b13645ad68` |
| `candidate/candidate-retry-p2-1.csv.metadata` | `ffdd035a41758613768fbef6fe183716eb32efc5c2fdc09fe0a881eddc8d23b7` |
| `candidate/candidate-retry-p2-2.csv` | `6d643205fcfa4c7b24245b69900e1804587965fc4974b3f18942004e3a0c5718` |
| `candidate/candidate-retry-p2-2.csv.metadata` | `c6b889bd7600439f154cbf004dc5be19a0429c7bf62c5b7b57961235fb540282` |
| `candidate/candidate-retry-p2-3.csv` | `2ac6186ce98e1b2f8714c05c0b5206e85299e8d6735ea9e852d6ae8f1a997361` |
| `candidate/candidate-retry-p2-3.csv.metadata` | `425553be3e276672acd1c07b95b061bedc5f16be326987dc3483007649dd32df` |

## Inclusive thresholds

- One-worker throughput ratio: at least `0.90`.
- Eight-worker distinct-key throughput ratio: at least `0.85`.
- Every p95 latency ratio: at most `1.25`.

All cells pass independently; no result offsets another.
