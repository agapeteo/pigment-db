# Final Performance Gate

Status: **FAILED — attempt 1 preserved; optimized retry required**.

The user approved the attempt-1 quiet-machine window at
`2026-08-21T02:05:50-05:00`. Three complete candidate matrices were captured
sequentially on CPUs `12-19` and paired ordinally with the three immutable
pre-feature matrices. All six matrices contain the required 36 cells and use
the frozen harness SHA-256
`c8ca6c94e6f38d54e456462bce2e8fad0d3cffa2b2457f4c2818129b1c62006c`.

## Attempt 1 result

- 30 of 36 cells passed.
- Every eight-worker throughput cell passed.
- Every median p95-latency cell passed.
- Six one-worker throughput cells failed the inclusive `0.90` floor.
- The release decision remains **NO-GO** until an optimized candidate passes a
  new complete capture; thresholds are unchanged.

## Per-cell medians

| Family | Backing | Profile | Workers | Baseline ops/s | Candidate ops/s | Throughput | Baseline p95 ns | Candidate p95 ns | p95 ratio | Verdict |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| key_map | file | minimal_callback | 1 | 366,758.430 | 360,954.463 | 0.984175 | 4,005 | 4,104 | 1.024719 | PASS |
| key_map | file | minimal_callback | 8 | 117,323.758 | 113,376.224 | 0.966353 | 123,379 | 132,848 | 1.076747 | PASS |
| key_map | file | ordinary_write | 1 | 556,992.650 | 529,874.457 | 0.951313 | 3,120 | 3,193 | 1.023397 | PASS |
| key_map | file | ordinary_write | 8 | 177,807.913 | 181,373.535 | 1.020053 | 81,975 | 81,936 | 0.999524 | PASS |
| key_map | file | successful_remove | 1 | 480,459.808 | 420,858.595 | 0.875950 | 2,819 | 3,372 | 1.196169 | FAIL |
| key_map | file | successful_remove | 8 | 61,159.105 | 62,399.334 | 1.020279 | 67,550 | 68,685 | 1.016802 | PASS |
| key_map | vector | minimal_callback | 1 | 1,307,657.181 | 1,278,478.006 | 0.977686 | 875 | 856 | 0.978286 | PASS |
| key_map | vector | minimal_callback | 8 | 217,642.575 | 219,323.631 | 1.007724 | 36,893 | 38,353 | 1.039574 | PASS |
| key_map | vector | ordinary_write | 1 | 2,561,879.420 | 2,517,578.131 | 0.982708 | 391 | 351 | 0.897698 | PASS |
| key_map | vector | ordinary_write | 8 | 503,651.085 | 498,637.278 | 0.990045 | 9,516 | 9,956 | 1.046238 | PASS |
| key_map | vector | successful_remove | 1 | 2,473,557.848 | 2,137,648.033 | 0.864200 | 509 | 475 | 0.933202 | FAIL |
| key_map | vector | successful_remove | 8 | 73,967.170 | 72,770.671 | 0.983824 | 9,361 | 9,294 | 0.992843 | PASS |
| key_set | file | minimal_callback | 1 | 433,815.711 | 420,640.258 | 0.969629 | 3,239 | 3,539 | 1.092621 | PASS |
| key_set | file | minimal_callback | 8 | 118,766.480 | 124,727.432 | 1.050191 | 105,628 | 106,230 | 1.005699 | PASS |
| key_set | file | ordinary_write | 1 | 550,299.211 | 458,613.176 | 0.833389 | 2,618 | 3,241 | 1.237968 | FAIL |
| key_set | file | ordinary_write | 8 | 179,076.647 | 162,836.893 | 0.909314 | 81,162 | 85,771 | 1.056788 | PASS |
| key_set | file | successful_remove | 1 | 532,704.353 | 495,285.121 | 0.929756 | 2,913 | 3,069 | 1.053553 | PASS |
| key_set | file | successful_remove | 8 | 60,458.128 | 59,749.491 | 0.988279 | 66,790 | 67,438 | 1.009702 | PASS |
| key_set | vector | minimal_callback | 1 | 1,919,788.328 | 1,663,649.702 | 0.866580 | 597 | 671 | 1.123953 | FAIL |
| key_set | vector | minimal_callback | 8 | 225,248.902 | 217,047.039 | 0.963588 | 34,632 | 35,571 | 1.027114 | PASS |
| key_set | vector | ordinary_write | 1 | 2,018,742.418 | 2,011,946.763 | 0.996634 | 458 | 497 | 1.085153 | PASS |
| key_set | vector | ordinary_write | 8 | 495,841.385 | 495,338.555 | 0.998986 | 8,023 | 8,604 | 1.072417 | PASS |
| key_set | vector | successful_remove | 1 | 2,755,784.103 | 2,652,588.859 | 0.962553 | 285 | 296 | 1.038596 | PASS |
| key_set | vector | successful_remove | 8 | 72,361.427 | 72,619.135 | 1.003561 | 7,025 | 7,457 | 1.061495 | PASS |
| key_value | file | minimal_callback | 1 | 574,540.040 | 568,257.207 | 0.989065 | 2,879 | 2,851 | 0.990274 | PASS |
| key_value | file | minimal_callback | 8 | 138,599.058 | 144,224.113 | 1.040585 | 80,532 | 84,497 | 1.049235 | PASS |
| key_value | file | ordinary_write | 1 | 598,072.119 | 571,237.306 | 0.955131 | 2,705 | 2,874 | 1.062477 | PASS |
| key_value | file | ordinary_write | 8 | 189,859.484 | 176,380.113 | 0.929003 | 74,539 | 77,819 | 1.044004 | PASS |
| key_value | file | successful_remove | 1 | 525,048.577 | 522,505.642 | 0.995157 | 2,524 | 2,560 | 1.014263 | PASS |
| key_value | file | successful_remove | 8 | 64,766.619 | 65,111.191 | 1.005320 | 65,150 | 66,352 | 1.018450 | PASS |
| key_value | vector | minimal_callback | 1 | 3,801,749.855 | 3,255,675.409 | 0.856362 | 246 | 268 | 1.089431 | FAIL |
| key_value | vector | minimal_callback | 8 | 277,258.628 | 281,927.816 | 1.016841 | 7,943 | 6,999 | 0.881153 | PASS |
| key_value | vector | ordinary_write | 1 | 3,870,481.545 | 3,447,334.593 | 0.890673 | 255 | 295 | 1.156863 | FAIL |
| key_value | vector | ordinary_write | 8 | 513,205.941 | 486,691.587 | 0.948336 | 8,314 | 8,515 | 1.024176 | PASS |
| key_value | vector | successful_remove | 1 | 3,075,184.772 | 3,132,005.459 | 1.018477 | 299 | 280 | 0.936455 | PASS |
| key_value | vector | successful_remove | 8 | 75,221.955 | 74,362.238 | 0.988571 | 7,081 | 7,519 | 1.061856 | PASS |

## Artifact checksums

The three immutable baseline checksums remain in `baseline.md`. Attempt-1
candidate checksums and full provenance are in `candidate.md`. All artifacts
remain preserved under `benchmarks/baseline/` and `benchmarks/candidate/`.

## Inclusive thresholds

- One-worker throughput ratio: at least `0.90`.
- Eight-worker distinct-key throughput ratio: at least `0.85`.
- Every p95 latency ratio: at most `1.25`.

All cells must pass; results cannot offset one another.
