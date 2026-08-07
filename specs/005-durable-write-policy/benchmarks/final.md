# Final Durability Performance Report

Status: **PASS — all 54/54 acceptance comparisons passed**

Passing capture: `protocol-v5-20260807-220443`

Raw paired evidence: `final.csv`, byte-identical to `attempts/protocol-v5-20260807-220443.csv`.

SHA-256: `d3292e8d2dc73f4185ed1ec917a29bc23bcc627238f7a8effa0ee0be5183016e`

## Acceptance Summary

| Matrix | Passing | Required | Verdict |
|---|---:|---:|---|
| Buffered pre-feature/candidate | 36 | 36 | PASS |
| Append-plus-barrier/physical candidate | 18 | 18 | PASS |
| **Total** | **54** | **54** | **PASS** |

Acceptance uses the median operations/second and median sample-p95 from each side's eleven measured samples. One-worker throughput must be at least 0.90 of its comparator, eight-worker throughput at least 0.85, and candidate p95 no more than 1.25 of its comparator. Pair-level ratios are diagnostic only.

The lowest throughput ratio was `0.949319` (buffered/key_map/file/ordinary_write/8 workers). The highest latency ratio was `1.065339` (buffered/key_set/file/successful_remove/8 workers).

## Provenance and Validation

- Pre-feature commit: `6d7edc7c29a60a94c59effeeb2b78d8b95038135`; dirty-state hash `default-hasher:e33e9641d6e15aea`.
- Candidate commit: `6d7edc7c29a60a94c59effeeb2b78d8b95038135`; dirty-state hash `default-hasher:f20647ba796d9a0c`.
- Toolchain: `rustc 1.97.0 (2d8144b78 2026-07-07)`, target `x86_64-unknown-linux-gnu`.
- Host: `Linux 7.0.11-76070011-generic x86_64`, `Intel(R) Core(TM) Ultra 7 155H`.
- Root: `/tmp/pigment-db-durability-bench` on ext4; process affinity `12-19`, verified as eight distinct 3,800 MHz physical cores.
- Protocol: one release process linking both versions; five warmup and eleven measured AB/BA pairs per comparison; start-only buffered scheduling and per-operation physical/reference scheduling.
- Raw validation: 1,188 data rows, 54 unique comparison cells, 594 pair groups, exact alternating order, both variants in every pair, and zero failed operations.
- Staged/imported/final CSV copies are byte-identical by `cmp` and SHA-256.

## Buffered Comparisons — 36 Display Rows

| Store | Storage | Workload | Workers | Baseline ops/s | Candidate ops/s | Throughput ratio | Baseline p95 ns | Candidate p95 ns | Latency ratio | Verdict |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| key_value | vector | ordinary_write | 1 | 1866482.477 | 1872823.743 | 1.003397 | 473 | 471 | 0.995772 | PASS |
| key_value | vector | ordinary_write | 8 | 821936.860 | 795091.022 | 0.967338 | 22171 | 21914 | 0.988408 | PASS |
| key_value | vector | successful_remove | 1 | 1663473.694 | 1654046.117 | 0.994333 | 559 | 559 | 1.000000 | PASS |
| key_value | vector | successful_remove | 8 | 328866.182 | 329238.061 | 1.001131 | 20281 | 19923 | 0.982348 | PASS |
| key_value | vector | minimal_callback | 1 | 1716460.982 | 1797795.114 | 1.047385 | 523 | 499 | 0.954111 | PASS |
| key_value | vector | minimal_callback | 8 | 808394.916 | 789841.869 | 0.977050 | 21011 | 19687 | 0.936985 | PASS |
| key_value | file | ordinary_write | 1 | 406084.558 | 408067.317 | 1.004883 | 2672 | 2574 | 0.963323 | PASS |
| key_value | file | ordinary_write | 8 | 200773.047 | 197401.001 | 0.983205 | 114364 | 105953 | 0.926454 | PASS |
| key_value | file | successful_remove | 1 | 459827.002 | 456471.357 | 0.992702 | 2185 | 2235 | 1.022883 | PASS |
| key_value | file | successful_remove | 8 | 122503.106 | 117701.807 | 0.960807 | 114255 | 120326 | 1.053136 | PASS |
| key_value | file | minimal_callback | 1 | 406529.760 | 401803.393 | 0.988374 | 2578 | 2675 | 1.037626 | PASS |
| key_value | file | minimal_callback | 8 | 197484.605 | 200792.046 | 1.016748 | 114852 | 114432 | 0.996343 | PASS |
| key_set | vector | ordinary_write | 1 | 1014002.310 | 1006415.928 | 0.992518 | 825 | 822 | 0.996364 | PASS |
| key_set | vector | ordinary_write | 8 | 749625.815 | 787421.375 | 1.050419 | 20640 | 21975 | 1.064680 | PASS |
| key_set | vector | successful_remove | 1 | 1209947.972 | 1184811.030 | 0.979225 | 631 | 647 | 1.025357 | PASS |
| key_set | vector | successful_remove | 8 | 802677.081 | 832485.106 | 1.037136 | 21503 | 20960 | 0.974748 | PASS |
| key_set | vector | minimal_callback | 1 | 668726.066 | 683507.895 | 1.022104 | 1676 | 1661 | 0.991050 | PASS |
| key_set | vector | minimal_callback | 8 | 369828.282 | 364889.617 | 0.986646 | 64817 | 65211 | 1.006079 | PASS |
| key_set | file | ordinary_write | 1 | 348296.756 | 348035.007 | 0.999248 | 3012 | 2979 | 0.989044 | PASS |
| key_set | file | ordinary_write | 8 | 189264.992 | 190908.488 | 1.008684 | 114854 | 109917 | 0.957015 | PASS |
| key_set | file | successful_remove | 1 | 380327.139 | 378451.672 | 0.995069 | 2832 | 2828 | 0.998588 | PASS |
| key_set | file | successful_remove | 8 | 194526.495 | 193226.803 | 0.993319 | 105159 | 112030 | 1.065339 | PASS |
| key_set | file | minimal_callback | 1 | 312644.767 | 308018.028 | 0.985201 | 3693 | 3676 | 0.995397 | PASS |
| key_set | file | minimal_callback | 8 | 152310.856 | 155435.718 | 1.020516 | 114406 | 114174 | 0.997972 | PASS |
| key_map | vector | ordinary_write | 1 | 1276301.770 | 1307346.659 | 1.024324 | 736 | 746 | 1.013587 | PASS |
| key_map | vector | ordinary_write | 8 | 1013399.037 | 1006501.727 | 0.993194 | 15132 | 15664 | 1.035157 | PASS |
| key_map | vector | successful_remove | 1 | 945711.698 | 940432.695 | 0.994418 | 1126 | 1129 | 1.002664 | PASS |
| key_map | vector | successful_remove | 8 | 698689.775 | 697853.285 | 0.998803 | 27620 | 28004 | 1.013903 | PASS |
| key_map | vector | minimal_callback | 1 | 526941.893 | 516548.723 | 0.980276 | 1891 | 1922 | 1.016393 | PASS |
| key_map | vector | minimal_callback | 8 | 321633.432 | 328161.085 | 1.020295 | 74287 | 74170 | 0.998425 | PASS |
| key_map | file | ordinary_write | 1 | 410021.307 | 405063.425 | 0.987908 | 3289 | 3453 | 1.049863 | PASS |
| key_map | file | ordinary_write | 8 | 194225.906 | 184382.418 | 0.949319 | 102378 | 101462 | 0.991053 | PASS |
| key_map | file | successful_remove | 1 | 352398.927 | 348967.965 | 0.990264 | 3265 | 3426 | 1.049311 | PASS |
| key_map | file | successful_remove | 8 | 173831.895 | 176503.334 | 1.015368 | 107917 | 101678 | 0.942187 | PASS |
| key_map | file | minimal_callback | 1 | 276886.980 | 275142.091 | 0.993698 | 5700 | 5880 | 1.031579 | PASS |
| key_map | file | minimal_callback | 8 | 135962.507 | 137268.647 | 1.009607 | 142302 | 136948 | 0.962376 | PASS |

## Physical Candidate — 18 Display Rows

| Store | Workload | Workers | Candidate ops/s | Candidate p95 ns | Reference ops/s | Reference p95 ns | Throughput ratio | Latency ratio | Verdict |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| key_value | ordinary_write | 1 | 978.796 | 1017263 | 985.078 | 1022115 | 0.993623 | 0.995253 | PASS |
| key_value | ordinary_write | 8 | 953.153 | 12834945 | 967.005 | 12736775 | 0.985675 | 1.007708 | PASS |
| key_value | successful_remove | 1 | 985.408 | 1003887 | 980.479 | 1011506 | 1.005027 | 0.992468 | PASS |
| key_value | successful_remove | 8 | 932.888 | 12842542 | 948.113 | 12882118 | 0.983942 | 0.996928 | PASS |
| key_value | minimal_callback | 1 | 990.095 | 1017303 | 992.111 | 1032118 | 0.997969 | 0.985646 | PASS |
| key_value | minimal_callback | 8 | 947.954 | 13164238 | 950.474 | 12783974 | 0.997349 | 1.029745 | PASS |
| key_set | ordinary_write | 1 | 983.478 | 1024745 | 987.861 | 1011134 | 0.995563 | 1.013461 | PASS |
| key_set | ordinary_write | 8 | 942.526 | 12852159 | 956.968 | 12989263 | 0.984908 | 0.989445 | PASS |
| key_set | successful_remove | 1 | 977.410 | 1020591 | 978.699 | 1024756 | 0.998682 | 0.995936 | PASS |
| key_set | successful_remove | 8 | 936.673 | 12964753 | 955.492 | 12726711 | 0.980305 | 1.018704 | PASS |
| key_set | minimal_callback | 1 | 976.087 | 1032608 | 985.504 | 1015973 | 0.990444 | 1.016373 | PASS |
| key_set | minimal_callback | 8 | 937.435 | 13066468 | 956.886 | 12861495 | 0.979673 | 1.015937 | PASS |
| key_map | ordinary_write | 1 | 986.303 | 1022343 | 987.342 | 1025996 | 0.998948 | 0.996440 | PASS |
| key_map | ordinary_write | 8 | 948.354 | 12935925 | 955.701 | 13528827 | 0.992313 | 0.956175 | PASS |
| key_map | successful_remove | 1 | 981.107 | 1012782 | 985.748 | 1012773 | 0.995293 | 1.000009 | PASS |
| key_map | successful_remove | 8 | 941.968 | 11003195 | 958.300 | 12732491 | 0.982957 | 0.864182 | PASS |
| key_map | minimal_callback | 1 | 971.438 | 1042378 | 986.130 | 1008319 | 0.985101 | 1.033778 | PASS |
| key_map | minimal_callback | 8 | 950.132 | 12753192 | 948.193 | 13602703 | 1.002045 | 0.937548 | PASS |

## Append-Plus-Barrier Reference — 18 Display Rows

| Store | Workload | Workers | Reference ops/s | Reference p95 ns | Paired candidate ops/s | Paired candidate p95 ns | Candidate throughput ratio | Candidate latency ratio | Verdict |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| key_value | ordinary_write | 1 | 985.078 | 1022115 | 978.796 | 1017263 | 0.993623 | 0.995253 | PASS |
| key_value | ordinary_write | 8 | 967.005 | 12736775 | 953.153 | 12834945 | 0.985675 | 1.007708 | PASS |
| key_value | successful_remove | 1 | 980.479 | 1011506 | 985.408 | 1003887 | 1.005027 | 0.992468 | PASS |
| key_value | successful_remove | 8 | 948.113 | 12882118 | 932.888 | 12842542 | 0.983942 | 0.996928 | PASS |
| key_value | minimal_callback | 1 | 992.111 | 1032118 | 990.095 | 1017303 | 0.997969 | 0.985646 | PASS |
| key_value | minimal_callback | 8 | 950.474 | 12783974 | 947.954 | 13164238 | 0.997349 | 1.029745 | PASS |
| key_set | ordinary_write | 1 | 987.861 | 1011134 | 983.478 | 1024745 | 0.995563 | 1.013461 | PASS |
| key_set | ordinary_write | 8 | 956.968 | 12989263 | 942.526 | 12852159 | 0.984908 | 0.989445 | PASS |
| key_set | successful_remove | 1 | 978.699 | 1024756 | 977.410 | 1020591 | 0.998682 | 0.995936 | PASS |
| key_set | successful_remove | 8 | 955.492 | 12726711 | 936.673 | 12964753 | 0.980305 | 1.018704 | PASS |
| key_set | minimal_callback | 1 | 985.504 | 1015973 | 976.087 | 1032608 | 0.990444 | 1.016373 | PASS |
| key_set | minimal_callback | 8 | 956.886 | 12861495 | 937.435 | 13066468 | 0.979673 | 1.015937 | PASS |
| key_map | ordinary_write | 1 | 987.342 | 1025996 | 986.303 | 1022343 | 0.998948 | 0.996440 | PASS |
| key_map | ordinary_write | 8 | 955.701 | 13528827 | 948.354 | 12935925 | 0.992313 | 0.956175 | PASS |
| key_map | successful_remove | 1 | 985.748 | 1012773 | 981.107 | 1012782 | 0.995293 | 1.000009 | PASS |
| key_map | successful_remove | 8 | 958.300 | 12732491 | 941.968 | 11003195 | 0.982957 | 0.864182 | PASS |
| key_map | minimal_callback | 1 | 986.130 | 1008319 | 971.438 | 1042378 | 0.985101 | 1.033778 | PASS |
| key_map | minimal_callback | 8 | 948.193 | 13602703 | 950.132 | 12753192 | 1.002045 | 0.937548 | PASS |

The three result sections contain exactly 72 display rows: 36 buffered comparisons, 18 physical candidates, and 18 matching references.

## Preserved Earlier Attempts

- Protocol v1 `candidate-20260807-060515`: failed `44/54`; its physical start-only comparison was invalidated and preserved.
- Protocol v2 `20260807-102902`: failed `52/54`; physical passed, while two buffered p95 cells exposed schedule-induced tail noise.
- Protocol v3 `20260807-132706`: execution failed after measurement before CSV persistence; no verdict was reconstructed.
- Protocol v3 `20260807-145653`: failed `40/54` against a drifted historical buffered comparator.
- Protocol v4 `20260807-155736`: failed `50/54`; T263 later showed all four failed cells pass under same-process counterbalancing.
- T263 remains a focused diagnostic and was not promoted or merged into protocol-v5 evidence.

No threshold was weakened, and no samples from different attempts were merged.
