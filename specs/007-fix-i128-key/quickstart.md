# Quickstart: Validate Full-Range Signed I128 Keys

## Prerequisites

- Run from the repository root on branch `codex/010-fix-i128-key`.
- Keep frozen historical fixtures unchanged; tests verify their fixed bytes or digest.

## 1. Public signed-domain behavior

```bash
cargo test --test i128_key signed_i128 -- --nocapture
```

Expected: signed minimum, negative, zero, above-`u64` maximum, and signed maximum values retain exact public ordering and report 16 bytes.

## 2. Durable reopen behavior

```bash
cargo test --test i128_key signed_i128_keys_round_trip_through_v2_reopen -- --exact --nocapture
```

Expected: all boundary entries are available through public reads after repeated reopen, in signed order.

## 3. Frozen historical compatibility

```bash
cargo test historical_i128 -- --nocapture
```

Expected: immutable legacy, V1, and earlier-V2 payloads normalize `0..=u64::MAX` exactly; put/remove and mixed-version ordering agree with the public state contract.

## 4. Offline migration

```bash
cargo test --test migration_cli i128_key::historical_i128_sources_migrate_to_current_v2_without_source_changes -- --exact --nocapture
```

Expected: migrated output uses current V2 map actions, reopens to identical logical state, and source bytes remain unchanged.

## 5. Full gates

```bash
cargo test --all-features -- --test-threads=1
cargo fmt --all -- --check
cargo clippy --offline --all-targets --all-features -- -D warnings
cargo test --doc --all-features
```

Expected: all commands exit successfully with no failures or new diagnostics.

## Validation evidence

**Pre-change checkpoint (2026-08-08)**:

- Existing model tests: 4 passed.
- Existing WAL replay tests: 3 passed.
- Existing migration CLI integration target: 13 passed.
- Frozen fixture binary identities are recorded in `tests/fixtures/i128_key/README.md` and independently reproduced before production edits.

**Observed RED evidence**:

- Signed minimum decoded as `0` instead of `i128::MIN` under the old public payload.
- Ordinary current puts emitted `[4, 4, 4, 4, 4]` instead of action `6`.
- Current remove emitted action `5` instead of `7`.
- Grouped current compute emitted action `4` instead of `6`.
- The frozen earlier-V2 fixture failed startup as `InvalidArtifact` after the public type correction and before the historical decoder.
- A truncated current-action record failed startup before V2 tail classification accepted actions `6`/`7`.
- Historical offline migration failed destination validation while snapshot output still used action `4`.

**Final GREEN evidence (2026-08-08)**:

- Focused `i128_key` integration target: 9 passed.
- Migration CLI integration target: 15 passed, including all historical formats and current segmented compaction.
- Full all-features suite: 261 unit tests passed with 9 documented release-only ignores; all integration targets passed.
- `cargo fmt --all -- --check`: passed.
- `cargo clippy --offline --all-targets --all-features -- -D warnings`: passed with zero diagnostics.
- `cargo test --doc --all-features`: passed.
