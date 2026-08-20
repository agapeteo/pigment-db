# Contract: Windows Physical Durability

## Dependency and unsafe boundary

Add only this target-specific dependency:

```toml
[target.'cfg(windows)'.dependencies]
windows-sys = { version = "0.61.2", features = ["Win32_Storage_FileSystem"] }
```

The crate denies unsafe code generally. `src/durability/windows.rs` is the single documented exception and contains every Windows FFI pointer conversion and call. No compaction, WAL, recovery, family, or test-support module contains unsafe code.

## Safe internal namespace API

The Windows module exposes a safe internal operation with two explicit modes:

- `NoReplace`: `MoveFileExW(source, destination, MOVEFILE_WRITE_THROUGH)`; destination must not exist.
- `ReplaceExisting`: `MoveFileExW(source, destination, MOVEFILE_WRITE_THROUGH | MOVEFILE_REPLACE_EXISTING)`; used only where the protocol explicitly permits replacement.

`MOVEFILE_COPY_ALLOWED` is never set, so a same-volume invariant cannot silently become copy-and-delete. There is no fallback to `std::fs::rename` after a physical-mode error.

## Path conversion and call safety

1. Validate/canonicalize an existing anchor parent and append exact source/destination leaf components.
2. Convert native Windows paths losslessly with `OsStrExt::encode_wide`.
3. Reject an interior NUL as invalid input.
4. Append exactly one UTF-16 NUL terminator.
5. Keep both vectors alive for the full FFI call.
6. Treat nonzero `BOOL` as success.
7. On zero, call `io::Error::last_os_error()` immediately before any other OS call.

Owned active WAL handles are closed or replaced before namespace operations. External handles opened without delete sharing may produce a sharing violation; this error is propagated according to publication authority and never triggers a weaker rename.

## Physical preflight

Before a physical store is exposed, preflight the actual target directory without touching authoritative WAL artifacts:

1. Generate unique disposable names and create the source with `create_new`.
2. Write and flush a sentinel, call file `sync_all`, close it, and reopen/verify it.
3. Perform same-directory no-replace write-through movement and verify content/path result.
4. Probe write-through replace-existing behavior if the actual publication protocol uses it.
5. Clean disposable artifacts only when their identity is proven.
6. Confirm cleanup; otherwise fail construction rather than exposing the store.

Content barrier failure maps to `DurabilitySupportError::RequiredBarrierUnavailable` for file content. Namespace movement/preflight failure maps to the directory-entry barrier. The requested policy is never changed to buffered.

## Publication coverage

All physical-mode namespace transitions use the platform durability abstraction:

- fresh-store publication;
- normal WAL rotation;
- recovery publication/rollback;
- manifest `.next` to main transitions;
- old to previous-generation publication;
- staging to canonical replacement for closed compaction;
- family staging cutover for online compaction;
- cleanup phase transitions where namespace durability is part of authority.

WAL contents, rollback data, staging segments, and manifests continue to use file synchronization before namespace publication. Live state is not published before its complete persistent representation is synchronized.

Linux/macOS physical behavior remains rename plus existing parent-directory synchronization. Buffered Windows uses established standard-library behavior and never calls the Win32 write-through helper.

## Failure contract

- File synchronization failure retains current rollback/failed-closed semantics.
- Write-through movement failure is returned with the exact maintenance/WAL operation and path.
- Preflight failure occurs before authoritative artifact mutation or store exposure.
- A failed namespace operation after publication starts is resolved by manifest evidence; the library never guesses or deletes ambiguous old/new artifacts.
- Unsupported filesystem primitives return structured refusal, never success or silent downgrade.

## Windows CI evidence

Replace the current physical-unsupported expectation with real Windows tests for all families:

- physical fresh construction and existing open;
- ordinary mutations, compute batches, rollback, rotation, and recovery;
- closed and online compaction with each manifest phase/fault;
- content-sync and write-through-move fault mapping;
- no-replace destination conflict, replace-existing behavior, and external sharing violation;
- Unicode names and supported long absolute paths;
- buffered compatibility and proof that buffered mode does not invoke write-through moves;
- static assertion that unsafe code exists only in `src/durability/windows.rs`.

Primary platform behavior is defined by [Microsoft `MoveFileExW`](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-movefileexw). File-content semantics are grounded in [Microsoft `FlushFileBuffers`](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers), and handle sharing behavior in [Microsoft `CreateFileW`](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilew).
