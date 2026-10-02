//! specs/014 FR-1: a reading allocates nothing beyond its result: nothing for figures, and only
//! its detail for a failed-closed answer.
//!
//! A global allocator counts the allocations each thread makes, so this is its own test binary:
//! the allocator applies to every test linked into it. Each reading is counted on the calling
//! thread alone, so the test harness's own threads cannot disturb the count.

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::model::SearchKey;
use pigment_db::{CompactionError, DurableStoreOptions, FamilyStorageStats, WalSegmentSize};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

/// The system allocator, counting each allocation on the thread that makes it.
struct CountingAllocator;

// SAFETY: every call is forwarded unchanged to the system allocator. The count is a const-
// initialised thread-local with no destructor, so updating it allocates nothing, and `try_with`
// skips the count while the thread's locals are being torn down.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
        // SAFETY: the caller's contract for `alloc` is passed on unchanged.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
        // SAFETY: the caller's contract for `alloc_zeroed` is passed on unchanged.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
        // SAFETY: the caller's contract for `realloc` is passed on unchanged.
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract for `dealloc` is passed on unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// The allocations this thread makes while `read` runs, and its result.
fn counted(
    read: impl FnOnce() -> Result<FamilyStorageStats, CompactionError>,
) -> (usize, Result<FamilyStorageStats, CompactionError>) {
    let before = ALLOCATIONS.with(Cell::get);
    let result = read();
    let after = ALLOCATIONS.with(Cell::get);
    (after - before, result)
}

fn rotating() -> DurableStoreOptions {
    DurableStoreOptions::default().with_wal_segment_size(WalSegmentSize::try_from(256_u64).unwrap())
}

fn key(index: usize) -> Vec<u8> {
    format!("key-{index:07}").into_bytes()
}

fn value(index: usize) -> Vec<u8> {
    format!("{index:064}").into_bytes()
}

/// Asserts that a reading of a store that has rotated allocates nothing. The first reading is
/// not counted, so nothing the process sets up once (a thread's first lock, say) is charged to the
/// reading.
fn assert_a_reading_allocates_nothing(
    read: impl Fn() -> Result<FamilyStorageStats, CompactionError>,
) {
    let first = read().unwrap();
    assert!(first.sealed_segment_count() >= 3, "{first:?}");
    let (allocations, reading) = counted(&read);
    assert_eq!(
        reading.unwrap(),
        first,
        "the reading changed with no write between"
    );
    assert_eq!(allocations, 0, "a reading allocated");
}

#[test]
fn a_key_value_reading_allocates_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableKeyValueStore::try_init_new_with_options(directory.path(), rotating())
        .unwrap()
        .into_store();
    for index in 0..12 {
        store.put(key(index), value(index));
    }
    assert_a_reading_allocates_nothing(|| store.tracked_storage_stats());
}

#[test]
fn a_key_set_reading_allocates_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableKeySetStore::try_init_new_with_options(directory.path(), rotating())
        .unwrap()
        .into_store();
    for index in 0..12 {
        store.append(key(index), value(index));
    }
    assert_a_reading_allocates_nothing(|| store.tracked_storage_stats());
}

#[test]
fn a_key_map_reading_allocates_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableKeyMapStore::try_init_new_with_options(directory.path(), rotating())
        .unwrap()
        .into_store();
    for index in 0..12 {
        store.put(key(index), SearchKey::from(index), value(index));
    }
    assert_a_reading_allocates_nothing(|| store.tracked_storage_stats());
}

/// Fails a store's WAL closed through its public API (FR-3's unconfirmed rollback): under Physical
/// durability a rotation synchronises the store directory, which needs read permission there, so
/// a directory left with write and search permission only lets the rotation's moves through and
/// then refuses that synchronisation, after which the WAL is failed closed. The write that
/// rotates must fail, and the permissions are restored before this returns. Unix only.
#[cfg(unix)]
fn fail_closed_by_a_rotation(directory: &std::path::Path, write: impl FnOnce() -> bool) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o300))
        .expect("take the store directory's read permission");
    let staged = std::fs::read_dir(directory).is_err();
    let rotated = staged && write();
    std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o755))
        .expect("restore the store directory's permissions");
    assert!(
        staged,
        "permissions are not enforced for this user, so this scenario cannot be staged; run the \
         suite as an unprivileged user"
    );
    assert!(!rotated, "the write that rotates did not fail");
}

/// FR-1 and FR-3: a failed-closed reading allocates its error's detail and nothing else. Its
/// result is `CompactionError::FailedClosed`, whose detail is one string.
#[cfg(unix)]
fn assert_a_failed_closed_reading_allocates_only_its_detail(
    read: impl Fn() -> Result<FamilyStorageStats, CompactionError>,
) {
    assert!(
        matches!(read(), Err(CompactionError::FailedClosed { .. })),
        "the WAL is not failed closed: {:?}",
        read()
    );
    let (allocations, reading) = counted(&read);
    let Err(CompactionError::FailedClosed { detail }) = reading else {
        panic!("the WAL is no longer failed closed: {reading:?}");
    };
    assert!(!detail.is_empty(), "a failed-closed reading gave no detail");
    assert_eq!(
        allocations, 1,
        "a failed-closed reading allocated more than its detail"
    );
}

#[cfg(unix)]
fn physical_rotating() -> DurableStoreOptions {
    rotating().with_durability_policy(pigment_db::DurabilityPolicy::Physical)
}

#[cfg(unix)]
#[test]
fn a_failed_closed_key_value_reading_allocates_only_its_detail() {
    let directory = tempfile::tempdir().unwrap();
    let store =
        DurableKeyValueStore::try_init_new_with_options(directory.path(), physical_rotating())
            .unwrap()
            .into_store();
    store.put(key(0), value(0));
    fail_closed_by_a_rotation(directory.path(), || store.try_put(key(1), value(1)).is_ok());
    assert_a_failed_closed_reading_allocates_only_its_detail(|| store.tracked_storage_stats());
}

#[cfg(unix)]
#[test]
fn a_failed_closed_key_set_reading_allocates_only_its_detail() {
    let directory = tempfile::tempdir().unwrap();
    let store =
        DurableKeySetStore::try_init_new_with_options(directory.path(), physical_rotating())
            .unwrap()
            .into_store();
    store.append(key(0), value(0));
    fail_closed_by_a_rotation(directory.path(), || {
        store.try_append(key(1), value(1)).is_ok()
    });
    assert_a_failed_closed_reading_allocates_only_its_detail(|| store.tracked_storage_stats());
}

#[cfg(unix)]
#[test]
fn a_failed_closed_key_map_reading_allocates_only_its_detail() {
    let directory = tempfile::tempdir().unwrap();
    let store =
        DurableKeyMapStore::try_init_new_with_options(directory.path(), physical_rotating())
            .unwrap()
            .into_store();
    store.put(key(0), SearchKey::from(0_usize), value(0));
    fail_closed_by_a_rotation(directory.path(), || {
        store
            .try_put(key(1), SearchKey::from(1_usize), value(1))
            .is_ok()
    });
    assert_a_failed_closed_reading_allocates_only_its_detail(|| store.tracked_storage_stats());
}

/// The count sees an allocation the calling thread makes, so a zero above is a measurement.
#[test]
fn the_count_sees_an_allocation_on_the_calling_thread() {
    let (allocations, _) = counted(|| {
        std::hint::black_box(vec![0_u8; 64]);
        Err(CompactionError::FailedClosed {
            detail: String::new(),
        })
    });
    assert!(allocations >= 1, "the counting allocator counted nothing");
}
