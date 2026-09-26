//! specs/012 FR-3: once a set has released its spare capacity, alternating one append and one
//! removal does not resize its table on every call.
//!
//! `capacity()` cannot show a resize: it is `len + growth_left`, which moves as removals leave
//! tombstones and appends reuse them. So this binary counts the allocations a set's table makes.
//! A `HashSet<Vec<u8>>` table is 16-byte aligned, while member bytes and the in-memory WAL are
//! byte-aligned. Only allocations made on the test's own thread while it is counting are seen.

use pigment_db::key_set_store::DurableKeySetStore;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct TableAllocations;

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static COUNT: Cell<usize> = const { Cell::new(0) };
}

fn note(layout: Layout) {
    if layout.align() == 16 && COUNTING.with(Cell::get) {
        COUNT.with(|count| count.set(count.get() + 1));
    }
}

unsafe impl GlobalAlloc for TableAllocations {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note(layout);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note(layout);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: TableAllocations = TableAllocations;

fn member(index: usize) -> Vec<u8> {
    format!("member-{index:06}").into_bytes()
}

/// Table allocations made by `work` on this thread.
fn table_allocations(work: impl FnOnce()) -> usize {
    COUNT.with(|count| count.set(0));
    COUNTING.with(|counting| counting.set(true));
    work();
    COUNTING.with(|counting| counting.set(false));
    COUNT.with(Cell::get)
}

/// 112 is a length at which a table sized exactly for its members is full: 128 buckets hold 112.
/// A rule that shrank to fit would grow on the next append and shrink on the next removal.
#[test]
fn alternating_appends_and_removals_do_not_resize_every_time() {
    const KEEP: usize = 112;
    let store = DurableKeySetStore::new_vec_based();
    for index in 0..10_000 {
        store.append(b"k".to_vec(), member(index));
    }
    for index in KEEP..10_000 {
        store.remove_from_set(b"k".to_vec(), member(index));
    }
    let extra = member(20_000);
    let allocations = table_allocations(|| {
        for _ in 0..200 {
            store.append(b"k".to_vec(), extra.clone());
            store.remove_from_set(b"k".to_vec(), extra.clone());
        }
    });
    assert!(
        allocations <= 2,
        "400 alternating calls made {allocations} table allocations"
    );
}

/// The counter itself: a set that must grow is seen growing.
#[test]
fn growing_a_set_is_counted() {
    let store = DurableKeySetStore::new_vec_based();
    let allocations = table_allocations(|| {
        for index in 0..1_000 {
            store.append(b"k".to_vec(), member(index));
        }
    });
    assert!(
        allocations >= 8,
        "1,000 appends made only {allocations} table allocations"
    );
}
