//! specs/012 FR-3, measured on the tables themselves rather than through `capacity()`.
//!
//! `capacity()` is `len + growth_left`, and a removal that leaves a tombstone does not give its slot
//! back to `growth_left`, so the public read can understate a table and moves without a resize. This
//! binary's allocator therefore watches the allocations a `HashSet<Vec<u8>>` table makes. Their
//! alignment and sizes are calibrated at run time from sets of known bucket counts, because both
//! depend on the platform's hashbrown group width (16 bytes with SSE2, 8 on NEON and the generic
//! 64-bit implementation). Only allocations made on the test's own thread while it is watching are
//! seen.

use pigment_db::key_set_store::DurableKeySetStore;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::HashSet;

/// Bucket counts are powers of two; this many exponents covers every table the tests build.
const EXPONENTS: usize = 20;

struct Tables;

thread_local! {
    static WATCHING: Cell<bool> = const { Cell::new(false) };
    static CALIBRATING: Cell<bool> = const { Cell::new(false) };
    static LAST: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
    /// A set table's alignment, and its byte size for 2^exponent buckets. Zero until calibrated.
    static ALIGN: Cell<usize> = const { Cell::new(0) };
    static SIZES: Cell<[usize; EXPONENTS]> = const { Cell::new([0; EXPONENTS]) };
    /// Table allocations seen while watching, and live tables per bucket exponent.
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    static LIVE: Cell<[isize; EXPONENTS]> = const { Cell::new([0; EXPONENTS]) };
}

/// The bucket exponent of a set-table allocation of this layout, if it is one.
fn exponent_of(layout: Layout) -> Option<usize> {
    if layout.align() != ALIGN.with(Cell::get) {
        return None;
    }
    SIZES
        .with(Cell::get)
        .iter()
        .position(|size| *size == layout.size())
}

fn allocated(layout: Layout) {
    if CALIBRATING.with(Cell::get) {
        LAST.with(|last| last.set((layout.align(), layout.size())));
    }
    if WATCHING.with(Cell::get) {
        if let Some(exponent) = exponent_of(layout) {
            ALLOCATIONS.with(|count| count.set(count.get() + 1));
            LIVE.with(|live| {
                let mut tables = live.get();
                tables[exponent] += 1;
                live.set(tables);
            });
        }
    }
}

fn freed(layout: Layout) {
    if WATCHING.with(Cell::get) {
        if let Some(exponent) = exponent_of(layout) {
            LIVE.with(|live| {
                let mut tables = live.get();
                tables[exponent] -= 1;
                live.set(tables);
            });
        }
    }
}

unsafe impl GlobalAlloc for Tables {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        allocated(layout);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        freed(layout);
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        freed(layout);
        allocated(Layout::from_size_align(new_size, layout.align()).expect("a valid layout"));
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Tables = Tables;

/// The usable capacity of a table of `buckets` buckets, as hashbrown computes it.
fn usable(buckets: usize) -> usize {
    if buckets < 8 {
        buckets - 1
    } else {
        buckets / 8 * 7
    }
}

/// Learn a set table's alignment and its size for every bucket count, from sets of known capacity.
fn calibrate() {
    if ALIGN.with(Cell::get) != 0 {
        return;
    }
    let mut sizes = [0; EXPONENTS];
    let mut align = 0;
    for (exponent, size) in sizes.iter_mut().enumerate().skip(2) {
        CALIBRATING.with(|on| on.set(true));
        let set: HashSet<Vec<u8>> = HashSet::with_capacity(usable(1 << exponent));
        CALIBRATING.with(|on| on.set(false));
        let (seen_align, seen_size) = LAST.with(Cell::get);
        assert!(
            set.capacity() >= usable(1 << exponent),
            "calibration: the set did not allocate"
        );
        assert!(
            align == 0 || align == seen_align,
            "calibration: set tables differ in alignment"
        );
        align = seen_align;
        *size = seen_size;
        drop(set);
    }
    ALIGN.with(|cell| cell.set(align));
    SIZES.with(|cell| cell.set(sizes));
}

/// Run `work` while watching set tables: the tables allocated, and the bucket counts still live.
fn watch(work: impl FnOnce()) -> (usize, Vec<usize>) {
    calibrate();
    ALLOCATIONS.with(|count| count.set(0));
    LIVE.with(|live| live.set([0; EXPONENTS]));
    WATCHING.with(|on| on.set(true));
    work();
    WATCHING.with(|on| on.set(false));
    let live = LIVE.with(Cell::get);
    let buckets = (0..EXPONENTS)
        .flat_map(|exponent| std::iter::repeat_n(1 << exponent, live[exponent].max(0) as usize))
        .collect();
    (ALLOCATIONS.with(Cell::get), buckets)
}

fn member(index: usize) -> Vec<u8> {
    format!("member-{index:06}").into_bytes()
}

/// The watcher itself: a set that must grow is seen growing, and its one live table is seen.
#[test]
fn growing_a_set_is_counted() {
    let store = DurableKeySetStore::new_vec_based();
    let (allocations, live) = watch(|| {
        for index in 0..1_000 {
            store.append(b"k".to_vec(), member(index));
        }
    });
    assert!(
        allocations >= 8,
        "1,000 appends made only {allocations} table allocations"
    );
    assert_eq!(
        live,
        vec![2_048],
        "one live table of 2,048 buckets holds 1,000 members"
    );
}

/// FR-3 on the real table. A set grown to fill its table and then cut down by removals leaves most
/// removed slots as tombstones, so `capacity()` follows `len` down while the table stays large. A
/// trigger gated on `capacity()` would never fire here.
#[test]
fn a_full_table_cut_down_is_released_by_its_real_size() {
    for (grown, keep) in [(3_584, 600), (14_336, 2_000)] {
        let store = DurableKeySetStore::new_vec_based();
        let (_, live) = watch(|| {
            for index in 0..grown {
                store.append(b"k".to_vec(), member(index));
            }
            for index in keep..grown {
                store.remove_from_set(b"k".to_vec(), member(index));
            }
        });
        assert_eq!(
            live.len(),
            1,
            "exactly the one set's table is live: {live:?}"
        );
        assert!(
            usable(live[0]) <= 4 * keep,
            "{grown} members cut to {keep} left a table of {} buckets ({} usable), above 4 x len",
            live[0],
            usable(live[0])
        );
    }
}

/// The hysteresis, across the thresholds of the rule itself: alternating `step` appends and `step`
/// removals near any length does not resize the table on every round.
#[test]
fn alternating_appends_and_removals_do_not_resize_every_time() {
    let keeps = [
        13, 14, 15, 27, 28, 29, 55, 56, 57, 110, 111, 112, 113, 223, 224, 225, 447, 448, 449,
    ];
    for keep in keeps {
        for step in 1..=3 {
            let store = DurableKeySetStore::new_vec_based();
            for index in 0..8 * keep {
                store.append(b"k".to_vec(), member(index));
            }
            for index in keep..8 * keep {
                store.remove_from_set(b"k".to_vec(), member(index));
            }
            let (allocations, _) = watch(|| {
                for round in 0..100 {
                    for extra in 0..step {
                        store.append(b"k".to_vec(), member(100_000 + round * 10 + extra));
                    }
                    for extra in 0..step {
                        store.remove_from_set(b"k".to_vec(), member(100_000 + round * 10 + extra));
                    }
                }
            });
            assert!(
                allocations <= 2,
                "{keep} members, {step} appends then {step} removals per round: {allocations} table \
                 allocations in 100 rounds"
            );
        }
    }
}
