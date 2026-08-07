//! Paired steady-state and startup performance gates.

use pigment_db::key_value_store::DurableKeyValueStore;
use std::fmt::Write as _;
use std::hint::black_box;
use std::time::Instant;

#[path = "../mutation_ordering/performance.rs"]
mod issue3;

const STARTUP_OPERATIONS: usize = 1_000_000;
const STARTUP_SAMPLES: usize = 11;
const STARTUP_OUTPUT_ENV: &str = "PIGMENT_DB_STARTUP_OUTPUT";
const STARTUP_DIAGNOSTIC_OPERATIONS_ENV: &str = "PIGMENT_DB_STARTUP_DIAGNOSTIC_OPERATIONS";
const STARTUP_DIAGNOSTIC_SAMPLES_ENV: &str = "PIGMENT_DB_STARTUP_DIAGNOSTIC_SAMPLES";

fn diagnostic_override(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .ok()
                .filter(|value| *value > 0)
                .unwrap_or_else(|| panic!("{name} must be a positive integer"))
        })
        .unwrap_or(default)
}

fn startup_key(index: usize) -> Vec<u8> {
    let mut key = b"startup-key-".to_vec();
    key.extend_from_slice(&index.to_le_bytes());
    key
}

fn build_complete_startup_history(directory: &std::path::Path, operations: usize) {
    let store = DurableKeyValueStore::try_init_new(directory)
        .expect("initialize startup benchmark store")
        .into_store();
    for index in 0..operations {
        store.put(startup_key(index), index.to_le_bytes().to_vec());
    }
    black_box(store);
}

fn measure_complete_startup(
    directory: &std::path::Path,
    operations: usize,
    sample_count: usize,
) -> Vec<u128> {
    let mut samples = Vec::with_capacity(sample_count);
    for sample in 0..sample_count {
        let started = Instant::now();
        let store = DurableKeyValueStore::try_init_new(directory)
            .expect("reopen complete startup benchmark store")
            .into_store();
        let elapsed = started.elapsed().as_nanos();
        assert_eq!(store.size(), operations);
        println!("STARTUP,complete,{sample},{operations},{elapsed}");
        samples.push(elapsed);
        black_box(store);
    }
    samples
}

fn write_startup_csv(
    path: &std::path::Path,
    operations: usize,
    samples: &[u128],
) -> std::io::Result<()> {
    let mut csv = String::from("mode,sample,operations,elapsed_ns\n");
    for (sample, elapsed) in samples.iter().enumerate() {
        writeln!(csv, "complete,{sample},{operations},{elapsed}")
            .expect("write startup benchmark row");
    }
    std::fs::write(path, csv)
}

fn write_paired_startup_csv(
    path: &std::path::Path,
    operations: usize,
    complete: &[u128],
    torn: &[u128],
) -> std::io::Result<()> {
    let mut csv = String::from("mode,sample,operations,elapsed_ns\n");
    for (sample, elapsed) in complete.iter().enumerate() {
        writeln!(csv, "complete,{sample},{operations},{elapsed}")
            .expect("write complete startup row");
    }
    for (sample, elapsed) in torn.iter().enumerate() {
        writeln!(csv, "torn,{sample},{operations},{elapsed}").expect("write torn startup row");
    }
    std::fs::write(path, csv)
}

fn median(samples: &[u128]) -> u128 {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    sorted[sorted.len() / 2]
}

#[test]
#[ignore = "release-only immutable complete-history startup baseline"]
fn complete_startup_million_operations_baseline() {
    let operations = diagnostic_override(STARTUP_DIAGNOSTIC_OPERATIONS_ENV, STARTUP_OPERATIONS);
    let sample_count = diagnostic_override(STARTUP_DIAGNOSTIC_SAMPLES_ENV, STARTUP_SAMPLES);
    let directory = tempfile::tempdir().expect("create startup benchmark directory");
    build_complete_startup_history(directory.path(), operations);
    let samples = measure_complete_startup(directory.path(), operations, sample_count);
    if let Some(path) = std::env::var_os(STARTUP_OUTPUT_ENV) {
        write_startup_csv(std::path::Path::new(&path), operations, &samples)
            .expect("write startup benchmark CSV");
    }
    black_box(samples);
}

#[test]
#[ignore = "release-only paired complete-versus-torn startup gate"]
fn complete_versus_torn_startup_million_operations() {
    let operations = diagnostic_override(STARTUP_DIAGNOSTIC_OPERATIONS_ENV, STARTUP_OPERATIONS);
    let sample_count = diagnostic_override(STARTUP_DIAGNOSTIC_SAMPLES_ENV, STARTUP_SAMPLES);
    let complete_directory = tempfile::tempdir().expect("create complete startup directory");
    let torn_directory = tempfile::tempdir().expect("create torn startup directory");
    build_complete_startup_history(complete_directory.path(), operations);
    let complete_path = complete_directory.path().join("kv.wal.dat");
    let complete_bytes = std::fs::read(&complete_path).expect("read complete startup history");
    let torn_path = torn_directory.path().join("kv.wal.dat");
    std::fs::write(&torn_path, &complete_bytes).expect("seed torn startup history");
    let torn_store = DurableKeyValueStore::try_init_new(torn_directory.path())
        .expect("open torn startup seed")
        .into_store();
    torn_store.put(b"terminally-torn".to_vec(), b"discard".to_vec());
    drop(torn_store);
    let mut torn_bytes = std::fs::read(&torn_path).expect("read appended startup history");
    torn_bytes.pop();

    let mut complete_samples = Vec::with_capacity(sample_count);
    let mut torn_samples = Vec::with_capacity(sample_count);
    for sample in 0..sample_count {
        let complete_started = Instant::now();
        let complete = DurableKeyValueStore::try_init_new(complete_directory.path())
            .expect("reopen complete startup history")
            .into_store();
        let complete_elapsed = complete_started.elapsed().as_nanos();
        assert_eq!(complete.size(), operations);
        drop(complete);

        std::fs::write(&torn_path, &torn_bytes).expect("restore paired torn startup history");
        let torn_started = Instant::now();
        let torn = DurableKeyValueStore::try_init_new(torn_directory.path())
            .expect("recover paired torn startup history");
        let torn_elapsed = torn_started.elapsed().as_nanos();
        assert_eq!(torn.status(), pigment_db::RecoveryStatus::Recovered);
        assert_eq!(torn.store().size(), operations);
        drop(torn);

        println!("STARTUP,complete,{sample},{operations},{complete_elapsed}");
        println!("STARTUP,torn,{sample},{operations},{torn_elapsed}");
        complete_samples.push(complete_elapsed);
        torn_samples.push(torn_elapsed);
    }

    if let Some(path) = std::env::var_os(STARTUP_OUTPUT_ENV) {
        write_paired_startup_csv(
            std::path::Path::new(&path),
            operations,
            &complete_samples,
            &torn_samples,
        )
        .expect("write paired startup CSV");
    }
    let complete_median = median(&complete_samples);
    let torn_median = median(&torn_samples);
    assert!(
        torn_median.saturating_mul(100) <= complete_median.saturating_mul(125),
        "torn startup median {torn_median}ns exceeds 125% of complete median {complete_median}ns"
    );
}

#[test]
#[ignore = "release-only focused steady-state threshold gate"]
fn key_map_vector_eight_worker_write_threshold() {
    issue3::assert_key_map_vector_eight_worker_write_threshold();
}
