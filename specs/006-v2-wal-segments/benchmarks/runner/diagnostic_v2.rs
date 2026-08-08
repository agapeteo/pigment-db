use pigment_db_baseline as baseline;
use pigment_db_candidate as candidate;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

const AFFINITY: &str = "12-19";
const WORKERS: usize = 8;
const WARMUP_PAIRS: usize = 3;
const MEASURED_PAIRS: usize = 11;
const OPERATIONS_PER_WORKER: usize = 65_536;
const VALIDATION_OPERATIONS: usize = 256;

#[derive(Clone, Copy)]
enum Family {
    KeyValue,
    KeySet,
    KeyMap,
}

impl Family {
    const ALL: [Self; 3] = [Self::KeyValue, Self::KeySet, Self::KeyMap];

    fn name(self) -> &'static str {
        match self {
            Self::KeyValue => "key_value",
            Self::KeySet => "key_set",
            Self::KeyMap => "key_map",
        }
    }
}

struct SystemSnapshot {
    cpu_some_stall_us: u64,
    io_some_stall_us: u64,
    io_full_stall_us: u64,
    load_one: f64,
}

impl SystemSnapshot {
    fn capture() -> Self {
        let cpu = std::fs::read_to_string("/proc/pressure/cpu").expect("read /proc/pressure/cpu");
        let io = std::fs::read_to_string("/proc/pressure/io").expect("read /proc/pressure/io");
        let load = std::fs::read_to_string("/proc/loadavg").expect("read /proc/loadavg");
        Self {
            cpu_some_stall_us: parse_pressure_total(&cpu, "some")
                .expect("parse CPU some pressure total"),
            io_some_stall_us: parse_pressure_total(&io, "some")
                .expect("parse I/O some pressure total"),
            io_full_stall_us: parse_pressure_total(&io, "full")
                .expect("parse I/O full pressure total"),
            load_one: load
                .split_whitespace()
                .next()
                .expect("load average field")
                .parse()
                .expect("parse one-minute load average"),
        }
    }
}

struct Measurement {
    elapsed: Duration,
    cpu_some_stall_us: u64,
    io_some_stall_us: u64,
    io_full_stall_us: u64,
    load_one_before: f64,
    load_one_after: f64,
}

impl Measurement {
    fn capture(operation: impl FnOnce() -> Duration) -> Self {
        let before = SystemSnapshot::capture();
        let elapsed = operation();
        let after = SystemSnapshot::capture();
        Self {
            elapsed,
            cpu_some_stall_us: after
                .cpu_some_stall_us
                .saturating_sub(before.cpu_some_stall_us),
            io_some_stall_us: after
                .io_some_stall_us
                .saturating_sub(before.io_some_stall_us),
            io_full_stall_us: after
                .io_full_stall_us
                .saturating_sub(before.io_full_stall_us),
            load_one_before: before.load_one,
            load_one_after: after.load_one,
        }
    }
}

struct Row {
    pair: usize,
    position: usize,
    family: Family,
    implementation: &'static str,
    measurement: Measurement,
}

impl Row {
    fn operations(&self) -> usize {
        WORKERS * OPERATIONS_PER_WORKER
    }

    fn writes_per_second(&self) -> f64 {
        self.operations() as f64 / self.measurement.elapsed.as_secs_f64()
    }
}

fn parse_pressure_total(contents: &str, class: &str) -> Option<u64> {
    contents
        .lines()
        .find(|line| line.split_whitespace().next() == Some(class))?
        .split_whitespace()
        .find_map(|field| field.strip_prefix("total="))?
        .parse()
        .ok()
}

fn fixed_key(worker: usize, operation: usize) -> Vec<u8> {
    let mut key = Vec::with_capacity(16);
    key.extend_from_slice(&(worker as u64).to_le_bytes());
    key.extend_from_slice(&(operation as u64).to_le_bytes());
    key
}

fn run_workers<F>(operations_per_worker: usize, operation: F) -> Duration
where
    F: Fn(usize, usize) + Send + Sync + 'static,
{
    let operation = Arc::new(operation);
    let start_barrier = Arc::new(Barrier::new(WORKERS + 1));
    let handles = (0..WORKERS)
        .map(|worker| {
            let operation = Arc::clone(&operation);
            let start_barrier = Arc::clone(&start_barrier);
            std::thread::spawn(move || {
                start_barrier.wait();
                for index in 0..operations_per_worker {
                    operation(worker, index);
                }
            })
        })
        .collect::<Vec<_>>();
    let started = Instant::now();
    start_barrier.wait();
    for handle in handles {
        handle.join().expect("diagnostic worker panicked");
    }
    started.elapsed()
}

macro_rules! measure_variant {
    ($db:ident, $family:expr, $operations:expr, $root:expr, $label:expr) => {{
        let directory = tempfile::Builder::new()
            .prefix($label)
            .tempdir_in($root)
            .expect("create diagnostic sample directory");
        match $family {
            Family::KeyValue => {
                let store = Arc::new(
                    $db::key_value_store::DurableKeyValueStore::try_init_new(directory.path())
                        .expect("initialize key/value diagnostic store")
                        .into_store(),
                );
                Measurement::capture(move || {
                    run_workers($operations, move |worker, operation| {
                        store.put(fixed_key(worker, operation), vec![0x51; 32]);
                    })
                })
            }
            Family::KeySet => {
                let store = Arc::new(
                    $db::key_set_store::DurableKeySetStore::try_init_new(directory.path())
                        .expect("initialize key/set diagnostic store")
                        .into_store(),
                );
                Measurement::capture(move || {
                    run_workers($operations, move |worker, operation| {
                        store.append(fixed_key(worker, operation), vec![0x52; 32]);
                    })
                })
            }
            Family::KeyMap => {
                let store = Arc::new(
                    $db::key_map_store::DurableKeyMapStore::try_init_new(directory.path())
                        .expect("initialize key/map diagnostic store")
                        .into_store(),
                );
                Measurement::capture(move || {
                    run_workers($operations, move |worker, operation| {
                        store.put(
                            fixed_key(worker, operation),
                            $db::model::SearchKey::from(operation),
                            vec![0x53; 32],
                        );
                    })
                })
            }
        }
    }};
}

fn measure_baseline(family: Family, operations: usize, root: &Path, label: &str) -> Measurement {
    measure_variant!(baseline, family, operations, root, label)
}

fn measure_candidate(family: Family, operations: usize, root: &Path, label: &str) -> Measurement {
    measure_variant!(candidate, family, operations, root, label)
}

fn inherited_affinity() -> String {
    std::fs::read_to_string("/proc/self/status")
        .expect("read process status")
        .lines()
        .find_map(|line| line.strip_prefix("Cpus_allowed_list:\t"))
        .expect("process status contains Cpus_allowed_list")
        .trim()
        .to_owned()
}

fn validate_linked(root: &Path) {
    for family in Family::ALL {
        let baseline = measure_baseline(family, VALIDATION_OPERATIONS, root, "validate-baseline");
        let candidate =
            measure_candidate(family, VALIDATION_OPERATIONS, root, "validate-candidate");
        assert!(!baseline.elapsed.is_zero());
        assert!(!candidate.elapsed.is_zero());
    }
}

fn run_capture(root: &Path) -> Vec<Row> {
    let mut rows = Vec::with_capacity(3 * MEASURED_PAIRS * 2);
    for family in Family::ALL {
        println!("warming {}/8", family.name());
        for pair in 0..WARMUP_PAIRS {
            if pair % 2 == 0 {
                let _ = measure_baseline(family, OPERATIONS_PER_WORKER, root, "warm-baseline");
                let _ = measure_candidate(family, OPERATIONS_PER_WORKER, root, "warm-candidate");
            } else {
                let _ = measure_candidate(family, OPERATIONS_PER_WORKER, root, "warm-candidate");
                let _ = measure_baseline(family, OPERATIONS_PER_WORKER, root, "warm-baseline");
            }
        }
        println!("capturing {}/8", family.name());
        for pair in 0..MEASURED_PAIRS {
            let implementations = if pair % 2 == 0 {
                ["baseline", "candidate"]
            } else {
                ["candidate", "baseline"]
            };
            for (position, implementation) in implementations.into_iter().enumerate() {
                let measurement = match implementation {
                    "baseline" => {
                        measure_baseline(family, OPERATIONS_PER_WORKER, root, "sample-baseline")
                    }
                    "candidate" => {
                        measure_candidate(family, OPERATIONS_PER_WORKER, root, "sample-candidate")
                    }
                    _ => unreachable!(),
                };
                rows.push(Row {
                    pair,
                    position,
                    family,
                    implementation,
                    measurement,
                });
            }
        }
    }
    rows
}

fn write_capture(path: &Path, capture_id: &str, rows: &[Row]) {
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap_or_else(|error| panic!("create write-once output {}: {error}", path.display()));
    writeln!(
        output,
        "capture_id,baseline_commit,candidate_tree,affinity,pair,position,family,workers,implementation,operations,elapsed_ns,writes_per_second,cpu_some_stall_us,io_some_stall_us,io_full_stall_us,load_one_before,load_one_after"
    )
    .unwrap();
    let candidate_tree = std::env::var("PIGMENT_DB_V2_CANDIDATE_TREE")
        .expect("PIGMENT_DB_V2_CANDIDATE_TREE is required");
    for row in rows {
        writeln!(
            output,
            "{capture_id},f5bf40e9861b544f867d5aa940fa52eb940b5e54,{candidate_tree},{AFFINITY},{},{},{},{WORKERS},{},{},{},{:.6},{},{},{},{:.2},{:.2}",
            row.pair,
            row.position,
            row.family.name(),
            row.implementation,
            row.operations(),
            row.measurement.elapsed.as_nanos(),
            row.writes_per_second(),
            row.measurement.cpu_some_stall_us,
            row.measurement.io_some_stall_us,
            row.measurement.io_full_stall_us,
            row.measurement.load_one_before,
            row.measurement.load_one_after,
        )
        .unwrap();
    }
    output.flush().unwrap();
}

fn main() {
    assert_eq!(inherited_affinity(), AFFINITY, "unexpected CPU affinity");
    let root = PathBuf::from(
        std::env::var_os("PIGMENT_DB_V2_BENCH_ROOT").expect("PIGMENT_DB_V2_BENCH_ROOT is required"),
    )
    .canonicalize()
    .expect("canonicalize diagnostic root");
    validate_linked(&root);
    if std::env::var_os("PIGMENT_DB_V2_VALIDATE_ONLY").is_some() {
        println!("linked diagnostic validation and PSI capture passed");
        return;
    }
    let capture_id =
        std::env::var("PIGMENT_DB_V2_CAPTURE_ID").expect("PIGMENT_DB_V2_CAPTURE_ID is required");
    let output = PathBuf::from(
        std::env::var_os("PIGMENT_DB_V2_OUTPUT").expect("PIGMENT_DB_V2_OUTPUT is required"),
    );
    assert!(!output.exists(), "diagnostic output already exists");
    let rows = run_capture(&root);
    write_capture(&output, &capture_id, &rows);
}

#[cfg(test)]
mod tests {
    use super::parse_pressure_total;

    #[test]
    fn pressure_parser_returns_the_named_total() {
        let contents = concat!(
            "some avg10=1.00 avg60=2.00 avg300=3.00 total=1234\n",
            "full avg10=4.00 avg60=5.00 avg300=6.00 total=99\n",
        );

        assert_eq!(parse_pressure_total(contents, "some"), Some(1_234));
        assert_eq!(parse_pressure_total(contents, "full"), Some(99));
    }
}
