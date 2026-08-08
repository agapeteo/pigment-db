use pigment_db_baseline as baseline;
use pigment_db_candidate as candidate;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

const AFFINITY: &str = "12-19";
const WARMUP_PAIRS: usize = 5;
const MEASURED_PAIRS: usize = 11;
const OPERATIONS_PER_WORKER: usize = 16_384;

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

struct Row {
    pair: usize,
    position: usize,
    family: Family,
    workers: usize,
    implementation: &'static str,
    elapsed: Duration,
}

impl Row {
    fn operations(&self) -> usize {
        self.workers * OPERATIONS_PER_WORKER
    }

    fn writes_per_second(&self) -> f64 {
        self.operations() as f64 / self.elapsed.as_secs_f64()
    }
}

fn fixed_key(worker: usize, operation: usize) -> Vec<u8> {
    let mut key = Vec::with_capacity(16);
    key.extend_from_slice(&(worker as u64).to_le_bytes());
    key.extend_from_slice(&(operation as u64).to_le_bytes());
    key
}

fn run_workers<F>(workers: usize, operation: F) -> Duration
where
    F: Fn(usize, usize) + Send + Sync + 'static,
{
    let operation = Arc::new(operation);
    let start_barrier = Arc::new(Barrier::new(workers + 1));
    let handles = (0..workers)
        .map(|worker| {
            let operation = Arc::clone(&operation);
            let start_barrier = Arc::clone(&start_barrier);
            std::thread::spawn(move || {
                start_barrier.wait();
                for index in 0..OPERATIONS_PER_WORKER {
                    operation(worker, index);
                }
            })
        })
        .collect::<Vec<_>>();
    let started = Instant::now();
    start_barrier.wait();
    for handle in handles {
        handle.join().expect("benchmark worker panicked");
    }
    started.elapsed()
}

macro_rules! measure_variant {
    ($db:ident, $family:expr, $workers:expr, $root:expr, $label:expr) => {{
        let directory = tempfile::Builder::new()
            .prefix($label)
            .tempdir_in($root)
            .expect("create benchmark sample directory");
        match $family {
            Family::KeyValue => {
                let store = Arc::new(
                    $db::key_value_store::DurableKeyValueStore::try_init_new(directory.path())
                        .expect("initialize key/value benchmark store")
                        .into_store(),
                );
                run_workers($workers, move |worker, operation| {
                    store.put(fixed_key(worker, operation), vec![0x51; 32]);
                })
            }
            Family::KeySet => {
                let store = Arc::new(
                    $db::key_set_store::DurableKeySetStore::try_init_new(directory.path())
                        .expect("initialize key/set benchmark store")
                        .into_store(),
                );
                run_workers($workers, move |worker, operation| {
                    store.append(fixed_key(worker, operation), vec![0x52; 32]);
                })
            }
            Family::KeyMap => {
                let store = Arc::new(
                    $db::key_map_store::DurableKeyMapStore::try_init_new(directory.path())
                        .expect("initialize key/map benchmark store")
                        .into_store(),
                );
                run_workers($workers, move |worker, operation| {
                    store.put(
                        fixed_key(worker, operation),
                        $db::model::SearchKey::from(operation),
                        vec![0x53; 32],
                    );
                })
            }
        }
    }};
}

fn measure_baseline(family: Family, workers: usize, root: &Path, label: &str) -> Duration {
    measure_variant!(baseline, family, workers, root, label)
}

fn measure_candidate(family: Family, workers: usize, root: &Path, label: &str) -> Duration {
    measure_variant!(candidate, family, workers, root, label)
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
        let baseline_elapsed = measure_baseline(family, 1, root, "validate-baseline");
        let candidate_elapsed = measure_candidate(family, 1, root, "validate-candidate");
        assert!(!baseline_elapsed.is_zero());
        assert!(!candidate_elapsed.is_zero());
    }
}

fn run_capture(root: &Path) -> Vec<Row> {
    let mut rows = Vec::with_capacity(3 * 2 * MEASURED_PAIRS * 2);
    for family in Family::ALL {
        for workers in [1, 8] {
            let label = format!("{}/{workers}", family.name());
            println!("warming {label}");
            for pair in 0..WARMUP_PAIRS {
                if pair % 2 == 0 {
                    let _ = measure_baseline(family, workers, root, "warm-baseline");
                    let _ = measure_candidate(family, workers, root, "warm-candidate");
                } else {
                    let _ = measure_candidate(family, workers, root, "warm-candidate");
                    let _ = measure_baseline(family, workers, root, "warm-baseline");
                }
            }
            println!("capturing {label}");
            for pair in 0..MEASURED_PAIRS {
                let baseline_first = pair % 2 == 0;
                let variants = if baseline_first {
                    ["baseline", "candidate"]
                } else {
                    ["candidate", "baseline"]
                };
                for (position, implementation) in variants.into_iter().enumerate() {
                    let elapsed = match implementation {
                        "baseline" => {
                            measure_baseline(family, workers, root, "sample-baseline")
                        }
                        "candidate" => {
                            measure_candidate(family, workers, root, "sample-candidate")
                        }
                        _ => unreachable!(),
                    };
                    rows.push(Row {
                        pair,
                        position,
                        family,
                        workers,
                        implementation,
                        elapsed,
                    });
                }
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
        "capture_id,baseline_commit,candidate_tree,affinity,pair,position,family,workers,implementation,operations,elapsed_ns,writes_per_second"
    )
    .unwrap();
    let candidate_tree = std::env::var("PIGMENT_DB_V2_CANDIDATE_TREE")
        .expect("PIGMENT_DB_V2_CANDIDATE_TREE is required");
    for row in rows {
        writeln!(
            output,
            "{capture_id},f5bf40e9861b544f867d5aa940fa52eb940b5e54,{candidate_tree},{AFFINITY},{},{},{},{},{},{},{},{:.6}",
            row.pair,
            row.position,
            row.family.name(),
            row.workers,
            row.implementation,
            row.operations(),
            row.elapsed.as_nanos(),
            row.writes_per_second(),
        )
        .unwrap();
    }
    output.flush().unwrap();
}

fn main() {
    assert_eq!(inherited_affinity(), AFFINITY, "unexpected CPU affinity");
    let root = PathBuf::from(
        std::env::var_os("PIGMENT_DB_V2_BENCH_ROOT")
            .expect("PIGMENT_DB_V2_BENCH_ROOT is required"),
    )
    .canonicalize()
    .expect("canonicalize benchmark root");
    validate_linked(&root);
    if std::env::var_os("PIGMENT_DB_V2_VALIDATE_ONLY").is_some() {
        println!("linked V1 baseline and V2 candidate validation passed");
        return;
    }
    let capture_id = std::env::var("PIGMENT_DB_V2_CAPTURE_ID")
        .expect("PIGMENT_DB_V2_CAPTURE_ID is required");
    let output = PathBuf::from(
        std::env::var_os("PIGMENT_DB_V2_OUTPUT").expect("PIGMENT_DB_V2_OUTPUT is required"),
    );
    assert!(!output.exists(), "capture output already exists");
    let rows = run_capture(&root);
    write_capture(&output, &capture_id, &rows);
}
