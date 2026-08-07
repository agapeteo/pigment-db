use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, Once};

pub fn fixture_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/legacy")
        .join(name)
}

pub fn copy_fixture(name: &str, directory: &Path, target_name: &str) -> PathBuf {
    let target = directory.join(target_name);
    fs::copy(fixture_path(name), &target).expect("copy frozen WAL fixture");
    target
}

pub fn snapshot_files(directory: &Path, names: &[&str]) -> BTreeMap<String, Vec<u8>> {
    names
        .iter()
        .filter_map(|name| {
            let path = directory.join(name);
            path.exists().then(|| {
                (
                    (*name).to_owned(),
                    fs::read(path).expect("read WAL snapshot"),
                )
            })
        })
        .collect()
}

struct CapturingLogger;

static LOGGER: CapturingLogger = CapturingLogger;
static LOGS: Mutex<Vec<String>> = Mutex::new(Vec::new());
static INSTALL_LOGGER: Once = Once::new();

impl log::Log for CapturingLogger {
    fn enabled(&self, _metadata: &log::Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            LOGS.lock().unwrap().push(record.args().to_string());
        }
    }

    fn flush(&self) {}
}

pub fn start_log_capture() {
    INSTALL_LOGGER.call_once(|| {
        log::set_logger(&LOGGER).expect("install test logger");
        log::set_max_level(log::LevelFilter::Trace);
    });
    LOGS.lock().unwrap().clear();
}

pub fn captured_logs() -> Vec<String> {
    LOGS.lock().unwrap().clone()
}
