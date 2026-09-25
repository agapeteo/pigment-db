//! The unit-test binary's one logger. `log` accepts a single logger per process, so every unit test
//! that asserts on a log line shares this one.

use std::sync::{Mutex, Once};

struct TestLogger;

static TEST_LOGGER: TestLogger = TestLogger;
static INSTALL: Once = Once::new();

/// Every message logged since the buffer was last cleared, from every thread.
pub(crate) static TEST_LOGS: Mutex<Vec<String>> = Mutex::new(Vec::new());

impl log::Log for TestLogger {
    fn enabled(&self, _metadata: &log::Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &log::Record<'_>) {
        TEST_LOGS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(record.args().to_string());
    }

    fn flush(&self) {}
}

/// Installs the logger, once per process.
pub(crate) fn install() {
    INSTALL.call_once(|| {
        log::set_logger(&TEST_LOGGER).unwrap();
        log::set_max_level(log::LevelFilter::Trace);
    });
}

/// Installs the logger and clears what it has captured. Tests running in parallel may clear it
/// too, so a test that must see its own line should retry the action that logs it.
pub(crate) fn capture_logs() {
    install();
    TEST_LOGS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
}

/// Whether any captured message satisfies `matches`.
pub(crate) fn logged(matches: impl Fn(&str) -> bool) -> bool {
    TEST_LOGS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .iter()
        .any(|message| matches(message))
}
