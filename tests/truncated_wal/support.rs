//! Shared public-API helpers for truncated-WAL integration tests.

use std::path::Path;
use std::sync::{Mutex, Once};

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

pub(crate) fn read_bytes(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

pub(crate) fn start_log_capture() {
    INSTALL_LOGGER.call_once(|| {
        log::set_logger(&LOGGER).expect("install truncated-WAL test logger");
        log::set_max_level(log::LevelFilter::Trace);
    });
    LOGS.lock().unwrap().clear();
}

pub(crate) fn captured_logs() -> Vec<String> {
    LOGS.lock().unwrap().clone()
}

pub(crate) fn assert_v1_timestamp_contract(bytes: &[u8], expected_granularity: u64) {
    assert_eq!(&bytes[..8], b"PIGWAL\r\n");
    assert_eq!(
        u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
        expected_granularity
    );
    let mut previous = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
    let mut cursor = 40_usize;
    let mut open_group: Option<(u32, u32, u64)> = None;
    while cursor < bytes.len() {
        let frame = &bytes[cursor..];
        let payload_len = u32::from_le_bytes(frame[6..10].try_into().unwrap()) as usize;
        let frame_len = 46 + payload_len;
        let physical_start = u32::from_le_bytes(frame[14..18].try_into().unwrap());
        let mutation_start = u32::from_le_bytes(frame[18..22].try_into().unwrap());
        let index = u32::from_le_bytes(frame[22..26].try_into().unwrap());
        let count = u32::from_le_bytes(frame[26..30].try_into().unwrap());
        let timestamp = u64::from_le_bytes(frame[30..38].try_into().unwrap());
        assert_eq!(physical_start as usize, cursor);
        assert!(timestamp >= previous);
        if index == 0 {
            assert_eq!(mutation_start, physical_start);
            open_group = Some((mutation_start, count, timestamp));
        }
        assert_eq!(open_group, Some((mutation_start, count, timestamp)));
        if index + 1 == count {
            previous = timestamp;
            open_group = None;
        }
        cursor += frame_len;
    }
    assert_eq!(cursor, bytes.len());
    assert!(open_group.is_none());
}
