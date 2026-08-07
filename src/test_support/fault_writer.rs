//! Scriptable WAL writers used by rejection and rollback unit tests.

use std::io::{self, Write};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WriterFault {
    WriteCall(usize),
    FlushCall(usize),
}

#[derive(Default)]
struct WriterState {
    bytes: Vec<u8>,
    write_calls: usize,
    flush_calls: usize,
    fault: Option<WriterFault>,
    rollback_fails: bool,
}

#[derive(Clone, Default)]
pub(crate) struct ScriptedWriterHandle {
    state: Arc<Mutex<WriterState>>,
}

pub(crate) struct ScriptedWriter {
    state: Arc<Mutex<WriterState>>,
}

impl ScriptedWriter {
    pub(crate) fn new(fault: WriterFault, rollback_fails: bool) -> (Self, ScriptedWriterHandle) {
        let state = Arc::new(Mutex::new(WriterState {
            fault: Some(fault),
            rollback_fails,
            ..WriterState::default()
        }));
        (
            Self {
                state: Arc::clone(&state),
            },
            ScriptedWriterHandle { state },
        )
    }
}

impl ScriptedWriterHandle {
    pub(crate) fn bytes(&self) -> Vec<u8> {
        lock(&self.state).bytes.clone()
    }

    pub(crate) fn write_calls(&self) -> usize {
        lock(&self.state).write_calls
    }

    pub(crate) fn flush_calls(&self) -> usize {
        lock(&self.state).flush_calls
    }
}

impl Write for ScriptedWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let mut state = lock(&self.state);
        state.write_calls += 1;
        if state.fault == Some(WriterFault::WriteCall(state.write_calls)) {
            return Err(io::Error::other("scripted write rejection"));
        }
        state.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut state = lock(&self.state);
        state.flush_calls += 1;
        if state.fault == Some(WriterFault::FlushCall(state.flush_calls)) {
            return Err(io::Error::other("scripted flush rejection"));
        }
        Ok(())
    }
}

pub(crate) fn rollback_scripted(writer: &mut ScriptedWriter, checkpoint: usize) -> io::Result<()> {
    let mut state = lock(&writer.state);
    if state.rollback_fails {
        return Err(io::Error::other("scripted rollback rejection"));
    }
    state.bytes.truncate(checkpoint);
    Ok(())
}

#[derive(Default)]
struct BlockingState {
    bytes: Vec<u8>,
    write_calls: usize,
    block_call: usize,
    reached: bool,
    released: bool,
}

#[derive(Clone)]
pub(crate) struct BlockingWriterController {
    state: Arc<(Mutex<BlockingState>, Condvar)>,
}

pub(crate) struct BlockingWriter {
    state: Arc<(Mutex<BlockingState>, Condvar)>,
}

impl BlockingWriter {
    pub(crate) fn new(block_call: usize) -> (Self, BlockingWriterController) {
        let state = Arc::new((
            Mutex::new(BlockingState {
                block_call,
                ..BlockingState::default()
            }),
            Condvar::new(),
        ));
        (
            Self {
                state: Arc::clone(&state),
            },
            BlockingWriterController { state },
        )
    }
}

impl BlockingWriterController {
    pub(crate) fn wait_until_blocked(&self) {
        let (state, changed) = &*self.state;
        let state = lock(state);
        let (state, timeout) = changed
            .wait_timeout_while(
                state,
                crate::test_support::mutation_schedule::WATCHDOG,
                |state| !state.reached,
            )
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(
            state.reached && !timeout.timed_out(),
            "writer did not block"
        );
    }

    pub(crate) fn release(&self) {
        let (state, changed) = &*self.state;
        let mut state = lock(state);
        state.released = true;
        changed.notify_all();
    }
}

impl Drop for BlockingWriterController {
    fn drop(&mut self) {
        self.release();
    }
}

impl Write for BlockingWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let (state, changed) = &*self.state;
        let mut state = lock(state);
        state.write_calls += 1;
        if state.write_calls == state.block_call {
            state.reached = true;
            changed.notify_all();
            while !state.released {
                state = changed
                    .wait(state)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
        }
        state.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) fn rollback_blocking(writer: &mut BlockingWriter, checkpoint: usize) -> io::Result<()> {
    let (state, _) = &*writer.state;
    lock(state).bytes.truncate(checkpoint);
    Ok(())
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scripted_writer_tracks_progress_and_rollback() {
        let (mut writer, handle) = ScriptedWriter::new(WriterFault::WriteCall(2), false);
        writer.write_all(b"accepted").unwrap();
        assert!(writer.write_all(b"rejected").is_err());
        assert_eq!(handle.bytes(), b"accepted");
        assert_eq!(handle.write_calls(), 2);
        rollback_scripted(&mut writer, 0).unwrap();
        assert!(handle.bytes().is_empty());
    }
}
