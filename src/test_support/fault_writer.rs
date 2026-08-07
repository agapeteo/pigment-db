//! Scriptable WAL writers used by rejection and rollback unit tests.

use std::io::{self, Write};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(clippy::enum_variant_names)]
pub(crate) enum WriterFault {
    WriteCall(usize),
    PartialWriteCall { call: usize, written: usize },
    FlushCall(usize),
    TruncateCall(usize),
    DataBarrierCall(usize),
    FullBarrierCall(usize),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BarrierKind {
    Data,
    Full,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WriterEvent {
    Write,
    Flush,
    Truncate,
    DataBarrier,
    FullBarrier,
}

#[derive(Default)]
struct WriterState {
    volatile_bytes: Vec<u8>,
    durable_bytes: Vec<u8>,
    write_calls: usize,
    flush_calls: usize,
    truncate_calls: usize,
    data_barrier_calls: usize,
    full_barrier_calls: usize,
    faults: Vec<WriterFault>,
    fail_next_write: bool,
    rollback_fails: bool,
    events: Vec<WriterEvent>,
}

#[derive(Default)]
struct BarrierState {
    blocked_kind: Option<BarrierKind>,
    reached: Option<BarrierKind>,
    released: bool,
}

#[derive(Clone, Default)]
pub(crate) struct ScriptedWriterHandle {
    state: Arc<Mutex<WriterState>>,
    barrier: Arc<(Mutex<BarrierState>, Condvar)>,
}

pub(crate) struct ScriptedWriter {
    state: Arc<Mutex<WriterState>>,
    barrier: Arc<(Mutex<BarrierState>, Condvar)>,
}

impl ScriptedWriter {
    pub(crate) fn new(fault: WriterFault, rollback_fails: bool) -> (Self, ScriptedWriterHandle) {
        Self::scripted(Some(fault), rollback_fails, None)
    }

    pub(crate) fn scripted(
        fault: Option<WriterFault>,
        rollback_fails: bool,
        blocked_kind: Option<BarrierKind>,
    ) -> (Self, ScriptedWriterHandle) {
        Self::scripted_many_with_bytes(
            fault.into_iter().collect(),
            rollback_fails,
            blocked_kind,
            Vec::new(),
        )
    }

    pub(crate) fn scripted_with_bytes(
        fault: Option<WriterFault>,
        rollback_fails: bool,
        blocked_kind: Option<BarrierKind>,
        initial_bytes: Vec<u8>,
    ) -> (Self, ScriptedWriterHandle) {
        Self::scripted_many_with_bytes(
            fault.into_iter().collect(),
            rollback_fails,
            blocked_kind,
            initial_bytes,
        )
    }

    pub(crate) fn scripted_many_with_bytes(
        faults: Vec<WriterFault>,
        rollback_fails: bool,
        blocked_kind: Option<BarrierKind>,
        initial_bytes: Vec<u8>,
    ) -> (Self, ScriptedWriterHandle) {
        let state = Arc::new(Mutex::new(WriterState {
            volatile_bytes: initial_bytes.clone(),
            durable_bytes: initial_bytes,
            faults,
            rollback_fails,
            ..WriterState::default()
        }));
        let barrier = Arc::new((
            Mutex::new(BarrierState {
                blocked_kind,
                ..BarrierState::default()
            }),
            Condvar::new(),
        ));
        (
            Self {
                state: Arc::clone(&state),
                barrier: Arc::clone(&barrier),
            },
            ScriptedWriterHandle { state, barrier },
        )
    }
}

impl ScriptedWriterHandle {
    pub(crate) fn bytes(&self) -> Vec<u8> {
        lock(&self.state).volatile_bytes.clone()
    }

    pub(crate) fn durable_bytes(&self) -> Vec<u8> {
        lock(&self.state).durable_bytes.clone()
    }

    pub(crate) fn write_calls(&self) -> usize {
        lock(&self.state).write_calls
    }

    pub(crate) fn flush_calls(&self) -> usize {
        lock(&self.state).flush_calls
    }

    pub(crate) fn truncate_calls(&self) -> usize {
        lock(&self.state).truncate_calls
    }

    pub(crate) fn data_barrier_calls(&self) -> usize {
        lock(&self.state).data_barrier_calls
    }

    pub(crate) fn full_barrier_calls(&self) -> usize {
        lock(&self.state).full_barrier_calls
    }

    pub(crate) fn events(&self) -> Vec<WriterEvent> {
        lock(&self.state).events.clone()
    }

    pub(crate) fn simulate_power_loss(&self) -> Vec<u8> {
        let mut state = lock(&self.state);
        state.volatile_bytes = state.durable_bytes.clone();
        state.volatile_bytes.clone()
    }

    pub(crate) fn wait_until_barrier_blocked(&self, expected: BarrierKind) {
        let (state, changed) = &*self.barrier;
        let state = lock(state);
        let (state, timeout) = changed
            .wait_timeout_while(
                state,
                crate::test_support::mutation_schedule::WATCHDOG,
                |state| state.reached != Some(expected),
            )
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(state.reached, Some(expected), "barrier did not block");
        assert!(!timeout.timed_out(), "barrier wait timed out");
    }

    pub(crate) fn release_barrier(&self) {
        let (state, changed) = &*self.barrier;
        let mut state = lock(state);
        state.released = true;
        changed.notify_all();
    }
}

impl Drop for ScriptedWriterHandle {
    fn drop(&mut self) {
        self.release_barrier();
    }
}

impl Write for ScriptedWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let mut state = lock(&self.state);
        state.write_calls += 1;
        state.events.push(WriterEvent::Write);
        if state.fail_next_write {
            state.fail_next_write = false;
            return Err(io::Error::other(
                "scripted write rejection after partial write",
            ));
        }
        if state
            .faults
            .contains(&WriterFault::WriteCall(state.write_calls))
        {
            return Err(io::Error::other("scripted write rejection"));
        }
        if let Some(written) = state.faults.iter().find_map(|fault| match *fault {
            WriterFault::PartialWriteCall { call, written } if call == state.write_calls => {
                Some(written)
            }
            _ => None,
        }) {
            let written = written.min(buffer.len());
            state.volatile_bytes.extend_from_slice(&buffer[..written]);
            state.fail_next_write = true;
            return Ok(written);
        }
        state.volatile_bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut state = lock(&self.state);
        state.flush_calls += 1;
        state.events.push(WriterEvent::Flush);
        if state
            .faults
            .contains(&WriterFault::FlushCall(state.flush_calls))
        {
            return Err(io::Error::other("scripted flush rejection"));
        }
        Ok(())
    }
}

pub(crate) fn rollback_scripted(writer: &mut ScriptedWriter, checkpoint: usize) -> io::Result<()> {
    let mut state = lock(&writer.state);
    state.truncate_calls += 1;
    state.events.push(WriterEvent::Truncate);
    if state.rollback_fails
        || state
            .faults
            .contains(&WriterFault::TruncateCall(state.truncate_calls))
    {
        return Err(io::Error::other("scripted rollback rejection"));
    }
    state.volatile_bytes.truncate(checkpoint);
    Ok(())
}

pub(crate) fn sync_data_scripted(writer: &mut ScriptedWriter) -> io::Result<()> {
    {
        let mut state = lock(&writer.state);
        state.data_barrier_calls += 1;
        state.events.push(WriterEvent::DataBarrier);
        if state
            .faults
            .contains(&WriterFault::DataBarrierCall(state.data_barrier_calls))
        {
            return Err(io::Error::other("scripted data barrier rejection"));
        }
    }
    block_barrier(&writer.barrier, BarrierKind::Data);
    let mut state = lock(&writer.state);
    state.durable_bytes = state.volatile_bytes.clone();
    Ok(())
}

pub(crate) fn sync_all_scripted(writer: &mut ScriptedWriter) -> io::Result<()> {
    {
        let mut state = lock(&writer.state);
        state.full_barrier_calls += 1;
        state.events.push(WriterEvent::FullBarrier);
        if state
            .faults
            .contains(&WriterFault::FullBarrierCall(state.full_barrier_calls))
        {
            return Err(io::Error::other("scripted full barrier rejection"));
        }
    }
    block_barrier(&writer.barrier, BarrierKind::Full);
    let mut state = lock(&writer.state);
    state.durable_bytes = state.volatile_bytes.clone();
    Ok(())
}

fn block_barrier(barrier: &Arc<(Mutex<BarrierState>, Condvar)>, kind: BarrierKind) {
    let (state, changed) = &**barrier;
    let mut state = lock(state);
    if state.blocked_kind == Some(kind) {
        state.reached = Some(kind);
        changed.notify_all();
        while !state.released {
            state = changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }
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

    #[test]
    fn scripted_barriers_track_order_failures_and_durable_bytes() {
        let (mut writer, handle) = ScriptedWriter::scripted(None, false, None);
        writer.write_all(b"accepted").unwrap();
        writer.flush().unwrap();
        assert!(handle.durable_bytes().is_empty());
        sync_data_scripted(&mut writer).unwrap();
        assert_eq!(handle.durable_bytes(), b"accepted");
        writer.write_all(b"volatile").unwrap();
        assert_eq!(handle.simulate_power_loss(), b"accepted");
        assert_eq!(handle.write_calls(), 2);
        assert_eq!(handle.flush_calls(), 1);
        assert_eq!(handle.data_barrier_calls(), 1);
        assert_eq!(handle.full_barrier_calls(), 0);
        assert_eq!(
            handle.events(),
            [
                WriterEvent::Write,
                WriterEvent::Flush,
                WriterEvent::DataBarrier,
                WriterEvent::Write,
            ]
        );

        let (mut writer, handle) =
            ScriptedWriter::scripted(Some(WriterFault::FullBarrierCall(1)), false, None);
        writer.write_all(b"uncertain").unwrap();
        assert!(sync_all_scripted(&mut writer).is_err());
        assert!(handle.durable_bytes().is_empty());
        assert_eq!(handle.full_barrier_calls(), 1);
    }

    #[test]
    fn partial_write_and_truncate_faults_are_independent() {
        let (mut writer, handle) = ScriptedWriter::scripted(
            Some(WriterFault::PartialWriteCall {
                call: 1,
                written: 3,
            }),
            false,
            None,
        );
        assert!(writer.write_all(b"partial").is_err());
        assert_eq!(handle.bytes(), b"par");
        rollback_scripted(&mut writer, 0).unwrap();
        assert_eq!(handle.truncate_calls(), 1);
        sync_all_scripted(&mut writer).unwrap();
        assert!(handle.durable_bytes().is_empty());

        let (mut writer, handle) =
            ScriptedWriter::scripted(Some(WriterFault::TruncateCall(1)), false, None);
        writer.write_all(b"uncertain").unwrap();
        assert!(rollback_scripted(&mut writer, 0).is_err());
        assert_eq!(handle.bytes(), b"uncertain");
        assert_eq!(handle.truncate_calls(), 1);
    }

    #[test]
    fn data_and_full_barriers_block_until_explicit_release() {
        for kind in [BarrierKind::Data, BarrierKind::Full] {
            let (mut writer, handle) = ScriptedWriter::scripted(None, false, Some(kind));
            writer.write_all(b"blocked").unwrap();
            std::thread::scope(|scope| {
                let operation = scope.spawn(|| match kind {
                    BarrierKind::Data => sync_data_scripted(&mut writer),
                    BarrierKind::Full => sync_all_scripted(&mut writer),
                });
                handle.wait_until_barrier_blocked(kind);
                assert!(handle.durable_bytes().is_empty());
                handle.release_barrier();
                operation.join().unwrap().unwrap();
            });
            assert_eq!(handle.durable_bytes(), b"blocked");
        }
    }
}
