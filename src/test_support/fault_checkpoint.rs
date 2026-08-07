//! Deterministic fault checkpoint recording for unit-test-only pipelines.

#![allow(dead_code)]

use std::sync::Mutex;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FaultCheckpoint {
    FreshPublication,
    RepairPublication,
    Migration,
}

#[derive(Default)]
pub(crate) struct FaultCheckpointLog {
    reached: Mutex<Vec<FaultCheckpoint>>,
}

impl FaultCheckpointLog {
    pub(crate) fn record(&self, checkpoint: FaultCheckpoint) {
        self.reached.lock().unwrap().push(checkpoint);
    }

    pub(crate) fn reached(&self) -> Vec<FaultCheckpoint> {
        self.reached.lock().unwrap().clone()
    }
}
