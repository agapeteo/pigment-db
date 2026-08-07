//! Complete paired acceptance matrix for protocol v5.

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ComparisonPolicy {
    Buffered,
    Physical,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Comparator {
    PreFeature,
    AppendPlusBarrier,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum WorkerSchedule {
    StartOnly,
    PerOperation,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[allow(clippy::enum_variant_names)]
pub enum StoreFamily {
    KeyValue,
    KeySet,
    KeyMap,
}

impl StoreFamily {
    pub const ALL: [Self; 3] = [Self::KeyValue, Self::KeySet, Self::KeyMap];
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum StorageMode {
    Vector,
    File,
}

impl StorageMode {
    pub const ALL: [Self; 2] = [Self::Vector, Self::File];
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Workload {
    Write,
    Remove,
    Callback,
}

impl Workload {
    pub const ALL: [Self; 3] = [Self::Write, Self::Remove, Self::Callback];
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ComparisonCell {
    pub policy: ComparisonPolicy,
    pub comparator: Comparator,
    pub schedule: WorkerSchedule,
    pub family: StoreFamily,
    pub storage: StorageMode,
    pub workload: Workload,
    pub workers: usize,
}

pub fn complete_comparison_matrix() -> Vec<ComparisonCell> {
    let mut matrix = Vec::with_capacity(54);
    for family in StoreFamily::ALL {
        for storage in StorageMode::ALL {
            for workload in Workload::ALL {
                for workers in [1, 8] {
                    matrix.push(ComparisonCell {
                        policy: ComparisonPolicy::Buffered,
                        comparator: Comparator::PreFeature,
                        schedule: WorkerSchedule::StartOnly,
                        family,
                        storage,
                        workload,
                        workers,
                    });
                }
            }
        }
    }
    for family in StoreFamily::ALL {
        for workload in Workload::ALL {
            for workers in [1, 8] {
                matrix.push(ComparisonCell {
                    policy: ComparisonPolicy::Physical,
                    comparator: Comparator::AppendPlusBarrier,
                    schedule: WorkerSchedule::PerOperation,
                    family,
                    storage: StorageMode::File,
                    workload,
                    workers,
                });
            }
        }
    }
    matrix
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn complete_matrix_pairs_every_required_cell_with_one_matching_comparator() {
        let matrix = complete_comparison_matrix();
        let unique: HashSet<_> = matrix.iter().copied().collect();

        assert_eq!(matrix.len(), 54);
        assert_eq!(unique.len(), 54);
        assert_eq!(
            matrix
                .iter()
                .filter(|cell| cell.policy == ComparisonPolicy::Buffered)
                .count(),
            36
        );
        assert_eq!(
            matrix
                .iter()
                .filter(|cell| cell.policy == ComparisonPolicy::Physical)
                .count(),
            18
        );
        assert!(matrix.iter().all(|cell| match cell.policy {
            ComparisonPolicy::Buffered => {
                cell.comparator == Comparator::PreFeature
                    && cell.schedule == WorkerSchedule::StartOnly
            }
            ComparisonPolicy::Physical => {
                cell.comparator == Comparator::AppendPlusBarrier
                    && cell.schedule == WorkerSchedule::PerOperation
                    && cell.storage == StorageMode::File
            }
        }));
    }
}
