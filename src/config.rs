//! Validated timestamp configuration shared by all durable store families.

use std::error::Error;
use std::fmt;
use std::num::NonZeroU64;
use std::time::Duration;

const DEFAULT_GRANULARITY_NANOS: u64 = 60_000_000_000;
const DEFAULT_WAL_SEGMENT_BYTES: u64 = 1024 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
/// Selects when a successful mutation is acknowledged.
pub enum DurabilityPolicy {
    /// Preserve the historical write-plus-flush acknowledgement behavior.
    ///
    /// Buffered success does not promise survival of sudden power loss.
    #[default]
    Buffered,
    /// Acknowledge only after the complete logical WAL mutation reaches a direct
    /// file-data barrier.
    ///
    /// File-backed construction is supported on Linux and macOS only when both
    /// file-content and parent-directory preflights succeed. It never falls back
    /// to [`Self::Buffered`]. Vector-backed construction rejects this policy.
    Physical,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// A validated, nonzero timestamp-bucket width representable in nanoseconds.
///
/// Construct this value with [`TryFrom<std::time::Duration>`]. The default is
/// one minute.
pub struct TimestampGranularity(NonZeroU64);

impl TimestampGranularity {
    pub(crate) const fn nanos(self) -> u64 {
        self.0.get()
    }
}

impl Default for TimestampGranularity {
    fn default() -> Self {
        Self(NonZeroU64::new(DEFAULT_GRANULARITY_NANOS).unwrap())
    }
}

impl TryFrom<Duration> for TimestampGranularity {
    type Error = TimestampGranularityError;

    fn try_from(duration: Duration) -> Result<Self, Self::Error> {
        let nanos =
            u64::try_from(duration.as_nanos()).map_err(|_| TimestampGranularityError::TooLarge)?;
        NonZeroU64::new(nanos)
            .map(Self)
            .ok_or(TimestampGranularityError::Zero)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Describes why a duration cannot be used as timestamp granularity.
pub enum TimestampGranularityError {
    /// The duration contains zero nanoseconds.
    Zero,
    /// The duration contains more nanoseconds than fit in `u64`.
    TooLarge,
}

impl fmt::Display for TimestampGranularityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Zero => formatter.write_str("timestamp granularity must be nonzero"),
            Self::TooLarge => formatter.write_str("timestamp granularity nanoseconds exceed u64"),
        }
    }
}

impl Error for TimestampGranularityError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// A validated, nonzero target size for one active V2 WAL segment.
///
/// The limit is checked between complete logical mutations. A single mutation
/// larger than the target remains intact in one oversized segment.
pub struct WalSegmentSize(NonZeroU64);

impl WalSegmentSize {
    /// Returns the configured target size in bytes.
    pub const fn as_bytes(self) -> u64 {
        self.0.get()
    }
}

impl Default for WalSegmentSize {
    fn default() -> Self {
        Self(NonZeroU64::new(DEFAULT_WAL_SEGMENT_BYTES).unwrap())
    }
}

impl TryFrom<u64> for WalSegmentSize {
    type Error = WalSegmentSizeError;

    fn try_from(bytes: u64) -> Result<Self, Self::Error> {
        NonZeroU64::new(bytes)
            .map(Self)
            .ok_or(WalSegmentSizeError::Zero)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Describes why a byte count cannot be used as a WAL segment target.
pub enum WalSegmentSizeError {
    /// A WAL segment target must contain at least one byte.
    Zero,
}

impl fmt::Display for WalSegmentSizeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Zero => formatter.write_str("WAL segment size must be nonzero"),
        }
    }
}

impl Error for WalSegmentSizeError {}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
/// Additive options shared by all durable store families.
///
/// File-backed V2 stores apply an explicitly selected timestamp granularity by
/// rotating before the next accepted mutation. Options that set only durability
/// or segment size preserve the active segment's persisted granularity. The
/// durability and segment-size policies are runtime-only; a later no-options
/// reopen selects their defaults again. Options never authorize legacy or V1
/// migration.
pub struct DurableStoreOptions {
    timestamp_granularity: Option<TimestampGranularity>,
    durability_policy: DurabilityPolicy,
    wal_segment_size: WalSegmentSize,
}

impl DurableStoreOptions {
    /// Selects a validated timestamp granularity.
    pub fn with_timestamp_granularity(
        mut self,
        timestamp_granularity: TimestampGranularity,
    ) -> Self {
        self.timestamp_granularity = Some(timestamp_granularity);
        self
    }

    pub(crate) const fn granularity_nanos(self) -> u64 {
        match self.timestamp_granularity {
            Some(granularity) => granularity.nanos(),
            None => DEFAULT_GRANULARITY_NANOS,
        }
    }

    pub(crate) const fn requested_granularity_nanos(self) -> Option<u64> {
        match self.timestamp_granularity {
            Some(granularity) => Some(granularity.nanos()),
            None => None,
        }
    }

    /// Selects the acknowledgement policy for this store opening.
    ///
    /// Physical startup returns a structured support or recovery error if the
    /// requested guarantees cannot be established; it never downgrades.
    pub fn with_durability_policy(mut self, durability_policy: DurabilityPolicy) -> Self {
        self.durability_policy = durability_policy;
        self
    }

    pub(crate) const fn durability_policy(self) -> DurabilityPolicy {
        self.durability_policy
    }

    /// Selects the target size at which the active V2 WAL rotates before the
    /// next complete logical mutation.
    pub fn with_wal_segment_size(mut self, wal_segment_size: WalSegmentSize) -> Self {
        self.wal_segment_size = wal_segment_size;
        self
    }

    /// Returns the configured V2 WAL segment target.
    pub const fn wal_segment_size(self) -> WalSegmentSize {
        self.wal_segment_size
    }
}

#[cfg(test)]
pub(crate) fn durability_probe_options(physical: bool) -> DurableStoreOptions {
    DurableStoreOptions {
        durability_policy: if physical {
            DurabilityPolicy::Physical
        } else {
            DurabilityPolicy::Buffered
        },
        ..DurableStoreOptions::default()
    }
}
