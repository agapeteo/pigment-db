//! Validated timestamp configuration shared by all durable store families.

use std::error::Error;
use std::fmt;
use std::num::NonZeroU64;
use std::time::Duration;

const DEFAULT_GRANULARITY_NANOS: u64 = 60_000_000_000;

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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
/// Additive options shared by all durable store families.
///
/// Passing options when opening an existing V1 store may stage a compacted V1
/// replacement to change its persisted granularity. The last accepted bucket
/// and logical state are preserved. Options never authorize legacy migration.
pub struct DurableStoreOptions {
    timestamp_granularity: TimestampGranularity,
}

impl DurableStoreOptions {
    /// Selects a validated timestamp granularity.
    pub fn with_timestamp_granularity(
        mut self,
        timestamp_granularity: TimestampGranularity,
    ) -> Self {
        self.timestamp_granularity = timestamp_granularity;
        self
    }

    pub(crate) const fn granularity_nanos(self) -> u64 {
        self.timestamp_granularity.nanos()
    }
}
