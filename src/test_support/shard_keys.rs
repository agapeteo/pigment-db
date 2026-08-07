//! Opaque same-shard and different-shard key selection for unit tests.

use dashmap::DashMap;
use std::hash::BuildHasher;

const KEY_BYTES: usize = 32;
const SEARCH_LIMIT: usize = 1_000_000;

pub(crate) struct ShardKeySet {
    pub(crate) anchor: Vec<u8>,
    pub(crate) same_shard: Vec<u8>,
    pub(crate) different_shard: Vec<u8>,
}

pub(crate) fn select_shard_keys<V, S>(map: &DashMap<Vec<u8>, V, S>) -> ShardKeySet
where
    S: BuildHasher + Clone,
{
    let anchor = candidate(0);
    let anchor_shard = map.determine_map(&anchor);
    let mut same_shard = None;
    let mut different_shard = None;

    for index in 1..SEARCH_LIMIT {
        let key = candidate(index);
        if map.determine_map(&key) == anchor_shard {
            same_shard.get_or_insert_with(|| key.clone());
        } else {
            different_shard.get_or_insert_with(|| key.clone());
        }
        if same_shard.is_some() && different_shard.is_some() {
            break;
        }
    }

    ShardKeySet {
        anchor,
        same_shard: same_shard.expect("find an opaque same-shard key"),
        different_shard: different_shard.expect("find an opaque different-shard key"),
    }
}

fn candidate(index: usize) -> Vec<u8> {
    let mut key = vec![b'k'; KEY_BYTES];
    key[..std::mem::size_of::<usize>()].copy_from_slice(&index.to_ne_bytes());
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selector_returns_distinct_opaque_keys() {
        let map = DashMap::<Vec<u8>, ()>::new();
        let keys = select_shard_keys(&map);
        assert_ne!(keys.anchor, keys.same_shard);
        assert_ne!(keys.anchor, keys.different_shard);
        assert_ne!(keys.same_shard, keys.different_shard);
    }
}
