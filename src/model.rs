use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::Debug;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct KeyValueRequest {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SortedMapEntry {
    key: Vec<u8>,

    search_key: SearchKey,

    #[serde(with = "serde_bytes")]
    value: Vec<u8>,
}

impl SortedMapEntry {
    pub fn new(key: Vec<u8>, search_key: SearchKey, value: Vec<u8>) -> Self {
        Self {
            key,
            search_key,
            value,
        }
    }

    pub fn entry(self) -> (Vec<u8>, SearchKey, Vec<u8>) {
        (self.key, self.search_key, self.value)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SortedMapKey {
    key: Vec<u8>,
    search_key: SearchKey,
}

impl SortedMapKey {
    pub fn new(key: Vec<u8>, search_key: SearchKey) -> Self {
        Self { key, search_key }
    }
    pub fn owned(self) -> (Vec<u8>, SearchKey) {
        (self.key, self.search_key)
    }
}

#[derive(Eq, Debug, Clone, Serialize, Deserialize)]
pub struct SearchKey(Vec<Key>);

impl SearchKey {
    pub fn first(&self) -> Option<&Key> {
        self.0.first()
    }

    pub fn get(&self, idx: usize) -> Option<&Key> {
        self.0.get(idx)
    }

    pub fn into_key_vec(self) -> Vec<Key> {
        self.0
    }
    
    pub fn slice(&self) -> &[Key] {
        self.0.as_slice()
    }
}

pub trait BytesLen {
    fn bytes_len(&self) -> usize;
}
impl BytesLen for SearchKey {
    fn bytes_len(&self) -> usize {
        self.0.iter().map(|k| k.bytes_len()).sum()
    }
}
impl BytesLen for Vec<u8> {
    fn bytes_len(&self) -> usize {
        self.len()
    }
}

impl BytesLen for String {
    fn bytes_len(&self) -> usize {
        self.len()
    }
}

impl BytesLen for &'static str {
    fn bytes_len(&self) -> usize {
        self.len()
    }
}

impl BytesLen for Key {
    fn bytes_len(&self) -> usize {
        match &self {
            Key::Bool(_) => 1,
            Key::I(_) => 1,
            Key::U8(_) => 1,
            Key::I16(_) => 2,
            Key::U16(_) => 2,
            Key::I32(_) => 4,
            Key::U32(_) => 4,
            Key::I64(_) => 8,
            Key::U64(_) => 8,
            Key::USIZE(_) => (usize::BITS / 8) as usize,
            Key::I128(_) => 16,
            Key::U128(_) => 16,
            Key::Char(_) => 4,
            Key::Str(str) => str.bytes_len(),
            Key::Bytes(bytes) => bytes.len()
        }
    }
}

impl From<usize> for SearchKey {
    fn from(value: usize) -> Self {
        Self(vec![Key::USIZE(value)])
    }
}

impl From<&'static str> for SearchKey {
    fn from(value: &'static str) -> Self {
        Self(vec![Key::Str(value.into())])
    }
}

impl From<Vec<u8>> for SearchKey {
    fn from(value: Vec<u8>) -> Self {
        Self(vec![Key::Bytes(value)])
    }
}

impl From<&[u8]> for SearchKey {
    fn from(value: &[u8]) -> Self {
        Self(vec![Key::Bytes(value.to_vec())])
    }
}

impl From<Vec<Key>> for SearchKey {
    fn from(value: Vec<Key>) -> Self {
        Self(value)
    }
}

impl From<&[Key]> for SearchKey {
    fn from(value: &[Key]) -> Self {
        Self(Vec::from(value))
    }
}

impl Ord for SearchKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialEq<Self> for SearchKey {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl PartialOrd for SearchKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.0.cmp(&other.0))
    }
}

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum Key {
    Bool(bool),
    I(i8),
    U8(u8),
    I16(i16),
    U16(u16),
    I32(i32),
    U32(u32),
    I64(i64),
    U64(u64),
    USIZE(usize),
    I128(u64),
    U128(u128),
    Char(char),
    Str(String),
    Bytes(Vec<u8>),
}

pub const MIN_BYTES: Vec<u8> = vec![];

// pub const ALL_BYTES_RANGE: Range<SearchKey> = (SearchKey::from(MIN_BYTES)...);

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        ops::Bound,
        sync::Arc,
        time::{Duration, Instant},
    };

    use dashmap::DashMap;
    use std::ops::Bound::Included;
    use std::ops::Bound::Unbounded;

    #[test]
    fn test_key_ord() {
        let empty: Vec<u8> = vec![];
        let zero: Vec<u8> = vec![0];
        let zero_zero: Vec<u8> = vec![0, 0];
        assert!(empty < zero);
        assert!(empty < zero_zero);
        assert!(zero < zero_zero);
    }

    // pub fn test_slice_range() {
    //     let mut map: BTreeMap<Vec<u8>, &'static str> = BTreeMap::new();
    //     map.insert(vec![0], "0");
    //     map.insert(vec![0, 255], "0, 0");
    //     map.insert(vec![1, 0], "1, 0");
    //     map.insert(vec![255, 255], "1, 0");
    //
    //     for (&k, &v) in map.range((Unbounded, Unbounded)) {
    //         println!("{}", v)
    //     }
    // }

    #[test]
    pub fn test_str_range() {
        let mut map: BTreeMap<&'static str, usize> = BTreeMap::new();
        map.insert("apple", 1);
        map.insert("brother", 2);
        map.insert("cool", 3);
        map.insert("door", 4);

        for (k, _v) in map.range::<&str, (Bound<&str>, Bound<&str>)>((Included("az"), Unbounded)) {
            println!("-> {}", k)
        }
    }

    // #[test]
    // pub fn search_key_range() {
    //     let mut map: BTreeMap<SearchKey, Box<dyn Debug>> = BTreeMap::new();
    //     map.insert(1.into(), boxed(1));
    //     map.insert(33.into(), boxed(33));
    //
    //     map.insert("apple".into(), boxed("apple"));
    //     map.insert("banana".into(), boxed("banana"));
    //     map.insert("maple".into(), boxed("maple"));
    //     map.insert("zoo".into(), boxed("zoo"));
    //
    //     // map.insert(vec![].into(), boxed::<Vec<u8>>(vec![]));
    //     map.insert(vec![0].into(), boxed::<Vec<u8>>(vec![0]));
    //     map.insert(vec![255].into(), boxed::<Vec<u8>>(vec![255]));
    //
    //     for (_k, v) in map.range::<SearchKey, (Bound<SearchKey>, Bound<SearchKey>)>((
    //         Included("be".into()),
    //         Unbounded,
    //     )) {
    //         println!("-> {:?}", v)
    //     }
    // }

    #[test]
    pub fn test_dashmap_compute() {
        let map: std::sync::Arc<DashMap<&str, Vec<usize>>> =
            std::sync::Arc::new(DashMap::with_capacity(1));
        map.insert("a", vec![1]);
        map.insert("b", vec![2]);

        let t1_map = map.clone();
        let t1 = std::thread::spawn(move || {
            let opt = t1_map.get_mut("a");
            if let Some(mut val) = opt {
                val.value_mut().push(1);

                // std::thread::sleep(Duration::from_secs(1));
                // println!("after sleep t1");

                let opt_b = t1_map.get_mut("b");
                if let Some(mut val_other) = opt_b {
                    val_other.value_mut().push(1);
                }
            }
        });

        let t2_map = map.clone();
        let t2 = std::thread::spawn(move || {
            let opt = t2_map.get_mut("b");
            if let Some(mut val) = opt {
                val.value_mut().push(2);

                // std::thread::sleep(Duration::from_secs(1));
                // println!("after sleep t2");

                let opt_a = t2_map.get_mut("a");
                if let Some(mut val_other) = opt_a {
                    val_other.value_mut().push(2);
                }
            }
        });

        t1.join().unwrap();
        t2.join().unwrap();

        // let opt = map.get_mut("a");
        // if let Some(mut val) = opt {
        //     val.push(4);
        // }

        let opt = map.get("a");
        if let Some(vec) = opt {
            println!("a => {:?}", vec.value());
        }

        let opt = map.get("b");
        if let Some(vec) = opt {
            println!("b => {:?}", vec.value());
        }
    }

    #[test]
    fn test_map_lock() {
        let map = DashMap::new();
        map.insert(1, 1);
        map.insert(10, 10);
        let map_main = Arc::new(map);
        let map_1 = map_main.clone();
        let map_2 = map_main.clone();
        let started = Instant::now();

        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(3));
            if let Some(mut val) = map_1.get_mut(&1) {
                // std::thread::sleep(Duration::from_secs(3));
                *val.value_mut() += 1;
            } else {
                panic!("no sleep")
            }
            // }).join().unwrap();
        });

        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(500));
            map_2.remove(&1);
        })
        .join()
        .unwrap();

        println!("elapsed secs: {}", started.elapsed().as_secs());
        println!("map size: {}", map_main.len());
    }
}
