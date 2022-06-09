//! From kafka/log/OffsetIndex.scala
//!


pub struct OffsetIndex {
    file: PathBuf,
    base_offset: i64,
    max_index_size: i32,
    writable: bool,
    entry_size: i32,
}

impl OffsetIndex {
    fn new(file: PathBuf, base_offset: i64) -> Self {
        Self{
            file,
            base_offset,
            max_index_size: -1,
            writable: true,
            entry_size: 8
        }
    }
}
