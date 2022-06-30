//! From kafka/log/LazyIndex.scala

use super::{abstract_index::AbstractIndex, offset_index::OffsetIndex, time_index::TimeIndex};
use crate::utils::delete_file_if_exists;
use std::fs::rename;
use std::io;
use std::io::ErrorKind;
use std::path::PathBuf;

#[derive(Debug)]
pub enum LazyIndex {
    Offset((IndexFile, OffsetIndex)),
    Time((IndexFile, TimeIndex)),
}

impl LazyIndex {
    // RAFKA TODO: max_index_size = -1, writable = true
    pub fn for_offset(
        file: PathBuf,
        base_offset: i64,
        max_index_size: i32,
        writable: bool,
    ) -> Self {
        Self::Offset((
            IndexFile::new(file.clone()),
            OffsetIndex::new(file, base_offset, Some(max_index_size), Some(writable)),
        ))
    }

    // RAFKA TODO: max_index_size = -1, writable = true
    pub fn for_time(file: PathBuf, base_offset: i64, max_index_size: i32, writable: bool) -> Self {
        Self::Time((
            IndexFile::new(file.clone()),
            TimeIndex::new(file, base_offset, Some(max_index_size), Some(writable)),
        ))
    }

    pub fn close(&self) {
        match self {
            Self::Offset((index_file, _offset_index)) => {
                index_file.close();
            },
            Self::Time((index_file, _time_index)) => {
                index_file.close();
            },
        }
    }
}

trait IndexWrapper {
    fn file(&self) -> PathBuf;
    fn update_parent_dir(&mut self, f: PathBuf);
    fn rename_to(&mut self, dest: PathBuf) -> Result<(), io::Error>;
    fn delete_if_exists(&self) -> bool;
    fn close(&self);
    fn close_handler(&self);
}
#[derive(Debug)]
struct IndexFile {
    file: PathBuf,
}

impl IndexFile {
    fn new(file: PathBuf) -> Self {
        Self { file }
    }
}

impl IndexWrapper for IndexFile {
    fn file(&self) -> PathBuf {
        self.file.clone()
    }

    fn update_parent_dir(&mut self, parent_dir: PathBuf) {
        parent_dir.push(self.file.file_name().unwrap());
        self.file = parent_dir;
    }

    fn rename_to(&mut self, dest: PathBuf) -> Result<(), io::Error> {
        if let Err(err) = rename(&self.file, dest) {
            if err.kind() != ErrorKind::NotFound {
                return Err(err);
            }
        }
        self.file = dest;
        Ok(())
    }

    fn delete_if_exists(&self) {
        delete_file_if_exists(&self.file)
    }

    fn close(&self) {}

    fn close_handler(&self) {}
}

impl AbstractIndex for IndexFile {}
