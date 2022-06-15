//! From kafka/log/AbstractIndex.scala

pub trait AbstractIndex {
    fn close(&self) {
        self.trim_to_valid_size();
        self.close_handler();
    }
    fn trim_to_valid_size(&self) {}
    fn close_handler(&self) {
        // Drop the mmap
    }
}
