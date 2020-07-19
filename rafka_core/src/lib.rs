#![warn(rust_2018_idioms)]
#[macro_use]
extern crate failure;
#[macro_use]
extern crate slog_term;

use tokio::prelude::*;
use tokio_zookeeper::*;

mod server;
mod utils;
mod zk;
mod zookeeper;
#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
