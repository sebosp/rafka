//! From kafka/server/checkpoints/LeaderEpochCheckpointFile.scala

pub trait LeaderEpochCheckpoint {
  fn write(epochs: Vec<EpochEntry>) -> Self;
  fn read() -> Vec<EpochEntry>;
}

const LEADER_EPOCH_CHECKPOINT_FILENAME: &str = "leader-epoch-checkpoint";
const WHITE_SPACES_PATTERN: Re = Pattern.compile("\\s+");
lazy_static! {
    static ref WHITE_SPACES_PATTERN: Regex =
            Regex::new(r""\s+"").unwrap();
}

const CURRENT_VERSION: u32 = 0;

pub struct LeaderEpochCheckpointFile {
}
impl LeaderEpochCheckpointFile {
  fn new_file(dir: PathBuf) -> PathBuf {
      unimplemented!()
  }
}
