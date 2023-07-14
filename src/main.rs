use bytes::Bytes;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use thiserror::Error;

const LOGO: &str = r"
___.            __         .__                         
\_ |__ ___.__._/  |_  ____ |  |__ _____   __ __  ______
 | __ <   |  |\   __\/ __ \|  |  \\__  \ |  |  \/  ___/
 | \_\ \___  | |  | \  ___/|   Y  \/ __ \|  |  /\___ \ 
 |___  / ____| |__|  \___  >___|  (____  /____//____  >
     \/\/                \/     \/     \/           \/ 
";

fn main() {
    // No beans? No beans.
    println!("Hello, world!");
    println!("{LOGO}");
}

type KeyDir = HashMap<String, KeyDirRecord>;

/// Record for the in-memory cache that holds details about the ByteHausRecord
/// Exists in a 1:1 Mapping for each ByteHausRecord
struct KeyDirRecord {
    /// The ID of the File that this lives in
    file_id: Box<Path>,
    /// The size of the value
    value_sz: usize,
    /// The position of the value on Disk, maybe a pointer? An offset?
    /// TODO Figure out what the right data type in rust is here
    value_pos: usize,
    /// timestamp
    tstamp: usize,
}

/// This is the byte-aligned format for a single bytehaus record
struct ByteHausRecord {
    /// crc to prevent data corruption
    pub crc: i32,
    /// timestamp
    tstamp: i32,

    /// The Key Size in bytes
    pub ksz: usize,
    /// The value size in Bytes
    pub value_sz: usize,

    /// The Actual key
    pub key: Bytes,
    /// The Actual Value
    pub value: Bytes,
}

impl ByteHausRecord {
    fn get_keydir_record(&self) -> Option<KeyDirRecord> {
        None
    }
}

/// The entire DB is a single directory
struct ByteHaus {
    /// The current directory
    root_directory: PathBuf,

    /// this is a live counter of how many bytes we've written to a particular latest file, so we
    /// can see when to swap it out with a new one
    current_file_size_bytes: usize,

    /// The size after which we switch to a new active file
    max_file_size_bytes: usize,

    /// The latest file
    latest_file: PathBuf,

    /// Older Files, will be useful when we decice on how to compact and merge
    /// This is under an ArcMutex because we want the compaction thread to be able to have access
    /// to this while being owned by the ByteHaus itself
    previous_files: Arc<Mutex<Vec<PathBuf>>>,

    /// The In-Memory Index of key to value
    keydir: KeyDir,
}

/// ByteHaus is a simple implementation of the BitCask Protocol
impl ByteHaus {
    /// Open the Database File
    pub fn open_db(&mut self) -> Result<(), ByteHausError> {
        Ok(())
    }

    /// Read from the ByteHaus
    pub fn read(&mut self, key: String) -> Option<String> {
        None
    }

    /// Write to the ByteHaus
    pub fn write(&mut self, key: String, value: String) -> Result<(), ByteHausError> {
        Ok(())
    }

    /// Delete from the ByteHaus
    /// If the record doesn't exist, we return a bytehauserror
    pub fn delete(&mut self, key: String) -> Result<(), ByteHausError> {
        Ok(())
    }

    /// Close the ByteHaus
    pub fn close_db(&mut self) -> Result<(), ByteHausError> {
        // TODO
        // - Drop the In-Mem Hashmap
        // - Make sure the file handles are closed
        Ok(())
    }

    // internal utilities

    /// Create a new data file and set it as the primary
    fn cycle_data_file(&mut self) -> Result<(), ByteHausError> {
        Ok(())
    }

    /// Launch the Compaction Process on a separate thread
    fn launch_compactor(&mut self) -> Result<(), ByteHausError> {
        Ok(())
    }

    /// Update the KeyDir with a new record
    fn update_keydir(&mut self, new_record: KeyDirRecord) -> Result<(), ByteHausError> {
        Ok(())
    }

    /// Reconstruct the entire database from the Hint File
    fn reconstruct_from_hint_file(&mut self) -> Result<KeyDir, ByteHausError> {
        Ok(KeyDir::new())
    }

    /// Actually write append-only to our ByteHaus File
    /// TODO Decide on where to do the serialization/deserialization
    fn write_to_file(&mut self, record: ByteHausRecord) -> Result<(), ByteHausError> {
        Ok(())
    }
}

/// Structure to represent the compactor thread that runs in the background
/// TODO Decide on strategy - make it pluggable?
pub struct Compactor {}
impl Compactor {
    /// Function to merge two record files
    fn merge_files() -> Result<(), ByteHausError> {
        Ok(())
    }

    /// Create a Hint File
    fn create_hint_file() -> Result<(), ByteHausError> {
        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum ByteHausError {
    #[error("Writing to the Disk Failed")]
    DiskWriteFailed,

    #[error("Could not reconstruct the KeyDir from the Hint File!")]
    ReconstructionFailed,

    #[error("Could not Update the Keydir")]
    KeyDirUpdateFailed,

    #[error("Could not Open the Data File")]
    DataFileOpenFailed,

    #[error("Could not Create the Data File")]
    DataFileCreateFailed,

    #[error("Could not write to the data file")]
    DataFileWriteFailed,

    #[error("Could not Launch the Compactor")]
    CompactorLaunchFailed,

    #[error("Could not merge two data files")]
    DataFileMergeFailed,

    #[error("Could not merge two data files")]
    HintFileCreationFailed,

    #[error("Key does not exist")]
    DeleteKeyMissing,
}
