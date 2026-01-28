// Log Storage
use crate::bs::Entry;
use std::fs::OpenOptions;
use std::io::{self, Seek, SeekFrom};
use std::path::PathBuf;

pub struct Log {
    filename: PathBuf,
    fileptr: std::fs::File,
}

impl Log {
    pub fn open(filename: impl Into<PathBuf>) -> io::Result<Self> {
        let filename = filename.into();
        let fileptr = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&filename)?;

        Ok(Log { filename, fileptr })
    }

    pub fn close(self) -> io::Result<()> {
        Ok(())
    }

    pub fn write(&mut self, entry: &Entry) -> io::Result<()> {
        self.fileptr.seek(SeekFrom::End(0))?;
        entry.encode_into(&mut self.fileptr)?;
        Ok(())
    }

    pub fn read(&mut self) -> io::Result<Option<Entry>> {
        match Entry::decode(&mut self.fileptr) {
            Ok(entry) => Ok(Some(entry)),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::SeekFrom;

    #[test]
    fn log_write_then_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");

        let mut log = Log::open(&path).unwrap();

        let e1 = Entry::new(b"a".to_vec(), b"1".to_vec());
        let e2 = Entry::new(b"b".to_vec(), b"2".to_vec());

        log.write(&e1).unwrap();
        log.write(&e2).unwrap();

        // читаем с начала
        log.fileptr.seek(SeekFrom::Start(0)).unwrap();

        let r1 = log.read().unwrap().unwrap();
        let r2 = log.read().unwrap().unwrap();
        let r3 = log.read().unwrap();

        assert_eq!(r1.key(), b"a");
        assert_eq!(r2.key(), b"b");
        assert!(r3.is_none());
    }
}
