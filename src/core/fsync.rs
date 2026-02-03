//! fsync
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use libc::{open, fsync, close, O_DIRECTORY, O_RDONLY};

fn sync_dir(path: &Path) -> io::Result<()> {
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    let fd = unsafe { open(c_path.as_ptr(), O_RDONLY | O_DIRECTORY) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let res = unsafe { fsync(fd) };
    unsafe { close(fd) };
    if res < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

pub fn create_file_sync(path: &Path) -> io::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)?;

    if let Some(dir) = path.parent() {
        sync_dir(dir)?;
    }

    Ok(file)
}
