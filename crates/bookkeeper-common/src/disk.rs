//! Disk-space queries used by bookie server endpoints and the disk monitor.
//!
//! Wraps `statvfs(3)` on Unix; returns zeros on non-Unix targets (the bookie
//! is Unix-only in practice, but the helpers must compile cross-platform for
//! CI).

use std::io;
use std::path::Path;

/// Returns `(total_bytes, available_bytes)` for the filesystem containing
/// `path`. On Unix this is `f_blocks * f_frsize` and `f_bavail * f_frsize`
/// respectively. On non-Unix targets both values are 0.
#[cfg(unix)]
pub fn statvfs_bytes(path: &Path) -> io::Result<(u64, u64)> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let c_path = CString::new(path.as_os_str().as_bytes())
        .map_err(io::Error::other)?;
    // SAFETY: `c_path` is a valid NUL-terminated string and `stat` is a
    // zeroed `libc::statvfs` owned by this stack frame for the duration of
    // the call.
    unsafe {
        let mut stat: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c_path.as_ptr(), &mut stat) != 0 {
            return Err(io::Error::last_os_error());
        }
        let total = stat.f_blocks as u64 * stat.f_frsize as u64;
        let available = stat.f_bavail as u64 * stat.f_frsize as u64;
        Ok((total, available))
    }
}

#[cfg(not(unix))]
pub fn statvfs_bytes(_path: &Path) -> io::Result<(u64, u64)> {
    Ok((0, 0))
}

/// Convenience: same as [`statvfs_bytes`] but returns `(usage_ratio,
/// used_bytes, total_bytes)` where `usage_ratio` is in `[0.0, 1.0]`. Returns
/// `(0.0, 0.0, 0.0)` when the filesystem reports zero total (empty /
/// non-Unix).
pub fn statvfs_usage(path: &Path) -> io::Result<(f64, f64, f64)> {
    let (total, available) = statvfs_bytes(path)?;
    let total_f = total as f64;
    let available_f = available as f64;
    if total_f == 0.0 {
        return Ok((0.0, 0.0, 0.0));
    }
    let used = total_f - available_f;
    Ok((used / total_f, used, total_f))
}
