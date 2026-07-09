// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 Benjamin Chess
use std::fs;
use std::io;
use std::path::Path;

/// Keep only the `max` most recent files in `dir` whose names start with `prefix`.
/// Files are sorted by modification time (most recent first).
pub fn purge_file(dir: &Path, prefix: &str, max: usize) -> io::Result<()> {
    let mut entries: Vec<_> = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name().to_str().map_or(false, |name| name.starts_with(prefix))
        })
        .collect();

    // Sort by modification time, most recent first
    entries.sort_by(|a, b| {
        let a_mtime = a.metadata().and_then(|m| m.modified()).unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        let b_mtime = b.metadata().and_then(|m| m.modified()).unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        b_mtime.cmp(&a_mtime)
    });

    // Remove files beyond the max count
    for entry in entries.iter().skip(max) {
        fs::remove_file(entry.path())?;
    }

    Ok(())
}

/// Compute the total size (in bytes) of all files in `dir`.
pub fn dir_size(dir: &Path) -> io::Result<u64> {
    let mut total = 0u64;
    if dir.is_dir() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let meta = entry.metadata()?;
            if meta.is_file() {
                total += meta.len();
            } else if meta.is_dir() {
                total += dir_size(&entry.path())?;
            }
        }
    }
    Ok(total)
}

/// Check whether a directory is writable by creating and removing a temporary file.
pub fn is_dir_writeable(dir: &Path) -> io::Result<bool> {
    let test_file = dir.join(".write_test");
    match fs::File::create(&test_file) {
        Ok(_) => {
            let _ = fs::remove_file(&test_file);
            Ok(true)
        }
        Err(e) if e.kind() == io::ErrorKind::PermissionDenied => Ok(false),
        Err(e) => Err(e),
    }
}

/// Atomically create a symlink using a temporary file + rename.
/// This avoids partial symlink updates if the process crashes mid-write.
pub fn create_symlink(target: &Path, link: &Path) -> io::Result<()> {
    let tmp_link = link.with_extension("tmp");
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(target, &tmp_link)?;
    }
    #[cfg(not(unix))]
    {
        std::os::windows::fs::symlink_file(target, &tmp_link)?;
    }
    fs::rename(&tmp_link, link)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_purge_file() {
        let dir = std::env::temp_dir().join("purge_test");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        // Create test files that don't match the prefix
        for i in 0..3 {
            let path = dir.join(format!("other-{}", i));
            fs::write(&path, b"data").unwrap();
        }

        purge_file(&dir, "snap-", 2).unwrap();

        let remaining: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().into_string().unwrap())
            .collect();

        // All non-matching files should remain
        assert_eq!(remaining.len(), 3);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_is_dir_writeable() {
        let dir = std::env::temp_dir().join("writeable_test");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        assert!(is_dir_writeable(&dir).unwrap());
        let _ = fs::remove_dir_all(&dir);
    }
}
