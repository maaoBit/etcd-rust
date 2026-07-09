// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 Benjamin Chess

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

use crate::store::Store;

/// Determines the compaction scheduling strategy.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum CompactorMode {
    /// Compact at a fixed time interval, keeping the latest revision.
    Periodic,
    /// Compact when a threshold number of new revisions have been created.
    Revision,
}

/// Schedules automatic compaction of an in-memory Store.
///
/// # Periodic mode
/// The `retention` string is parsed as a human-readable duration (e.g. `"1h30m"`).
/// Compaction fires once per interval and compacts up to `current_revision()`.
///
/// # Revision mode
/// The `retention` string is parsed as an integer revision count (e.g. `"1000"`).
/// Compaction fires when `current_revision - last_compacted >= retention`.
///
/// A retention value of `"0"` or an unparseable string disables the compactor.
pub struct Compactor {
    mode: CompactorMode,
    retention: String,
    last_compacted: Arc<AtomicI64>,
    paused: Arc<AtomicBool>,
    store: Arc<Store>,
    stop_ch: Option<oneshot::Sender<()>>,
}

impl Compactor {
    /// Create a new compactor. Does not start running until [`Compactor::run`] is called.
    pub fn new(mode: CompactorMode, retention: &str, store: Arc<Store>) -> Self {
        let last_compacted = Arc::new(AtomicI64::new(store.compacted_revision()));
        Compactor {
            mode,
            retention: retention.to_string(),
            last_compacted,
            paused: Arc::new(AtomicBool::new(false)),
            store,
            stop_ch: None,
        }
    }

    /// Start the compaction background task. No-op if already running.
    pub fn run(&mut self) {
        if self.stop_ch.is_some() {
            return; // already running
        }

        let (tx, mut rx) = oneshot::channel::<()>();
        self.stop_ch = Some(tx);

        let mode = self.mode;
        let retention = self.retention.clone();
        let store = self.store.clone();
        let last_compacted = self.last_compacted.clone();
        let paused = self.paused.clone();
        let initial_last = store.compacted_revision();
        last_compacted.store(initial_last, Ordering::Relaxed);

        tokio::spawn(async move {
            match mode {
                CompactorMode::Periodic => {
                    let interval_dur = match parse_duration(&retention) {
                        Ok(d) if d.is_zero() => return,
                        Ok(d) => d,
                        Err(_) => return,
                    };
                    let mut interval = tokio::time::interval(interval_dur);
                    // Skip the first immediate tick that tokio::time::interval produces.
                    interval.tick().await;

                    loop {
                        tokio::select! {
                            _ = interval.tick() => {
                                if paused.load(Ordering::Relaxed) {
                                    continue;
                                }
                                let rev = store.current_revision();
                                if let Err(e) = store.compact(rev) {
                                    log::warn!("periodic compaction failed: {}", e);
                                } else {
                                    last_compacted.store(rev, Ordering::Relaxed);
                                }
                            }
                            _ = &mut rx => break,
                        }
                    }
                }
                CompactorMode::Revision => {
                    let retention_revs: i64 = match retention.parse() {
                        Ok(0) | Err(_) => return,
                        Ok(n) => n,
                    };
                    let mut check_interval = tokio::time::interval(Duration::from_secs(1));

                    loop {
                        tokio::select! {
                            _ = check_interval.tick() => {
                                if paused.load(Ordering::Relaxed) {
                                    continue;
                                }
                                let current = store.current_revision();
                                let last = last_compacted.load(Ordering::Relaxed);
                                if current - last >= retention_revs {
                                    if let Err(e) = store.compact(current) {
                                        log::warn!("revision compaction failed: {}", e);
                                    } else {
                                        last_compacted.store(current, Ordering::Relaxed);
                                    }
                                }
                            }
                            _ = &mut rx => break,
                        }
                    }
                }
            }
        });
    }

    /// Stop the compaction background task. Safe to call even if not running.
    pub fn stop(&mut self) {
        if let Some(tx) = self.stop_ch.take() {
            let _ = tx.send(());
        }
    }

    /// Pause the compaction background task without stopping it.
    /// Compaction will resume on the next tick after [`Compactor::resume`].
    pub fn pause(&self) {
        self.paused.store(true, Ordering::Relaxed);
    }

    /// Resume a previously paused compactor.
    pub fn resume(&self) {
        self.paused.store(false, Ordering::Relaxed);
    }
}

/// Parse a human-readable duration string into a [`Duration`].
///
/// Supported suffixes: `h` (hours), `m` (minutes), `s` (seconds).
/// Examples: `"1h30m"`, `"45m"`, `"10s"`, `"2h"`.
fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    if s.is_empty() || s == "0" {
        return Ok(Duration::ZERO);
    }
    let mut total = Duration::ZERO;
    let mut current = String::new();
    for c in s.chars() {
        if c.is_ascii_digit() {
            current.push(c);
        } else {
            if current.is_empty() {
                return Err(format!("invalid duration string: '{}'", s));
            }
            let val: u64 = current
                .parse()
                .map_err(|_| format!("invalid duration string: '{}'", s))?;
            match c {
                'h' => total += Duration::from_secs(
                    val.checked_mul(3600).ok_or_else(|| "duration overflow".to_string())?,
                ),
                'm' => total += Duration::from_secs(
                    val.checked_mul(60).ok_or_else(|| "duration overflow".to_string())?,
                ),
                's' => total += Duration::from_secs(val),
                other => return Err(format!("unknown duration suffix: '{}'", other)),
            }
            current.clear();
        }
    }
    if !current.is_empty() {
        return Err(format!("incomplete duration string: '{}'", s));
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration_zero() {
        assert_eq!(parse_duration("0").unwrap(), Duration::ZERO);
        assert_eq!(parse_duration("").unwrap(), Duration::ZERO);
    }

    #[test]
    fn test_parse_duration_hours() {
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7200));
    }

    #[test]
    fn test_parse_duration_minutes() {
        assert_eq!(parse_duration("30m").unwrap(), Duration::from_secs(1800));
        assert_eq!(parse_duration("1m").unwrap(), Duration::from_secs(60));
    }

    #[test]
    fn test_parse_duration_seconds() {
        assert_eq!(parse_duration("45s").unwrap(), Duration::from_secs(45));
        assert_eq!(parse_duration("0s").unwrap(), Duration::ZERO);
    }

    #[test]
    fn test_parse_duration_combined() {
        assert_eq!(parse_duration("1h30m").unwrap(), Duration::from_secs(5400));
        assert_eq!(parse_duration("1h30m10s").unwrap(), Duration::from_secs(5410));
        assert_eq!(parse_duration("2h15m30s").unwrap(), Duration::from_secs(8130));
    }

    #[test]
    fn test_parse_duration_errors() {
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("1x").is_err());
        assert!(parse_duration("h").is_err());
        assert!(parse_duration("1h30").is_err()); // trailing number without suffix
    }

    #[test]
    fn test_compactor_mode_equality() {
        assert_eq!(CompactorMode::Periodic, CompactorMode::Periodic);
        assert_eq!(CompactorMode::Revision, CompactorMode::Revision);
        assert_ne!(CompactorMode::Periodic, CompactorMode::Revision);
    }
}
