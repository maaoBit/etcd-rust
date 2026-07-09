// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 Benjamin Chess
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

/// A scheduler that can execute deferred tasks.
pub trait Scheduler: Send + Sync {
    /// Schedule a closure to run after `delay`.
    fn schedule(&self, delay: Duration, f: Box<dyn FnOnce() + Send + 'static>);
    /// Stop the scheduler. No further tasks will be executed.
    fn stop(&self);
}

/// A simple deferred task scheduler backed by tokio timers.
pub struct SimpleScheduler {
    stopped: Arc<AtomicBool>,
    /// Notify when the scheduler is stopped so we can cancel pending timers.
    stop_notify: Arc<Notify>,
}

impl SimpleScheduler {
    pub fn new() -> Self {
        Self {
            stopped: Arc::new(AtomicBool::new(false)),
            stop_notify: Arc::new(Notify::new()),
        }
    }
}

impl Scheduler for SimpleScheduler {
    fn schedule(&self, delay: Duration, f: Box<dyn FnOnce() + Send + 'static>) {
        if self.stopped.load(Ordering::Relaxed) {
            return;
        }
        let stopped = self.stopped.clone();
        let stop_notify = self.stop_notify.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = tokio::time::sleep(delay) => {
                    if !stopped.load(Ordering::Relaxed) {
                        f();
                    }
                }
                _ = stop_notify.notified() => {
                    // Scheduler was stopped, do not run the task.
                }
            }
        });
    }

    fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
        self.stop_notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[tokio::test]
    async fn test_scheduler_executes_task() {
        let scheduler = SimpleScheduler::new();
        let executed = Arc::new(Mutex::new(false));
        let executed_clone = executed.clone();

        scheduler.schedule(Duration::from_millis(10), Box::new(move || {
            *executed_clone.lock().unwrap() = true;
        }));

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(*executed.lock().unwrap());
        scheduler.stop();
    }

    #[tokio::test]
    async fn test_scheduler_stop_prevents_execution() {
        let scheduler = SimpleScheduler::new();
        let executed = Arc::new(Mutex::new(false));
        let executed_clone = executed.clone();

        scheduler.schedule(Duration::from_millis(100), Box::new(move || {
            *executed_clone.lock().unwrap() = true;
        }));

        scheduler.stop();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!*executed.lock().unwrap());
    }
}
