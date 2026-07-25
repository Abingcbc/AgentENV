use std::sync::{
    atomic::{AtomicUsize, Ordering},
    OnceLock,
};

use storage_util::io_ring::{spawn_io_ring_worker, IoRingHandle};

const TRANSIENT_IO_RING_LANES: usize = 4;

struct SharedTransientIoRings {
    handles: [IoRingHandle<io_uring::squeue::Entry>; TRANSIENT_IO_RING_LANES],
    next: AtomicUsize,
}

impl SharedTransientIoRings {
    fn new() -> Self {
        let handles = std::array::from_fn(|id| {
            let (handle, _join_handle) = spawn_io_ring_worker::<io_uring::squeue::Entry>(id);
            handle
        });

        Self {
            handles,
            next: AtomicUsize::new(0),
        }
    }

    fn next_handle(&self) -> IoRingHandle<io_uring::squeue::Entry> {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.handles.len();
        self.handles[idx].clone()
    }
}

/// Shared io_uring workers for short-lived overlaybd utility tasks.
///
/// Repeatedly spawning temporary workers in the long-running AgentENV server
/// causes allocator arena growth that keeps anonymous RSS elevated even after
/// the worker threads exit. Reusing a small process-global worker set avoids
/// that churn while reducing contention under concurrent transient metadata IO.
fn shared_transient_io_rings() -> &'static SharedTransientIoRings {
    static RINGS: OnceLock<SharedTransientIoRings> = OnceLock::new();

    RINGS.get_or_init(SharedTransientIoRings::new)
}

pub fn shared_transient_io_ring() -> IoRingHandle<io_uring::squeue::Entry> {
    shared_transient_io_rings().next_handle()
}

#[cfg(test)]
mod tests {
    use super::shared_transient_io_ring;

    #[test]
    fn shared_transient_io_ring_has_four_worker_channels() {
        let first_ref = super::shared_transient_io_rings();
        let second_ref = super::shared_transient_io_rings();

        assert!(std::ptr::eq(first_ref, second_ref));
        assert_eq!(first_ref.handles.len(), super::TRANSIENT_IO_RING_LANES);
        assert_eq!(first_ref.handles.len(), 4);

        let worker_ids = first_ref
            .handles
            .iter()
            .map(|handle| handle.worker_id)
            .collect::<Vec<_>>();
        assert_eq!(worker_ids, vec![0, 1, 2, 3]);
    }

    #[test]
    fn shared_transient_io_ring_returns_a_pool_worker() {
        let handle = shared_transient_io_ring();

        assert!(handle.worker_id < super::TRANSIENT_IO_RING_LANES);
    }
}
