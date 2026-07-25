use storage_util::io_ring::{spawn_io_ring_worker, IoRingHandle};

pub fn test_io_ring() -> IoRingHandle {
    let (handle, _join) = spawn_io_ring_worker::<io_uring::squeue::Entry>(0);
    handle
}
