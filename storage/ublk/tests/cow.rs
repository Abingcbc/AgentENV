use nix::sys::wait::waitpid;
use nix::unistd::{fork, ForkResult};
use rand::{rngs::SmallRng, RngExt, SeedableRng};
use std::io::Read;
use std::io::{pipe, PipeReader, PipeWriter};
use std::ops::{Deref, DerefMut};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::{fs::OpenOptions, io::Write};
use tempfile::TempDir;
use tokio::sync::mpsc as mpsc_async;
use tracing_log::log::LevelFilter;

use storage_util::io_ring::{spawn_io_ring_worker, IoRingHandle};
use uvm_ublk::{
    delete_dev, setup_tracing, ublk_available, wait_for_ublk_dev, BasicCowConfig, BasicCowTarget,
    UVMUblkCtrlBuilder, UVMUblkDevBuilder, UVMUblkTarget,
};

enum Message {
    DevStarted {
        dev_id: u32,
        dev_path: PathBuf,
        cfg: BasicCowConfig,
    },
    GoShutdown,
}

struct AutoDeleteFile {
    f: std::fs::File,
    path: PathBuf,
}

impl Deref for AutoDeleteFile {
    type Target = std::fs::File;

    fn deref(&self) -> &Self::Target {
        &self.f
    }
}

impl DerefMut for AutoDeleteFile {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.f
    }
}

impl Drop for AutoDeleteFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn write_str(writer: &mut PipeWriter, s: &str) {
    writer.write_all(&s.len().to_ne_bytes()).unwrap();
    writer.write_all(s.as_bytes()).unwrap();
}

fn read_str(reader: &mut PipeReader) -> String {
    let mut len_buf = [0u8; 8];
    reader.read_exact(len_buf.as_mut_slice()).unwrap();
    let len = usize::from_ne_bytes(len_buf);
    let mut buf = vec![0u8; len];
    reader.read_exact(buf.as_mut_slice()).unwrap();
    String::from_utf8(buf).expect("invalid string")
}

struct TestEnv {
    dev_id: u32,
    dev_path: PathBuf,
    cfg: BasicCowConfig,
    tx: mpsc_async::Sender<Message>,
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    #[allow(unused)]
    temp_dir: TempDir,
}

impl TestEnv {
    /// - `zc`: enable zero copy
    pub fn new(prefix: &str, size_mb: usize, zc: bool) -> Self {
        let mut rng = SmallRng::seed_from_u64(0xdeadbeef);
        let temp_dir = TempDir::new().unwrap();
        let origin_path = temp_dir.path().join(format!(
            "{prefix}-{}-origin",
            if zc { "zc" } else { "nozc" }
        ));
        let mut origin = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&origin_path)
            .unwrap();
        let cow_path = temp_dir
            .path()
            .join(format!("{prefix}-{}-cow", if zc { "zc" } else { "nozc" }));
        let cow = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&cow_path)
            .unwrap();
        cow.set_len((size_mb as u64) << 20).unwrap();

        // random fill the origin
        let mut buf = vec![0u8; 1024 * 1024];
        rng.fill(buf.as_mut_slice());
        for _ in 0..size_mb {
            origin.write_all(&buf).unwrap();
        }

        let cfg = BasicCowConfig {
            origin: origin_path,
            origin_dio: false,
            cow: cow_path,
            cow_dio: false,
            chunksize_kb: 16,
        };

        let (ctrl_ring, _) = spawn_io_ring_worker::<io_uring::squeue::Entry128>(0);
        let (ctrl_tx, mut main_rx) = mpsc_async::channel(4);
        let (main_tx, mut ctrl_rx) = mpsc_async::channel(4);
        let ctrl_ring_clone = ctrl_ring.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let ctrl = UVMUblkCtrlBuilder::new()
                    .name(BasicCowTarget::DEV_NAME)
                    .depth(16)
                    .nr_queues(1)
                    .zero_copy(zc)
                    .build(ctrl_ring_clone)
                    .unwrap();
                tracing::debug!("ctrl has been created");
                let tgt = BasicCowTarget::new(&cfg).unwrap();
                let mut dev = UVMUblkDevBuilder::new(ctrl)
                    .set_target(tgt)
                    .build()
                    .await
                    .unwrap();
                dev.start().await.unwrap();

                ctrl_tx
                    .send(Message::DevStarted {
                        dev_id: dev.dev_id(),
                        dev_path: dev.device_path(),
                        cfg,
                    })
                    .await
                    .unwrap();

                let msg = ctrl_rx.recv().await.unwrap();
                assert!(matches!(msg, Message::GoShutdown));
            });
        });
        let msg = main_rx.blocking_recv().unwrap();
        let Message::DevStarted {
            dev_path,
            cfg,
            dev_id,
        } = msg
        else {
            panic!("receive invalid message");
        };
        wait_for_ublk_dev(dev_id).unwrap();
        Self {
            dev_id,
            dev_path,
            cfg,
            tx: main_tx,
            ctrl_ring,
            temp_dir,
        }
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        self.tx.blocking_send(Message::GoShutdown).unwrap();
        // We just join the dev thread and stop the device, to make sure that nobody
        // opens ublkc device.
        let dev_id = self.dev_id;
        let ctrl_ring = self.ctrl_ring.clone();
        let h = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            rt.block_on(delete_dev(ctrl_ring, dev_id)).unwrap();
        });
        h.join().unwrap();
    }
}

#[test]
fn test_create() {
    test_create_(false);
}

#[test]
fn test_zc_create() {
    test_create_(true);
}

fn test_create_(zc: bool) {
    if !ublk_available() {
        eprintln!("ublk not available, skipping test");
        return;
    }
    setup_tracing(None, LevelFilter::Info).unwrap();
    let size_mb = 256;
    let (mut reader, mut writer) = pipe().unwrap();
    match unsafe { fork().expect("fork failed") } {
        ForkResult::Parent { child } => {
            drop(reader);
            let env = TestEnv::new("ublk-create", size_mb, zc);
            tracing::debug!("env has been created");
            write_str(&mut writer, env.cfg.origin.to_str().unwrap());
            write_str(&mut writer, env.dev_path.to_str().unwrap());
            drop(writer);
            let res = waitpid(child, None).unwrap();
            assert!(
                matches!(res, nix::sys::wait::WaitStatus::Exited(c, 0) if c == child),
                "weird child status {res:?}"
            );
        }
        ForkResult::Child => {
            drop(writer);
            let origin = read_str(&mut reader);
            let dev = read_str(&mut reader);
            drop(reader);
            // start the test in another process

            let mut blk_dev = OpenOptions::new().read(true).open(&dev).unwrap();
            // test read
            let mut origin = OpenOptions::new().read(true).open(&origin).unwrap();

            let mut buf = vec![0u8; 1 << 20];
            let mut origin_buf = vec![0u8; 1 << 20];
            for _ in 0..size_mb {
                blk_dev.read_exact(buf.as_mut_slice()).unwrap();
                origin.read_exact(origin_buf.as_mut_slice()).unwrap();
                assert_eq!(buf, origin_buf);
            }
            tracing::debug!("test_create finish");
            unsafe { libc::_exit(0) }
        }
    }
}

#[test]
fn test_rw() {
    test_rw_(false);
}

#[test]
fn test_zc_rw() {
    test_rw_(true);
}

fn test_rw_(zc: bool) {
    if !ublk_available() {
        eprintln!("ublk not available, skipping test");
        return;
    }
    setup_tracing(None, LevelFilter::Info).unwrap();
    let size_mb = 256;
    let (mut reader, mut writer) = pipe().unwrap();
    match unsafe { fork().expect("fork failed") } {
        ForkResult::Parent { child } => {
            drop(reader);
            let env = TestEnv::new("ublk-rw", size_mb, zc);
            tracing::debug!("env has been created");
            write_str(&mut writer, env.cfg.origin.to_str().unwrap());
            write_str(&mut writer, env.dev_path.to_str().unwrap());
            drop(writer);
            let res = waitpid(child, None).unwrap();
            assert!(
                matches!(res, nix::sys::wait::WaitStatus::Exited(c, 0) if c == child),
                "werid child status {res:?}"
            );
        }
        ForkResult::Child => {
            drop(writer);
            let origin = read_str(&mut reader);
            let dev = read_str(&mut reader);
            drop(reader);
            // start the test in another process
            let blk_dev = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&dev)
                .unwrap();
            let golden_path = format!("rw-golden{}", if zc { "-zc" } else { "" });

            std::fs::copy(&origin, &golden_path).unwrap();

            let golden = AutoDeleteFile {
                f: OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&golden_path)
                    .unwrap(),
                path: PathBuf::from(golden_path),
            };

            let mut rng = SmallRng::seed_from_u64(0xdeadbeef);

            for _ in 0..512 {
                // 60% read and 40 % write
                let read = rng.random_ratio(3, 5);
                // operation granularity range from 64 bytes to 8 MB.
                let len = rng.random_range(64..=8 * 1024);
                let offset = rng.random_range(0..((size_mb << 20) - len)) as u64;

                let mut buf = {
                    let mut buf = vec![0u8; len];
                    if !read {
                        rng.fill(buf.as_mut_slice());
                    }
                    buf
                };

                if read {
                    blk_dev.read_exact_at(buf.as_mut_slice(), offset).unwrap();
                    let mut golden_buf = vec![0u8; len];
                    golden
                        .read_exact_at(golden_buf.as_mut_slice(), offset)
                        .unwrap();
                    assert_eq!(golden_buf, buf);
                } else {
                    blk_dev.write_all_at(buf.as_slice(), offset).unwrap();
                    golden.write_all_at(buf.as_slice(), offset).unwrap();
                }
            }
            drop(golden);
            unsafe { libc::_exit(0) }
        }
    }
}
