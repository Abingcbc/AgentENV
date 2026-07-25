# ublk

This is a ublk server binary, similar to [libublk-rs](https://github.com/ublk-org/libublk-rs), but targeted at the tokio async runtime.

## Prerequisite

This ublk daemon is only tested on Ubuntu 24.04, with Ubuntu kernel version as 6.8.0-87-generic and 6.17.0-1007-oem.

You'd better make sure your kernel version is 6.8.

## Build

```rust
cargo build

# Or release mode
# cargo build --release

# Check help
./target/debug/uvm-ublk --help
```

## Running

`uvm-ublk` attempts to load the `ublk` kernel module automatically when it starts. If automatic loading fails, please load it manually following the below instructions, or rerun `cargo run --bin server -- --setup-only` to refresh both module state and device permissions.

```
# First insert kernel module
modprobe ublk_drv ublks_max=65536

# Then prepare your own base image
BASE_IMG=/path/to/your/base
COW_IMG=/path/to/store/the/local/write
UBLK_ID=1
size=$(stat -c %s $BASE_IMG)
truncate -s ${size} $COW_IMG
args=(--nr-queues 1 --depth 16 $UBLK_ID cow\
    --chunksize-kb 64 \
    --origin $BASE_IMG --cow $COW_IMG \
    --origin-dio
)

# This will start a command in current shell
./target/debug/uvm-ublk create "${args[@]}"

# Then you can access /dev/ublkb1 in another shell

# To remove the ublk device
./target/debug/uvm-ublk delete 1
```

Note that the `ublks_max=65536` module parameter is mainly used for lower-version kernels (e.g., 6.8.0). If your kernel version is higher (e.g., 6.17), you do not need this.

Do not use the zero-copy feature of uvm-ublk (e.g., --zero-copy) for a fuse-based filesystem if your kernel version is < 6.18. It has a bug in the kernel, which will be fixed in kernel 6.18.
