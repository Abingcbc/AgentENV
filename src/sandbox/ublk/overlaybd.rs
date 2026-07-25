use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use overlaybd::backend::local::LocalFile;
use overlaybd::config::{LayerConfig, UpperMode};
use overlaybd::index_file::{merge_files_ro, CommitArgs};
use overlaybd::virtual_file::VirtualFile;
use serde::{Deserialize, Serialize};
use tracing::debug;

use super::device::UblkDevice;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OverlaybdConfig {
    pub image_config_path: PathBuf,
    pub read_only: bool,
    #[serde(default = "default_runtime_upper_mode")]
    pub runtime_upper_mode: UpperMode,
}

fn default_runtime_upper_mode() -> UpperMode {
    UpperMode::LogStructured
}

#[derive(Clone, Debug)]
pub(crate) struct OverlaybdRuntimeHandle {
    pub(crate) device: UblkDevice,
    pub(crate) image_config_path: PathBuf,
    pub(crate) actual_virtual_size: u64,
}

/// Compact multiple overlaybd layers into one sealed commit at `output_path`.
///
/// Opens each layer file, merges them via overlaybd's LSMT compaction.
pub(crate) async fn compact_layers(
    layers: &[LayerConfig],
    output_path: &Path,
) -> Result<Option<PathBuf>> {
    if layers.is_empty() {
        return Ok(None);
    }

    let io_ring = overlaybd::transient_io_ring::shared_transient_io_ring();

    let mut src_files: Vec<Arc<dyn VirtualFile>> = Vec::with_capacity(layers.len());
    for layer in layers {
        let path = Path::new(&layer.file);
        let file: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::open_ro(path, io_ring.clone())
                .await
                .with_context(|| format!("open layer for compaction: {}", path.display()))?,
        );
        src_files.push(file);
    }

    let output_file: Arc<dyn VirtualFile> = Arc::new(
        LocalFile::new(output_path, io_ring)
            .await
            .context("create compacted layer output file")?,
    );

    let mut commit_args = CommitArgs::new(output_file);
    commit_args.concurrency = 32;
    merge_files_ro(&src_files, commit_args)
        .await
        .context("merge layers")?;

    debug!(
        output = %output_path.display(),
        input_layers = layers.len(),
        "compacted overlaybd layers"
    );
    Ok(Some(output_path.to_path_buf()))
}
