use std::path::Path;
use std::process::Command;

use anyhow::{bail, Context, Result};
use tracing::info;

fn kvm_accessible() -> bool {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/kvm")
        .is_ok()
}

pub fn add_user_to_group(user: &str, group: &str) -> Result<bool> {
    let groups = Command::new("id")
        .args(["-nG", user])
        .output()
        .with_context(|| format!("look up groups for {user}"))?;
    if !groups.status.success() {
        bail!("failed to look up groups for {user}");
    }
    if String::from_utf8_lossy(&groups.stdout)
        .split_whitespace()
        .any(|current| current == group)
    {
        return Ok(false);
    }

    let status = Command::new("usermod")
        .arg("-aG")
        .arg(group)
        .arg(user)
        .status()
        .context("failed to run usermod")?;
    if !status.success() {
        bail!("failed to add {user} to the {group} group");
    }
    Ok(true)
}

pub fn check() -> Result<()> {
    if !Path::new("/dev/kvm").exists() {
        bail!(
            "KVM device not found (/dev/kvm). \
             Please enable virtualization (VT-x/AMD-v) and load kvm modules."
        );
    }

    if kvm_accessible() {
        info!("/dev/kvm is accessible");
        return Ok(());
    }
    bail!(
        "/dev/kvm is not accessible for read/write; add the runtime user to the kvm group and restart its session"
    )
}
