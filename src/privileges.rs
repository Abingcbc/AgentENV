use std::os::unix::process::CommandExt;
use std::process::Command;

use anyhow::{bail, Context, Result};

pub use linux_cap::{CAP_NET_ADMIN, CAP_SYS_ADMIN};

fn capability_name(capability: i32) -> &'static str {
    match capability {
        CAP_NET_ADMIN => "CAP_NET_ADMIN",
        CAP_SYS_ADMIN => "CAP_SYS_ADMIN",
        _ => "unknown capability",
    }
}

/// Validate the capability contract required by the non-root runtime.
///
/// The inheritable and permitted checks are intentional: privileged helper
/// processes receive only their required capability through the ambient set
/// immediately before `exec`.
pub fn require_runtime_capabilities() -> Result<()> {
    let sets = linux_cap::CapabilitySets::current().context("read process capability sets")?;
    let is_root = nix::unistd::Uid::effective().is_root();
    let missing = [CAP_NET_ADMIN, CAP_SYS_ADMIN]
        .into_iter()
        .filter(|capability| {
            if is_root {
                !sets.effective(*capability).unwrap_or(false)
            } else {
                !sets.is_delegable(*capability).unwrap_or(false)
            }
        })
        .map(capability_name)
        .collect::<Vec<_>>();

    if !missing.is_empty() {
        bail!(
            "AENV runtime is missing required Linux capabilities: {}. Start it with CAP_NET_ADMIN and CAP_SYS_ADMIN in the inheritable, permitted, and effective sets (the installed systemd unit configures this automatically)",
            missing.join(", ")
        );
    }
    Ok(())
}

/// Clear ambient capabilities from the server so ordinary executables do not
/// inherit its network and namespace privileges.
pub fn clear_ambient_capabilities() -> Result<()> {
    linux_cap::clear_ambient_capabilities().context("clear ambient Linux capabilities")
}

/// Configure a child to receive only the listed capabilities across `exec`.
///
/// The parent must already have each capability in its inheritable and
/// permitted sets. The closure uses only async-signal-safe syscalls.
pub fn configure_std_command_capabilities(command: &mut Command, capabilities: &'static [i32]) {
    // SAFETY: the pre-exec closure performs only prctl/capset syscalls and
    // constructs io::Error from errno.
    unsafe {
        command.pre_exec(move || linux_cap::configure_current_process_capabilities(capabilities));
    }
}

pub fn configure_tokio_command_capabilities(
    command: &mut tokio::process::Command,
    capabilities: &'static [i32],
) {
    // SAFETY: the pre-exec closure performs only prctl/capset syscalls and
    // constructs io::Error from errno.
    unsafe {
        command.pre_exec(move || linux_cap::configure_current_process_capabilities(capabilities));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_capability_sets() {
        let status =
            "CapInh:\t0000000000201000\nCapPrm:\t0000000000201000\nCapEff:\t0000000000201000\n";
        let sets = linux_cap::CapabilitySets::from_proc_status(status).unwrap();
        assert!(sets.is_delegable(CAP_NET_ADMIN).unwrap());
        assert!(sets.is_delegable(CAP_SYS_ADMIN).unwrap());
    }

    #[test]
    fn capability_must_be_present_in_all_required_sets() {
        let status =
            "CapInh:\t0000000000201000\nCapPrm:\t0000000000201000\nCapEff:\t0000000000001000\n";
        let sets = linux_cap::CapabilitySets::from_proc_status(status).unwrap();
        assert!(sets.is_delegable(CAP_NET_ADMIN).unwrap());
        assert!(!sets.is_delegable(CAP_SYS_ADMIN).unwrap());
    }
}
