# OverlayBD OCI Conversion Tools

This directory contains the Rust-side conversion helpers for OCI -> overlaybd
layer conversion.

The actual `overlaybd-create`, `overlaybd-apply`, and `overlaybd-commit`
binaries are **not** committed to this repository. AgentENV setup downloads the
configured pinned `containerd/overlaybd` release package at startup and
extracts the tools into:

- `<deps_path>/overlaybd/bin`
- `<deps_path>/overlaybd/lib`

The pinned release tag and asset page URL live in `config/default.toml` under
`[overlaybd]`.

Because AgentENV currently uses the original upstream `overlaybd-apply`
behavior, setup also installs the packaged default config to:

- `/etc/overlaybd/overlaybd.json`

Use `overlaybd::tools::OverlaybdTools::from_overlaybd_install_root(...)` to
point the wrapper at that extracted installation root.
