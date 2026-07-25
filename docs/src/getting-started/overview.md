# Getting Started

AgentENV (abbreviated as AENV) is a self-hosted sandbox runtime for AI agents. It runs isolated Firecracker microVMs and exposes an E2B-compatible HTTP API — so existing E2B SDK code works against it without modification.
The repository is available at <https://github.com/kvcache-ai/AgentENV>.

## Why AgentENV

- **Sub-50 ms sandbox boot, massive concurrency** — Sandboxes boot from a template snapshot in ~49 ms. Sandboxes forked from the same template share a single mmap'd memory device; memory pages are fetched lazily via copy-on-write, so per-sandbox overhead stays low and many sandboxes can run concurrently on a single host.
- **Efficient snapshots** — Snapshot creation completes in ~133 ms. Firecracker diff snapshots capture only dirty pages, and each delta is compressed and stacked as a new overlaybd layer. Snapshots are first-class primitives — pause, fork, and restore any running sandbox at any point.
- **On-demand image pull** — Images are never fully downloaded upfront. The runtime reads blocks directly from the OCI registry on demand, so sandboxes start immediately without waiting for a full image pull.

## Features

- **Firecracker microVMs** with full Linux kernel isolation per sandbox
- **Pause and resume** with memory + disk snapshots for instant cold start
- **Layered block devices** via overlaybd + ublk for copy-on-write image sharing
- **Snapshot-backed template builder** for publishing reusable, pre-configured sandbox runtimes
- **E2B-compatible API** so existing E2B SDKs and CLIs work out of the box
- **Reverse proxy** to reach services running inside sandboxes via HTTP and WebSocket
- **Multi-node scaling** with a gateway + scheduler control plane (prototype)

## Who Is This For

AgentENV is built for teams running AI agents that need isolated execution environments: code interpreters, tool-use agents, autonomous coding agents, or any workload where you want a fresh (or cached) Linux environment per task.

## Interacting with the Server

AgentENV exposes an HTTP API. There are four ways to use it:

| Method | Best for |
|--------|----------|
| **[aenv CLI](./aenv-cli.md)** | Interactive use, scripting, local development |
| **[E2B CLI](../integration/e2b-cli.md)** | E2B-familiar workflows from the terminal |
| **[E2B SDK](../integration/e2b-sdk.md)** | Application code — the existing E2B SDK works against AgentENV without modification |
| **[HTTP API](../api/index.md)** | Direct control, other languages, automation |

## Where to Go Next

- **[Quick Start](./quickstart.md)** — Install the server, run your first sandbox. Takes ~5 minutes on a supported Linux host.
- **[Deployment](../deployment/manual-compile.md)** — Build from source, Docker Compose multi-node, or Kubernetes.
