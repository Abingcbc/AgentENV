# Proxy

The reverse proxy lets you reach services running inside a sandbox from outside. It supports HTTP requests, SSE streams, and WebSocket connections.

## Endpoints

- `ANY /proxy` forwards to `/` inside the sandbox
- `ANY /proxy/{path}` forwards to `/{path}` inside the sandbox
- Header-routed requests on otherwise unmatched paths forward the original path
  unchanged. This lets clients use the same base URL for API and sandbox data
  traffic when they send routing headers.
- When sandbox proxy domains are configured, host-based URLs shaped like
  `{port}-{sandboxID}.{domain}` forward the original path without routing
  headers.

Query strings are forwarded unchanged.

## Required Headers

Each proxied request must identify the target sandbox and port:

| Header | Description |
|--------|-------------|
| `x-agentenv-sandbox-id` | Sandbox UUID to route to |
| `x-agentenv-target-port` | Port of the service inside the sandbox |

E2B-compatible aliases are also accepted:

| Header | Alias for |
|--------|-----------|
| `e2b-sandbox-id` | `x-agentenv-sandbox-id` |
| `e2b-sandbox-port` | `x-agentenv-target-port` |

These routing headers are stripped before the request is forwarded to the sandbox.

Host-based proxy requests derive both values from `Host`, for example
`http://8080-<sandbox-uuid>.sandbox.example.com/health` targets port `8080`.
The configured domain must route to the AgentENV server in single-node mode or
to the gateway in multi-node mode. Host-based proxy traffic is always treated as
data-plane traffic; lifecycle and other control-plane APIs should use the base
API host.
