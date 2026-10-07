# OpenCode IP Rotator & Proxy Server

[![GitHub release](https://img.shields.io/github/v/release/alztrk/opencode-ip-rotator?style=flat-square&color=blue)](https://github.com/alztrk/opencode-ip-rotator)
[![Docker Image](https://img.shields.io/badge/docker-microservices-blue.svg?style=flat-square&logo=docker)](https://github.com/alztrk/opencode-ip-rotator)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=flat-square)](https://opensource.org/licenses/MIT)
[![Python 3.11](https://img.shields.io/badge/python-3.11-brightgreen.svg?style=flat-square&logo=python)](https://python.org)

A microservice-architected 3-tier egress proxy for OpenCode Zen. Every request rides **Tier 1 (Direct Home IP)** → on 429 cools down and escalates to **Tier 2 (Cloudflare WARP / proxy pool with IP rotation)** → when local tiers exhaust, **Tier 3 (Cloud Relay)** serves the request. Quota-category 429s fail fast (no rotation burn). Logs usage metrics into SQLite and displays real-time statistics on a clean web dashboard.

![OpenCode IP Rotator Dashboard Preview](docs/dashboard_preview.jpg)

> **Live-verified 2026-10-07:** Tier 1 request `200 "ok"` via `http://127.0.0.1:8765`; WARP rotation `{"status":"success"}` via rotator `:8001`; relay forward reaches upstream (returns genuine upstream `FreeTierError`/`FreeUsageLimitError`/`ModelError` shapes, proving end-to-end relay transport works).

---

## Key Features

- **3-Tier Automatic Failover**: Direct Home IP → WARP/proxy-pool rotation → cloud relay, with per-tier cooldowns and a live tier-state file (`/tmp/opencode-active-tier.json`) consumed by the `omp` statusline.
- **Quota-Aware 429 Handling**: `FreeUsageLimitError`/`GoUsageLimitError`/`BlackUsageLimitError` fail fast with upstream `Retry-After` preserved — never burns WARP rotations on account-level limits.
- **Verified Shared Egress**: The proxy and WARP service share one network namespace, so the observed egress path is the path used for upstream requests.
- **Microservices Architecture**: Decoupled `proxy-server` (FastAPI) and `warp-rotator` (Cloudflare WARP daemon) services built with Docker Compose.
- **Clean Management Dashboard**: Lightweight Web UI displaying active connections, current location, token statistics, and manual rotation controls.
- **SQLite Data Persistence**: Stores token consumption, model request counts, and historical IP rotation logs on disk.
- **USD Savings Calculator**: Estimates cost savings per model based on prompt and completion token rates.
- **Table Pagination**: Built-in 5-item pagination for model usage and IP rotation log tables.
- **Active Flow Locking**: DB-backed flow leases + in-process counters protect active SSE streams from rotation mid-flight; truncated streams emit an error chunk, never a clean `[DONE]`.
- **Anthropic + Responses API Compatibility**: Native `/v1/messages` (Claude clients, Vercel AI SDK) and `/v1/responses` endpoints, all riding the same 3-tier failover.
- **Custom Proxy Pool Support**: Round-robin outbound proxy pool via `data/proxies.txt` or `PROXY_LIST` environment variable (honored even when the file is absent).
- **Edge Relay Fleet**: Local relay (`server.py` `/`+`/relay`), Deno Deploy relay (`deno-relay.ts`), and authenticated Cloudflare Worker relay (`cf-relay/`, token via `wrangler secret put RELAY_TOKEN` — never committed).

---

## Architecture Overview

```
[OpenCode Client / omp+pi-bansos] ──> [Proxy Server (:8765 native / :8000 docker)]
                                                │ ① Tier 1: Direct Home IP (None proxy)
                                                │ ② Tier 2: WARP SOCKS5 :40000 / proxy pool
                                                │     (429 → rotate via Rotator :8001, cooldown direct 300s)
                                                │ ③ Tier 3: Cloud Relay (FALLBACK_RELAY_URL)
                                                │     shared helper attempt_cloud_relay_fallback()
                                                ▼
                                       [OpenCode Zen API Endpoint]
                                       https://opencode.ai/zen/v1

                    [WARP Rotator (:8001)] ──> [Cloudflare WARP Daemon]
                      POST /rotate · GET /status · GET /health
                      (owns shared netns; SQLite flow-lease guard)

                    [Edge Relays] ──x-relay-target/x-relay-path──> upstream
                      local (server.py) · deno-relay.ts · cf-relay worker
```

### Tier state contract

`server.py` publishes the live egress tier after every success to `/tmp/opencode-active-tier.json`:
```json
{"tier": "direct", "label": "direct (home)", "direct_available": true, "timestamp": 1791386366.88}
```
Labels: `direct (home)` · `warp` · `proxy_pool`/`custom_proxy` (actual name) · `render`. The `omp` statusline (`pi-statusline.ts`) reads this file and right-aligns `relay: <label>` on the `& think:` line, only when the `bansos` provider is active.

### Core Architecture Components

1. **Proxy Server (`server.py`):** OpenAI/Anthropic/Responses-compatible API proxy (native `http://127.0.0.1:8765`, Docker `:8000`). 3-tier retry loop per endpoint (`/v1/chat/completions`, `/v1/messages`, `/v1/responses`, `/`+`/relay`), SSE streaming with truncation detection, Web Management Dashboard.
2. **Rotator Module (`rotator.py`):** Owns the WARP network namespace; exposes `POST /rotate`, `GET /status`, `GET /health` on `:8001`. Guards rotation against active flow leases (SQLite `active_flow_leases`, TTL 90s).
3. **Edge Relays:** `deno-relay.ts` (Deno Deploy, open) and `cf-relay/` (Cloudflare Worker, `RELAY_TOKEN`-gated) forward `x-relay-target`/`x-relay-path` to `https://opencode.ai`, stripping hop-by-hop headers and never forwarding the relay credential upstream.
4. **Container Manager (`manager.py`):** Ephemeral container lifecycle management (self-destruction/re-creation past a rotation threshold for fresh hardware identifiers).
---

## Detailed Features

- **OpenAI Standard Compatibility:** Fully exposes `/v1/chat/completions` and `/v1/models` endpoints to integrate with standard clients.
- **Dynamic Header Forwarding:** Captures and forwards all incoming client metadata including `x-opencode-*` headers and injects `Authorization: Bearer public` credentials required by the upstream API.
- **Verified Public IP Rotation:** Validates public IP changes via external IP lookup services to guarantee a distinct IP allocation after every disconnection cycle.
- **Active Flow Locking:** Prevents IP rotations during active Server-Sent Events (SSE) streaming sessions to prevent connection truncation and stream drops.
- **Dynamic Model Auto-Discovery:** Periodically queries the upstream API to discover newly available free models without requiring code changes or static lists.
- **Web Management Dashboard:** Includes a clean, dark-themed web interface accessible at `http://127.0.0.1:8000/dashboard` for monitoring public IP status, active connections, total rotations, and triggering manual rotations.

---

## Quick Automated Setup

Run the automated installer script to install Python dependencies, verify system requirements, and automatically configure `~/.config/opencode/opencode.jsonc`:

```bash
python setup.py
```

---

## Installation & Deployment

### Option 1: Docker Container Deployment (Recommended)

Running the project in Docker isolates the execution environment, preventing local network configuration changes and ensuring a new environment identity (`/etc/machine-id`) on every initialization.

#### Prerequisites
- Docker Engine 20.10+
- Docker Compose v2+

#### Build and Launch
```bash
docker compose up -d --build
```

#### Access Web Dashboard
Open your browser and navigate to:
`http://127.0.0.1:8000/dashboard`

#### Environment Re-creation
To manually trigger environment self-destruction and re-create a container with fresh hardware identifiers:
```bash
python manager.py
```

---

### Option 2: Local Native Execution

#### Prerequisites
- Python 3.8 or higher
- Cloudflare WARP CLI (`warp-cli`) installed and added to system PATH
- Administrative privileges (required for `warp-cli` operations on Windows)

#### Steps

1. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Start the rotator background service:
   ```bash
   python rotator.py
   ```

3. Launch the proxy server:
   ```bash
   python server.py
   ```

---

## Configuration

### OpenAI-Compatible Provider (Default)

To use the local proxy server within OpenCode, update your configuration file at `~/.config/opencode/opencode.jsonc`:

```jsonc
{
  "provider": {
    "opencode-zen-local": {
      "npm": "@ai-sdk/openai-compatible",
      "options": {
        "baseURL": "http://127.0.0.1:8000/v1",
        "apiKey": "any"
      },
      "name": "OpenCode Zen Local Proxy"
    }
  }
}
```

> **Note:** Models are automatically discovered from the proxy server's `/v1/models` endpoint. You do not need to hardcode model names manually.

---

### Anthropic API Provider

The proxy exposes a native Anthropic-compatible `/v1/messages` endpoint. To use it with OpenCode's Anthropic provider:

```jsonc
{
  "provider": {
    "my-anthropic-proxy": {
      "npm": "@ai-sdk/anthropic",
      "options": {
        "baseURL": "http://127.0.0.1:8000",
        "apiKey": "any"
      }
    }
  }
}
```

> Requests sent to `/v1/messages` are translated to OpenAI format internally and routed through the same WARP-protected upstream.

---

### Custom Outbound Proxy Pool

If you want to use your own HTTP/SOCKS5 proxies instead of (or in addition to) Cloudflare WARP:

**Option 1 — File:** Create `data/proxies.txt` with one proxy per line:
```
http://user:pass@proxy1.example.com:8080
socks5://proxy2.example.com:1080
```

**Option 2 — Environment variable:**
```bash
PROXY_LIST="http://proxy1:8080,socks5://proxy2:1080" docker compose up -d
```

The proxy pool rotates in round-robin order across all outbound requests.

### Third-Party Agent & Harness Integration (`pi-bansos` / `omp` / `pi`)

If you use third-party coding agent harnesses such as **[omp](https://github.com/earendil-works/omp)** or **[pi](https://pi.dev)** with the `pi-bansos` extension, the proxy provides a native pass-through relay endpoint supporting the `x-relay-target` / `x-relay-path` header specification.

Requests forwarded from the harness retain full client fingerprinting and session affinity, while routing through Cloudflare WARP and automatically rotating egress IPs whenever HTTP 429 rate limits occur.

#### Setup via `pi-bansos-relay-state.json`

Add or update your agent state file (`~/.omp/agent/pi-bansos-relay-state.json` or `~/.pi/agent/pi-bansos-relay-state.json`):

```json
{
  "enabled": true,
  "url": "http://127.0.0.1:8000",
  "relays": [
    {
      "url": "http://127.0.0.1:8000",
      "label": "Local WARP Rotator"
    }
  ],
  "statusBar": "shown"
}
```

Alternatively, configure it live inside the TUI without restarts:
```text
/bansos url http://127.0.0.1:8000
/bansos on
```
---

## API Endpoints Reference

| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/v1/chat/completions` | `POST` | OpenAI-compatible chat completion endpoint with automatic retry and IP rotation. |
| `/v1/messages` | `POST` | Anthropic-compatible endpoint (`/v1/messages`) for Claude clients and `@ai-sdk/anthropic`. |
| `/v1/models` | `GET` | Returns list of currently discovered active free models. |
| `/dashboard` | `GET` | Renders the HTML Web Management Dashboard. |
| `/metrics` | `GET` | Returns structured JSON metrics including verified IP, uptime, and request counters. |
| `/api/rotate` | `POST` | Triggers an immediate manual IP rotation cycle. |
| `/` or `/relay` | `POST` / `GET` | Pass-through relay for harnesses using `x-relay-target` and `x-relay-path` headers. |

---

## Technical Specifications & Environment Variables

| Variable | Default Value | Description |
| :--- | :--- | :--- |
| `OPENCODE_ZEN_PORT` | `8000` | Local port for the proxy server. |
| `OPENCODE_ZEN_HOST` | `127.0.0.1` | Host address for binding the server (`0.0.0.0` in Docker). |
| `WARP_CHECK_INTERVAL` | `15` | Health check interval in seconds. |
| `WARP_ROTATION_INTERVAL` | `300` | Periodic IP rotation interval in seconds. |
| `AUTO_RECYCLE_THRESHOLD` | `50` | Maximum rotations before triggering container environment refresh. |
| `CORS_ALLOW_ORIGINS` | `http://127.0.0.1:8000,http://localhost:8000` | Comma-separated browser origins allowed to call the proxy. |
| `WARP_ROTATOR_URL` | `http://127.0.0.1:8001` | Internal rotator endpoint. Do not expose port 8001 publicly. |
| `FALLBACK_RELAY_URL` | *(unset = Tier 3 disabled)* | Your own cloud relay URL (e.g. `https://your-relay.workers.dev`); full prompts route through it on local-tier exhaustion. |
| `DIRECT_COOLDOWN_SECONDS` | `300` | Cooldown for Tier 1 after a 429 before direct is retried. |
| `RELAY_TOKEN` (cf-relay only) | *(unset = open dev mode)* | Set via `wrangler secret put RELAY_TOKEN`; NEVER in `wrangler.toml`. Production MUST set it. |

### Rate-limit behavior

The proxy classifies every upstream `429` (`rate_limits.py: classify_upstream_429`) and preserves `Retry-After`:

| Category | Trigger types | Behavior |
| :--- | :--- | :--- |
| `quota` | `FreeUsageLimitError`, `GoUsageLimitError`, `BlackUsageLimitError` | Fail fast: return 429 immediately. No WARP rotation, no relay escalation — rotation cannot fix account limits. |
| `rate_limit` | `RateLimitError` | Tier escalation: direct → cooldown → WARP rotation → cloud relay. |
| `upstream_rate_limit` | anything else | Same escalation as `rate_limit`. |

HTTP `408` is retried like `5xx`. Truncated SSE streams terminate with an error chunk, never a clean `[DONE]`.

---

## Responsibility Disclaimer

This project is intended for educational, research, and infrastructure resilience testing purposes. Users are responsible for ensuring their usage complies with applicable terms of service and acceptable use policies of third-party service providers. The maintainers assume no liability for account suspensions, service interruptions, or misuse.

---

## License

This software is released under the [MIT License](LICENSE).
