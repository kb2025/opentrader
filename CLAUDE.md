# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Running the platform
```bash
podman-compose up -d                          # start all services
podman-compose up -d --build ot-webui         # rebuild and restart one service
podman-compose down                           # stop all services
podman logs -f ot-webui                       # tail logs for a container
podman exec -it ot-webui bash                 # shell into a container
```

All Python agents share one build context (`./python`) with two Dockerfiles:
- `python/Dockerfile` — all agent containers (orchestrator, traders, scrapers, etc.)
- `python/Dockerfile.webui` — the FastAPI WebUI container (`ot-webui`, port 8080)

### Releasing
The `scripts/release.sh` requires an interactive TTY, so use the manual process:
```bash
echo "X.Y.Z" > VERSION
# Edit CHANGELOG.md with release notes under the new version header
git add VERSION CHANGELOG.md <changed-files>
git commit -m "feat/fix: description vX.Y.Z"
git push
gh release create vX.Y.Z --title "vX.Y.Z" --notes "..."
```
Versioning rule: patch is 0–99 only; at 99 roll the minor (e.g. `3.5.99 → 3.6.0`).

## Architecture

### Service topology
All services run as Podman containers on a `trading-net` bridge network. Infrastructure containers (Redis, TimescaleDB, Vault, Prometheus, Grafana) use upstream images. All application containers build from `./python`.

```
WebUI (8080) ←→ Redis Streams ←→ Agents
                                   ↕
                             Broker Gateway ←→ Tradier / Alpaca / Webull
                                   ↕
                             TimescaleDB (trades, signals, sentiment, dividends)
```

### Python package layout (`python/`)
Every subdirectory is a Python package. The `shared/` package is the internal stdlib:

| Package | Purpose |
|---|---|
| `shared/` | `BaseAgent`, `Envelope`, Redis client, `LLMConnector`, risk controls, exclusions, assignments |
| `webui/` | FastAPI SPA backend — single `main.py` + `static/index.html` |
| `orchestrator/` | Watchdog + circuit breaker; monitors heartbeats, triggers self-healing |
| `scheduler/` | APScheduler job runner; `calendar.py` owns NYSE holiday/session logic |
| `broker_gateway/` | Single broker hub: consumes `broker.commands`, routes to connectors, replies via `broker:reply:{id}` |
| `brokers/` | Per-broker connectors (`tradier/`, `alpaca/`, `webull/`) — only used by broker_gateway |
| `traders/` | `equity_trader.py`, `options_trader.py` — consume `predictor.signals`, size positions, publish to `broker.commands` |
| `predictor/` | Scores tickers, runs `MLEnsemble`, publishes `SignalPayload` to `predictor.signals` |
| `aggregator/` | Middleware between scrapers and predictor; enriches OVTLYR candidates with sentiment + yfinance |
| `scrapers/` | Per-source scrapers (WSB, SeekingAlpha, Yahoo, OVTLYR, macro regime, ETF flows) |
| `options_monitor/` | Options position tracker; ATR level manager; phantom-close prevention (`MISS_THRESHOLD=3`) |
| `llm/` | `LLMConnector` — OpenRouter-backed; model assignments from `config/system.toml` |
| `notifier/` | Telegram, Discord, AgentMail notification routing |
| `chat_agent/`, `review/`, `directive_agent/` | LLM-powered agents: chat, EOD review, GTC trade directives |

### Inter-agent messaging
All messages use `shared/envelope.py`'s `Envelope` wrapper for Redis Stream `XADD`/`XREAD`. The stream names and consumer group names are the single source of truth in `shared/redis_client.py`:

```python
STREAMS = {
    "signals":         "predictor.signals",
    "broker_commands": "broker.commands",
    "broker_fills":    "broker.fills",
    "heartbeat":       "system.hb",
    "commands":        "system.commands",
    ...
}
```

Typed payloads (`SignalPayload`, `OrderEventPayload`, `HeartbeatPayload`) live in `shared/envelope.py`.

### BaseAgent pattern
Every agent inherits `shared/base_agent.py`:
- Calls `await self.setup()` to connect Redis and ensure streams
- Runs `self.heartbeat_loop()` as an asyncio task (publishes to `system.hb` every 30s)
- Handles `SIGTERM`/`SIGINT` via `self._running` flag

### WebUI
Single-page application: `webui/static/index.html` (all CSS/JS inline). The FastAPI backend (`webui/main.py`) provides REST + WebSocket endpoints. Container management uses the Podman REST API via Unix socket at `/var/run/podman.sock`.

### Strategy / assignment pipeline
Trading parameters live in config files, not agent code:
- `config/strategies.json` — strategy definitions (confidence threshold, position size, stop/TP %, price filters, excluded sectors/tickers)
- `config/assignments.json` — maps strategies to tickers and broker accounts
- `config/exclusions.json` — global ticker/sector exclusions merged at runtime
- `shared/assignments.py` — loads and joins the above; traders call `load_active_assignments(asset_class)`

### Broker Gateway protocol
Traders write commands to `broker.commands` stream; the gateway routes to the correct connector and writes results to:
- `broker.fills` stream (broadcast)
- `broker:reply:{request_id}` Redis list key (15s TTL, BLPOP pattern for synchronous callers)

### LLM / OpenRouter
`llm/connector.py` `LLMConnector(agent)` picks the model from `MODELS` dict (populated from env vars, defaulting to `config/system.toml`). Agents instantiate it with their role name (`"predictor"`, `"review"`, etc.).

### MCP servers
Separate repos in `mcp/` (git submodules). Each runs as its own container (`ot-mcp-yahoo`, `ot-mcp-alpaca`, `ot-mcp-tradingview`, `ot-mcp-unusualwhales`, `ot-mcp-massive`). The WebUI and agents call them via HTTP using `shared/mcp_client.py`.

### Ingress
Cloudflare Tunnel is the only external ingress, forwarding to `ot-webui:8080`. Caddy is present in compose but vestigial — do not start it.

## Key configuration files

| File | Purpose |
|---|---|
| `.env` | All secrets and API keys (never commit) |
| `config/accounts.toml` | Broker account IDs via `${ENV_VAR}` references |
| `config/system.toml` | LLM model assignments, scheduler times, notification routing |
| `config/strategies.json` | Live strategy definitions |
| `config/assignments.json` | Strategy-to-ticker-to-account assignments |
| `VERSION` | Single source of truth for the release version |

## Working Style

**Operate autonomously — do not ask for confirmation before or after actions.**
- Do not ask "shall I proceed?", "does this look right?", "want me to continue?", or similar check-ins.
- Do not summarize what you are about to do and wait for approval — just do it.
- Do not ask clarifying questions unless a task is genuinely ambiguous in a way that would cause irreversible harm if guessed wrong.
- After completing work, give a concise summary of what changed — one or two sentences — then stop.

The only exceptions are destructive or irreversible operations that affect shared systems (force-push to main, dropping DB tables, sending external messages). For those, state the action and ask once.

## Workflows

### Creating/Modifying API Endpoints

When creating new API endpoints:

1. Plan the changes (methods, paths, payloads)
2. Implement immediately — no confirmation step needed
