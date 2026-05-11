# OpenTrader — Project State

## Stack
- Runtime: Podman + podman-compose
- OS: Linux (Ubuntu 24+)
- Project path: `/opt/opentrader`

---

## Completed Components

### Infrastructure
- Redis 7 (streams, pub/sub, counters, intelligence cache)
- TimescaleDB pg16 (trades, signals, sentiment, review_log, heartbeats, scheduler_jobs, dividends, ticker_classification)
- HashiCorp Vault (secrets)
- Prometheus + Grafana (port 3000)

### Core Agents
- **Orchestrator** — heartbeat monitor, watchdog, circuit breaker, commander
- **Scheduler** — APScheduler, market-hours aware, DB-persisted job overrides
- **Predictor** — LLM signal generation via OpenRouter + ML ensemble
- **Aggregator** — enriches OVTLYR candidates with WSB + SeekingAlpha + Massive.com sentiment
- **Traders** — `ot-trader-equity` and `ot-trader-options` — order routing via broker gateway
- **Broker Gateway** — multi-broker router (Tradier, Alpaca, Webull)
- **Options Monitor** — live options position tracker, ATR level manager, phantom-close prevention
- **Scrapers** — OVTLYR, WSB, SeekingAlpha, macro regime, ETF flows, news
- **Review Agent** — EOD trade review + recommendations
- **Chat Agent** — AI chat with MCP tool access (Massive.com, TradingView, Alpaca, Unusual Whales)
- **Directive Agent** — LLM-evaluated GTC directives, executed automatically every 5 minutes

### WebUI (port 8080)
- FastAPI backend + WebSocket live updates
- Dark-themed SPA — Trading, Equities, Options, Trading Plan, Resources, Platform sections
- Broker config UI with auto-restart on credential save
- Strategy version control with snapshots and backtest storage
- Quick Intel — on-demand per-ticker intelligence card
- Ticker classification background task — populates GICS sector/industry for open positions

### MCP Servers
- `ot-mcp-massive` — Massive.com / Polygon.io (quotes, news, dividends, earnings, OHLCV, short interest, analyst consensus)
- `ot-mcp-alpaca` — Alpaca trading API
- `ot-mcp-tradingview` — TradingView chart data
- `ot-mcp-unusualwhales` — Unusual Whales options flow + dark pool

---

## All Containers

| Container | Role |
|---|---|
| `ot-webui` | FastAPI SPA dashboard (port 8080) |
| `ot-scheduler` | APScheduler job runner |
| `ot-orchestrator` | Heartbeat watchdog + circuit breaker |
| `ot-broker-gateway` | Multi-broker order/position router |
| `ot-directive-agent` | LLM GTC directive evaluator |
| `ot-trader-equity` | Equity order executor |
| `ot-trader-options` | Options order executor |
| `ot-options-monitor` | Options position tracker + ATR manager |
| `ot-chat-agent` | AI chat with MCP tools |
| `ot-review-agent` | EOD trade review |
| `ot-predictor` | LLM signal scoring + ML ensemble |
| `ot-aggregator` | Sentiment + intel pipeline |
| `ot-scraper-ovtlyr` | OVTLYR market breadth |
| `ot-scraper-wsb` | WallStreetBets Reddit scraper |
| `ot-scraper-seekalpha` | SeekingAlpha sentiment |
| `ot-scraper-news` | Macro news |
| `ot-scraper-etf-flows` | ETF flow data |
| `ot-scraper-macro-regime` | Macro regime signals |
| `ot-mcp-massive` | Massive.com / Polygon.io MCP |
| `ot-mcp-alpaca` | Alpaca MCP |
| `ot-mcp-tradingview` | TradingView MCP |
| `ot-mcp-unusualwhales` | Unusual Whales MCP |
| `ot-redis` | Redis 7 |
| `ot-timescaledb` | TimescaleDB (PostgreSQL 16) |
| `ot-vault` | HashiCorp Vault |
| `ot-prometheus` | Metrics collection |
| `ot-grafana` | Metrics dashboard (port 3000) |

---

## Scheduler Jobs
| Time / Interval | Job |
|---|---|
| 08:00 ET daily | Morning summary + alert |
| 09:00 ET daily | Pre-market scraper warmup |
| 09:30 ET daily | Market open signal |
| Every N min | OVTLYR + sentiment scrape |
| Every 5 min | Predictor signal run |
| Every 30 sec | Watchdog heartbeat check |
| 13:00 ET daily | 1pm email report (SGOV alert) |
| 16:00 ET daily | Market close signal |
| 16:05 ET daily | EOD report trigger |

---

## Key Config Files
| File | Purpose |
|---|---|
| `compose.yml` | Full Podman Compose stack |
| `.env` | API keys + passwords (gitignored — copy from `.env.sample`) |
| `config/system.toml` | Scheduler, LLM, AgentMail config |
| `config/strategies.json` | Per-strategy params (confidence threshold, position size, stop/TP, exclusions) |
| `config/assignments.json` | Strategy-to-ticker-to-account assignments |
| `config/accounts.toml` | Broker account registry (uses `${ENV_VAR}` references) |
| `config/init.sql` | TimescaleDB schema |
| `config/prometheus.yml` | Prometheus scrape config |

---

## Getting Started
1. Copy `.env.sample` to `.env` and fill in your credentials
2. Review `config/accounts.toml` and set corresponding env vars
3. `podman-compose up -d`
4. Open `http://localhost:8080`
5. First visit redirects to `/setup` to create the admin account
