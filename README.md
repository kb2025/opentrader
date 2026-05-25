> ⚠️ **OpenTrader can lose real money.** Read the [Risk Disclosure](./RISK_DISCLOSURE.md)
> and [Terms of Use](./TERMS.md) before connecting to a live account. By using this
> software, you accept full responsibility for all trades it executes.

# OpenTrader

An AI-driven algorithmic trading platform built on a microservices architecture using Podman, Redis, and TimescaleDB. Supports multiple brokers (Tradier, Alpaca, Webull) with a real-time web dashboard, LLM-powered signals, automated trade execution, and real backtesting.

![Dashboard](artwork/opentrader-dashboard.png)

[![Release](https://img.shields.io/github/v/release/euriska/opentrader)](https://github.com/euriska/opentrader/releases)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

---

## Features

### Platform & Infrastructure
- **Multi-broker support** — Tradier, Alpaca, and Webull (paper + live accounts); multi-account within each broker
- **AI-powered signals** — LLM predictor via OpenRouter (Claude, GPT-4o, and more); model assignments configurable per agent in `config/system.toml`
- **Real-time WebUI** — Dark-themed SPA dashboard with live WebSocket updates; version broadcast on every update cycle
- **Secure login system** — Username/password auth with PBKDF2-SHA256 hashing and HMAC-SHA256 JWT session cookies; first-run `/setup` page creates the admin account; all routes protected by auth middleware
- **Encrypted secret storage** — API keys stored encrypted in DB (AES-128-CBC + HMAC-SHA256 via Fernet keyed from `SECRET_KEY`); never returned to the browser; managed via My Profile
- **Self-healing** — Orchestrator watchdog with circuit breaker and auto-restart; all agents publish heartbeats to `system.hb` every 30 s
- **Scheduler** — Market-hours-aware APScheduler job runner; DB-persisted jobs with per-job execution history (last run, status chip, error count, run count); CronTrigger path for daily reports
- **Notifications** — Telegram, Discord, and AgentMail alerts for fills, system events, circuit breaker trips, and EOD summaries

### Trading Dashboard
- **Stat cards** — Live equity/options position counts, 30-day trade counts, unrealized P&L, win rate, long/short exposure, SPY/QQQ OVTLYR breadth signals
- **Market Breadth** — OVTLYR bull/bear gauge with crossover detection, sparkline history, bull/bear count breakdown
- **Macro Regime card** — Current macro regime classification (expansion / slowdown / contraction / recovery) from `ot-scraper-macro-regime` with FRED overlay: HY OAS, IG OAS, Financial Stress Index (FSI), NBER recession signal; 2s10s yield spread bar chart; TIPS real yield trendline
- **Portfolio NAV History** — 90-day equity curve from daily broker snapshots with drawdown tracking
- **Daily P&L / Loss Limit** — Color-coded budget bar with circuit breaker banner; timezone-anchored to US Eastern; scanner-induced false option closures excluded
- **ETF Capital Flows** — Sector-level ETF flow ratio table with anomaly detection (z-score > 2.0 threshold); category filter
- **Predictor Signals** — Live ML ensemble signal stream with confidence bars
- **Trending Symbols** — Volume-anomaly ranked ticker list
- **Unified Market News** — Combined Alpha Vantage sentiment feed and Massive.com macro news; source badges (AV / MKT); category filter; newest-first

### Options Dashboard
- **Live positions table** — DTE, strike, delta, ATR levels (Emergency Exit / Exit Alert / Roll 1–9), underlying price, buy/sell signal; phantom-close prevention (`MISS_THRESHOLD=3` consecutive-miss Redis counter)
- **Portfolio Greeks panel** — Δ/Θ/ν/Γ summed across all active positions per underlying; VaR panel: delta-gamma VaR 95%, CVaR 95%, Max Loss, Portfolio PoP, Θ/day
- **Unusual Options Flow panel** — Real-time incremental volume delta tracking across all active underlyings via Polygon snapshots; per-contract importance score (0.40 × notional + 0.30 × vol delta + 0.20 × type weight + 0.10 × direction confidence); BULL/BEAR direction badge from 3-method inference (greeks.delta → day.change_percent → call/put heuristic); top 25 hits refreshed each scan cycle
- **YTD Performance panel** — P&L timeline and statistics across all option positions
- **Expiry Calendar** — Positions grouped by expiration with DTE urgency color coding (≤3 / ≤7 / ≤14 days) and per-expiry Greeks totals
- **OI Wall Detector** — Polygon-powered open interest wall collapse detection; confidence = 0.40 × OI size + 0.35 × drop speed + 0.25 × proximity; alerts to Redis + signals DB
- **Early Assignment Risk** — Flags call positions when ex-dividend date falls before expiration (≤10 days)
- **Stat card hover tooltips** — Active alerts, expiring positions (≤7 days)
- **Download + scheduled email report** — 1pm daily HTML report with SGOV ex-dividend SELL/BUY banners; IRA account detection

### Strategy Payoff Builder
- **14 strategies** — Long Call/Put, Covered Call, Cash-Secured Put, Long/Short Straddle, Long/Short Strangle, Bull Call Spread, Bear Put Spread, Bull Put Spread (credit), Bear Call Spread (credit), Long Butterfly, Iron Condor
- **Non-equidistant butterfly lot sizing** — GCD-based weight computation (n1 = upperWidth/GCD, n3 = lowerWidth/GCD, n2 = n1+n3); stats bar shows "Lots: ×n1 / ×n2 / ×n3"
- **Expiry P&L diagram** — SVG payoff curve with profit/loss zone fills, break-even markers, max P&L horizon lines, spot price indicator
- **Time-slice B-S curves** — Cyan dashed "Today" + amber dashed "50% DTE" Black-Scholes theoretical value overlays; y-axis auto-scales across all three curves
- **Newton-Raphson IV solver** — Per-leg implied volatility from market price; converges in ~10 iterations
- **Greeks surface heatmap** — 2D color table (spot ±30% × DTE) for Δ/Γ/ν/Θ; select-dropdown for instant client-side re-render; current spot column highlighted
- **Greeks sensitivity sparklines** — 2×2 grid of Δ/Γ/ν/Θ vs. spot curves (50-point resolution); zero-crossing and spot marker per chart
- **Per-leg price + Greeks table** — B-S theoretical value, net dollar P&L per contract, Δ/Γ/ν/Θ per leg at current spot
- **PoP** — Gaussian quadrature lognormal integration (400 points) for risk-neutral probability of profit

### Options Trader
- **Account selector + open positions** panel with DTE, OVTLYR buy-signal list
- **LightweightCharts candlestick** with EMA 10/20/50 + earnings and ex-dividend markers
- **Live broker options chain** — Tradier → Webull → Alpaca fallback chain; extrinsic value, IV, greeks, blue position highlighting
- **Multi-leg order builder** — Construct spreads, straddles, and complex legs from the chain
- **Risk & Sizing Calculator** — Per-account default risk %, deviation warning, max loss preview

### Options Trading Log
- **P&L tree** — Broker → Account → Ticker with milestone chains (Open → Roll → Closed/Expired)
- **Per-event P&L** — Entry/exit prices, realized vs unrealized breakdown
- **Post-close AI analysis** — Claude Haiku trade review on demand
- **CHAIN RISK bar** — Roll-chain risk indicator; 80Δ highlight; spread/extrinsic filters
- **18-month retention** — Full history with YTD performance panel

### Market Intelligence
- **Pipeline** — Per-ticker: WSB mention count + sentiment, SeekingAlpha analysis, Massive.com news, analyst ratings, earnings proximity, Unusual Whales options flow + dark pool data; cached in Redis `aggregator:intel:{ticker}`
- **Quick Intel card** — On-demand per-ticker intelligence with all sources in one panel
- **Unusual Whales MCP** — Options flow (bullish/bearish net premium), dark pool prints, market tide, Greek exposure, short interest

### Equity Features
- **Equity Dashboard** — Live positions across all broker accounts with heatmaps and P&L
- **Equity Trades** — Fills and open orders grouped by week, per-account tally, friendly reject reasons
- **Equity Dividend Income** — Per-broker filter, rolling 12-month actual vs projected bar chart, upcoming ex-dividend panel (7-day), received history; income projection from actual payment history only
- **Ticker Classification** — GICS sector + industry stored in `ticker_classification` DB table (30-day TTL); synced to Redis for exclusion system

### Strategy & Backtesting
- **Strategy Engineer** — AI-assisted builder with version control and Backtrader backtesting; pulls live TradingView data
- **Backtrader Engine** — EMA 10/21 crossover; configurable stop-loss/take-profit; full trade log; PDF + CSV exports; equity curve and indicator charts
- **Strategy Library** — All saved versions with backtest results
- **Strategy Assignment** — Assign strategies to tickers for live execution
- **Portfolio Optimizer** — Risk-weighted allocation across active strategies

### Trade Execution & Directives
- **Trade Directives** — Natural-language GTC directives; LLM evaluation every 5 min; automatic order execution
- **Broker Gateway** — Single hub routing `broker.commands` stream to Tradier/Alpaca/Webull connectors; replies via `broker:reply:{id}` Redis list (15 s TTL)

### Macro Hub
- **FRED integration** — HY OAS, IG OAS, Financial Stress Index, NBER recession signal; 2s10s spread; TIPS real yields
- **ETF flow anomalies** — z-score based sector rotation detection

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          WebUI  (port 8080)                              │
│              FastAPI + WebSocket + Static SPA (index.html)               │
│            Username/password auth · JWT session cookies (HMAC-SHA256)    │
└──────────────────┬───────────────────────────────────────────────────────┘
                   │  Redis Streams  /  Pub-Sub
     ┌─────────────┼────────────────────────────────┐
     │             │                                │
┌────▼─────┐  ┌────▼──────┐  ┌──────────────────────▼───────────────────┐
│Scheduler │  │Orchestrat.│  │              Broker Gateway               │
│APSchedul │  │Watchdog + │  │  Tradier · Alpaca · Webull connectors     │
│DB-persist│  │Circuit Bkr│  │  broker.commands → broker:reply:{id}      │
└──────────┘  └───────────┘  └──────────────────────────────────────────┘
     │
     │ schedules / triggers
     ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  Scrapers (scheduler-triggered)                                          │
│  OVTLYR · WSB · SeekingAlpha · AV News · ETF Flows · Macro Regime       │
└────────────────────────┬─────────────────────────────────────────────────┘
                         │  market.ticks / scanner.signals
                         ▼
┌───────────────────┐         ┌─────────────────────────────────────────┐
│   Aggregator      │────────▶│              Predictor                  │
│  Sentiment + UW   │  intel  │  LLM scoring + ML Ensemble              │
│  intel pipeline   │         │  SignalPayload → predictor.signals      │
└───────────────────┘         └────────────┬────────────────────────────┘
                                           │ signals
                              ┌────────────┼──────────────┐
                              ▼            ▼              ▼
                    ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
                    │Equity Trader │  │Options Trader│  │ Directive    │
                    │              │  │              │  │ Agent (GTC)  │
                    └──────┬───────┘  └──────┬───────┘  └──────┬───────┘
                           └─────────────────┴──────────────────┘
                                             │ broker.commands
                                             ▼
                                    Broker Gateway ──▶ Broker APIs

MCP Layer (passive HTTP servers, queried by agents):
  ot-mcp-massive        — Polygon.io quotes, news, dividends, earnings, options chain
  ot-mcp-tradingview    — Candlestick OHLCV + indicator data (TV scrape)
  ot-mcp-unusualwhales  — Options flow, dark pool, market tide, Greek exposure
  ot-mcp-alpaca         — Alpaca account data + order management
  ot-mcp-eodhd          — Fundamentals, analyst ratings, insider txns, earnings, macro

Review / Monitoring:
  ot-review-agent       — EOD trade analysis and recommendations
  ot-options-monitor    — ATR level manager; OI wall detector; unusual flow scanner
  ot-chat-agent         — AI chat with full MCP tool access

Storage:
  ot-redis              — Streams, pub/sub, intel cache, job state
  ot-timescaledb        — trades, signals, sentiment, dividends, scheduler_jobs,
                          option_positions, option_trade_log, etf_flow_snapshots,
                          report_config, ticker_classification
  ot-vault              — HashiCorp Vault (optional secrets backend)
  ot-prometheus         — Metrics scrape
  ot-grafana            — Metrics dashboard (port 3000)
```

### Signal Flow Detail

```
OVTLYR scraper ──────────────────────────────────────────────▶ predictor.signals
WSB scraper ──────────────────────────────────────────────────▶ aggregator
SeekingAlpha scraper ─────────────────────────────────────────▶ aggregator
AV News scraper ──────────────────────────────────────────────▶ aggregator
ETF Flows scraper ─────────────────────────────────────────────▶ DB (etf_flow_snapshots)
Macro Regime scraper ──────────────────────────────────────────▶ DB (signals) + Redis cache

Aggregator enriches with:
  Unusual Whales MCP  (options flow, dark pool)
  Massive MCP         (news, analyst ratings, short interest)
  EODHD MCP           (fundamentals, earnings, insider)
  → publishes enriched intel to Redis aggregator:intel:{ticker}

Predictor reads:
  aggregator intel + Polygon chain + OVTLYR signal
  → LLM scoring + ML Ensemble (XGBoost/LightGBM)
  → SignalPayload → predictor.signals stream

Equity/Options Traders consume predictor.signals:
  → size positions (risk controls, exclusions, assignments)
  → write broker.commands stream

Broker Gateway:
  → routes to Tradier / Alpaca / Webull connector
  → reply to broker:reply:{request_id} (15 s TTL)

Options Monitor (every 5 min):
  1. Fetch positions from all brokers
  2. Compute ATR levels (TradingView MCP) and save to option_positions DB
  3. Scan OI walls (Polygon) — collapse detection → alerts + signals DB
  4. Scan unusual flow (Polygon vol delta) — importance score → options:flow:latest Redis
```

---

## Services

| Container | Description | Port |
|---|---|---|
| `ot-webui` | Command Center dashboard (FastAPI + SPA) | 8080 |
| `ot-scheduler` | APScheduler job runner with DB-persisted jobs | — |
| `ot-orchestrator` | Heartbeat watchdog + circuit breaker | — |
| `ot-broker-gateway` | Multi-broker position/order router (Tradier/Alpaca/Webull) | — |
| `ot-directive-agent` | LLM-evaluated GTC trade directives | — |
| `ot-trader-equity` | Equity order executor | — |
| `ot-trader-options` | Options order executor | — |
| `ot-options-monitor` | Options position tracker; ATR levels; OI wall + unusual flow scanner | — |
| `ot-chat-agent` | AI chat with full MCP tool access | — |
| `ot-review-agent` | EOD trade review and recommendations | — |
| `ot-predictor` | LLM signal scoring + ML ensemble (XGBoost/LightGBM) | — |
| `ot-aggregator` | Sentiment + intel enrichment pipeline | — |
| `ot-scraper-ovtlyr` | OVTLYR market breadth scraper | — |
| `ot-scraper-wsb` | WallStreetBets Reddit sentiment scraper | — |
| `ot-scraper-seekalpha` | SeekingAlpha professional sentiment scraper | — |
| `ot-scraper-news` | Alpha Vantage macro news scraper | — |
| `ot-scraper-etf-flows` | ETF sector flow ratio scraper | — |
| `ot-scraper-macro-regime` | Macro regime classifier (FRED + market data) | — |
| `ot-mcp-alpaca` | Alpaca MCP server (account data + orders) | — |
| `ot-mcp-tradingview` | TradingView MCP server (OHLCV + indicators) | — |
| `ot-mcp-unusualwhales` | Unusual Whales MCP server (flow, dark pool, market tide) | — |
| `ot-mcp-massive` | Massive.com / Polygon.io MCP server (quotes, news, dividends, earnings, chain) | — |
| `ot-mcp-eodhd` | EODHD MCP server (fundamentals, analyst ratings, insider txns, earnings, macro) | — |
| `ot-redis` | Redis 7 (streams, pub/sub, intel cache, job state) | — |
| `ot-timescaledb` | TimescaleDB / PostgreSQL 16 (all persistent data) | — |
| `ot-vault` | HashiCorp Vault (optional secrets backend) | — |
| `ot-prometheus` | Metrics collection | — |
| `ot-grafana` | Metrics dashboard | 3000 |

---

## Quick Start

### Prerequisites
- Podman 4.0+ and podman-compose 1.0+
- Linux (tested on Ubuntu 24+)

### Install from source

```bash
git clone https://github.com/euriska/opentrader.git
cd opentrader
git submodule update --init --recursive

# Configure credentials
cp .env.sample .env
nano .env  # fill in your API keys and set SECRET_KEY

# Configure broker accounts
cp config/accounts.toml.sample config/accounts.toml
# accounts.toml uses ${ENV_VAR} references — set vars in .env

# Build and start
podman-compose up -d

# Open dashboard — you'll be redirected to /setup on first run
open http://localhost:8080
```

The first visit redirects to `/setup` where you create the admin username and password. After that, `/login` is the entry point. API keys are managed in **Platform → My Profile** and stored encrypted in the database.

### Install from pre-built images

Pre-built container images are published to GitHub Container Registry on every release.

```bash
git clone https://github.com/euriska/opentrader.git
cd opentrader
git submodule update --init --recursive

cp .env.sample .env && nano .env
cp config/accounts.toml.sample config/accounts.toml

export OT_VERSION=3.9.2
for img in ot-webui ot-python ot-mcp-tradingview ot-mcp-unusualwhales; do
  podman pull ghcr.io/euriska/${img}:${OT_VERSION}
done

podman-compose up -d
```

---

## Releasing

Releases use semantic versioning (`MAJOR.MINOR.PATCH`). Patch resets at 99 (e.g. `3.5.99 → 3.6.0`). The `VERSION` file is the single source of truth.

```bash
echo "X.Y.Z" > VERSION
# Edit CHANGELOG.md with release notes
git add VERSION CHANGELOG.md <changed-files>
git commit -m "feat/fix: description vX.Y.Z"
git push
gh release create vX.Y.Z --title "vX.Y.Z" --notes "Release notes here"
```

---

## Configuration

### `.env` — Required keys

| Variable | Description |
|---|---|
| `SECRET_KEY` | 32-byte hex key for JWT signing and API key encryption — generate with `openssl rand -hex 32`; random if unset (sessions invalidated on restart) |
| `OPENROUTER_API_KEY` | LLM provider — get at openrouter.ai |
| `DB_PASSWORD` | TimescaleDB password |
| `MASSIVE_API_KEY` | Polygon.io API key (quotes, news, dividends, earnings, market bars, options chains, snapshots) |
| `TRADIER_SANDBOX_API_KEY` | Tradier paper trading key |
| `TRADIER_PRODUCTION_API_KEY` | Tradier live trading key |
| `ALPACA_API_KEY` | Alpaca paper API key |
| `ALPACA_API_SECRET` | Alpaca paper API secret |
| `ALPACA_LIVE_API_KEY` | Alpaca live API key |
| `ALPACA_LIVE_API_SECRET` | Alpaca live API secret |
| `WEBULL_API_KEY` | Webull API key |
| `WEBULL_SECRET_KEY` | Webull secret key |
| `UNUSUAL_WHALES_API_KEY` | Unusual Whales API key (options flow + dark pool) |
| `FRED_API_KEY` | FRED API key — free at fred.stlouisfed.org; powers HY OAS, IG OAS, FSI, recession signal |
| `EODHD_API_KEY` | EODHD API key — fundamentals, analyst ratings, insider txns, earnings, dividends, macro |
| `TELEGRAM_BOT_TOKEN` | Telegram bot token (optional) |
| `DISCORD_WEBHOOK_URL` | Discord webhook (optional) |
| `AGENTMAIL_API_KEY` | AgentMail key for email reports (optional) |
| `OVTLYR_EMAIL` / `OVTLYR_PASSWORD` | OVTLYR credentials (optional) |

> **Note:** `SECRET_KEY` must be stable across restarts — if unset, a random key is generated each time, invalidating all session cookies. Set it once with `echo "SECRET_KEY=$(openssl rand -hex 32)" >> .env`.

See `.env.sample` for the full list.

### Broker accounts — `config/accounts.toml`

Copy from `config/accounts.toml.sample`. All account IDs reference `${ENV_VAR}` so no credentials are stored in the file itself. Additional API keys (broker tokens, notification webhooks) can also be managed through **Platform → My Profile** in the dashboard, where they are stored encrypted in the database.

---

## WebUI Navigation

The dashboard is organized into six sections:

### Trading
| Page | Description |
|---|---|
| Trading Dashboard | Live stat cards (equity/options split), market breadth gauge, NAV history chart, macro regime card, daily P&L/loss limit, ETF flows, predictor signals, unified news |
| Trade Directives | Natural-language GTC directives with LLM evaluation and order execution |
| Charts | Candlestick charts with indicator overlays, live position picker (equity + options), sentiment sub-panel (F&G + 30-day sparkline) |
| Price Alerts | Per-ticker price alert management |

### Equities
| Page | Description |
|---|---|
| Equity Trades | Fills and open orders grouped by week, per-account tally, friendly reject reasons |
| Equity Dashboard | Live equity positions across all broker accounts with heatmaps, P&L, and liquidate action |
| Equity Dividend Income | Dividend tracking with per-broker filter, actual-vs-projected bar chart, upcoming events, received history |

### Options
| Page | Description |
|---|---|
| Options Dashboard | Live positions with DTE/strike/delta/ATR levels; Portfolio Greeks + VaR panel; Unusual Flow panel; Expiry Calendar; YTD Performance |
| Options Trader | Full trading dashboard — account selector, positions, OVTLYR signals, EMA chart, live broker chain, multi-leg order builder, risk calculator |
| Options Trading Log | Full P&L tree (broker → account → ticker) with milestone chains, AI post-close analysis, YTD performance |

### Trading Plan
| Page | Description |
|---|---|
| Strategy Engineer | AI-assisted strategy builder with version history and Backtrader backtesting |
| Strategy Library | All saved strategy versions with backtest results |
| Strategy Assignment | Assign strategies to tickers for live execution |
| Portfolio Optimizer | Risk-weighted allocation optimizer |

### Resources
| Page | Description |
|---|---|
| Library | Trading book library with ISBN lookup, cover art, star ratings, and reader rank achievement system |
| Backtester | Standalone Backtrader backtest runner |
| Macro Hub | FRED macro indicators, yield curve, ETF flow anomalies, market breadth deep-dive |

### Platform
| Page | Description |
|---|---|
| Platform Dashboard | Agent health stat cards, live topology diagram (draggable/auto-arrange/SVG export), recent events, signal timeline |
| Agents | Per-container health tile/list view with status indicators and log viewer |
| Configuration | Connector credentials (22 API keys with set/unset status), sector/stock exclusions, risk controls |
| Logs | Live container log viewer — all 22 agent/MCP/scraper containers selectable, auto-refresh |
| Scheduler | Job manager — create, edit, enable/disable, run now; per-job execution history |
| System | Circuit breaker, halt/resume trading, container status table |
| My Profile | Avatar, password change, full API key management |

---

## Backtesting

The Strategy Engineer includes a real **Backtrader** backtesting engine:

- **EMA 10/21 crossover** strategy with configurable stop-loss and take-profit
- **Benchmark ticker** saved per strategy (default: SPY)
- **Full trade log** — entry/exit dates, prices, qty, P&L, exit reason
- **Exports** — PDF and CSV trade reports available from the Trades tab
- **Charts** — price + EMA lines with trade markers, volume panel, equity curve
- **Version-linked** — each strategy version stores its own backtest results for comparison

---

## Market Intelligence Pipeline

```
OVTLYR   → scanner.signals stream → Predictor (breadth signal)
WSB      → market.ticks → Aggregator
SeekingAlpha → market.ticks → Aggregator
AV News  → market.ticks → Aggregator

Aggregator enriches each candidate:
  ├── Unusual Whales MCP    — options flow net premium, dark pool prints
  ├── Massive MCP           — Polygon news, analyst consensus, short interest
  └── EODHD MCP             — fundamentals, earnings calendar, insider transactions
  → writes aggregator:intel:{ticker} Redis hash (used by Predictor)

Predictor:
  ├── reads aggregator:intel:{ticker}
  ├── runs LLM scoring (OpenRouter)
  └── runs ML Ensemble (XGBoost / LightGBM)
  → publishes SignalPayload to predictor.signals stream

Traders consume predictor.signals:
  → apply risk controls, exclusions, strategy rules
  → write broker.commands
```

Intelligence is cached in Redis (`aggregator:intel:{ticker}`) and adjusts predictor confidence by up to ±0.20.

---

## Options Monitor Details

The `ot-options-monitor` agent runs every 5 minutes and performs four tasks:

1. **Position scan** — fetches all open option positions from all brokers; computes ATR-based levels (Emergency Exit −3 ATR, Exit Alert −2 ATR, Roll 1–9 at +0.5 to +9 ATR); checks early assignment risk (call positions near ex-dividend); persists to `option_positions` and `option_trade_log` DB tables

2. **Phantom-close prevention** — uses Redis `options:miss:{id}` counter; position must be absent for `MISS_THRESHOLD=3` consecutive scans before being marked closed

3. **OI wall detector** — fetches Polygon v3 snapshot for each active underlying; identifies strikes with ≥3000 OI; fires alert when any wall drops ≥40% between scans; confidence = 0.40 × OI size pct + 0.35 × drop speed + 0.25 × proximity; writes to `signals` DB table

4. **Unusual flow scanner** — computes per-contract `vol_delta = current_day_volume − baseline` from Redis snapshot (`options:vol_snap:{underlying}`); scores hits with importance formula; writes top 25 to `options:flow:latest` Redis key (90-min TTL); surfaced in Options Dashboard "Unusual Options Flow" panel

---

## Dividend Data Sources

| Purpose | Source | Notes |
|---|---|---|
| Income projection | `dividend_history` DB table | Actual payments backfilled from broker history; `forward_annual_rate = recent_aps × annual_count` |
| Ex/pay dates, frequency | Massive.com / Polygon.io (primary) | `list_dividends` via Massive MCP, `MASSIVE_API_KEY` |
| Ex/pay dates fallback | dividend.com scrape | Usually Cloudflare-blocked; falls through gracefully |
| Upcoming events | Massive.com → dividendchannel.com → DB | Three-tier for 7-day forward calendar |

---

## Ticker Classification (GICS)

Sector and industry data for all open position tickers is stored in the `ticker_classification` DB table (30-day TTL per ticker) and synced to Redis hashes (`ticker:sectors` / `ticker:industries`) consumed by the trader exclusion system. The WebUI background task refreshes stale records automatically at startup.

---

## Supported Brokers

| Broker | Paper | Live | Notes |
|---|---|---|---|
| Tradier | ✅ | ✅ | Equities + options |
| Alpaca | ✅ | ✅ | Equities, crypto |
| Webull | ✅ | ✅ | Equities, options |

---

## Data Subscriptions

OpenTrader integrates with several external services. The table below lists each one, whether it is required or optional, the plan tier needed, approximate monthly cost, and what it provides.

> Prices are approximate and subject to change — always verify current pricing on the provider's website.

### Core Data & AI

| Service | Required | Plan | ~Cost/mo | What it provides |
|---|---|---|---|---|
| [Polygon.io](https://polygon.io/dashboard/signup) via Massive.com | **Yes** | Stocks Starter + Options add-on | $29–$79+ | Real-time and historical OHLCV, options chains (greeks, IV, expiry), analyst ratings, news, dividends, earnings dates, market snapshot data, per-contract volume data for unusual flow detection. Used by: predictor, aggregator, options_monitor (OI walls, unusual flow), dividend subsystem, options chain display. Options data requires the Options add-on tier. |
| [OpenRouter](https://openrouter.ai/keys) | **Yes** | Pay-as-you-go | $5–50 | LLM inference (Claude, GPT-4o, and others) for AI trade signals, EOD review, directive evaluation, chat agent, and post-close analysis. Billed per token. Model assignments configurable per agent in `config/system.toml`. |

### Broker Accounts

At least one broker account is required for live or paper trading. All three provide free API access once a brokerage account is open.

| Broker | Required | Plan | Cost | What it provides |
|---|---|---|---|---|
| [Tradier](https://developer.tradier.com) | Optional | Brokerage account | Free API | Equities and options paper and live trading. Free sandbox API for paper; production key requires an active Tradier brokerage account. |
| [Alpaca](https://alpaca.markets) | Optional | Brokerage account | Free API | Equities paper and live trading. Paper API freely available without funded account. |
| [Webull](https://developer.webull.com/apis/docs/authentication/IndividualApplicationAPI) | Optional | Developer API (Individual) | Varies | Equities and options paper and live trading. Requires a Webull Developer Portal application — two key pairs needed: API Key + Secret (v1) for account data/positions/orders; App Key + App Secret (v2) for options chains. |

### Market Intelligence

These services enhance the signal pipeline, options flow detection, and macro analysis.

| Service | Required | Plan | ~Cost/mo | What it provides |
|---|---|---|---|---|
| [Unusual Whales](https://unusualwhales.com) | Optional | API subscription | ~$50 | Real-time options flow (bullish/bearish net premium), dark pool prints, market tide, Greek exposure, short interest. Powers Market Intelligence pipeline and Quick Intel cards. |
| [OVTLYR](https://console.ovtlyr.com) | Optional | Subscription | ~$49 | Market breadth bull/bear gauge with crossover detection. Feeds predictor confidence adjustment and the Trading Dashboard Market Breadth card. |
| [EODHD](https://eodhd.com) | Optional | All-in-One | ~$80 | Fundamentals, analyst ratings, insider transactions, earnings calendar, dividends, news, and macro indicators via the `ot-mcp-eodhd` MCP server. Feeds aggregator enrichment and predictor scoring. |
| [EODData](https://eoddata.com) | Optional | Basic | ~$20 | Market breadth indicators: MAHQ (new highs), LOWQ (new lows), TRIN (Arms Index). Used as supplemental data for the Market Health panel. |
| [FRED](https://fred.stlouisfed.org/docs/api/api_key.html) | Optional | Free | Free | HY OAS, IG OAS, Financial Stress Index, NBER recession signal. Powers the Macro Regime classifier and Macro Hub FRED panel. Free API key from St. Louis Fed. |
| [Alpha Vantage](https://www.alphavantage.co/support/#api-key) | Optional | Free / Premium | Free–$50 | Macro and equity news with sentiment scores. Powers the Unified News feed (AV News scraper + news badges). Free tier is rate-limited; premium unlocks higher throughput. |

### Notifications (Optional, Free)

| Service | Required | Plan | Cost | What it provides |
|---|---|---|---|---|
| [Telegram](https://telegram.org) | Optional | Bot API | Free | Push notifications for trade fills, alerts, circuit breaker trips, EOD summaries. Create via [@BotFather](https://t.me/BotFather). |
| [Discord](https://discord.com) | Optional | Webhook | Free | Trade and system event notifications via incoming webhooks. Supports separate webhooks for trades, alerts, and EOD reports. |
| [AgentMail](https://agentmail.to) | Optional | Free tier | Free | Email delivery for EOD reports, trade review findings, and scheduled daily reports. Free tier provides up to 3 inboxes. |

### Minimum Viable Setup

To run OpenTrader with a single paper trading account and AI signals:

1. **OpenRouter** — required for all LLM features (~$10–20/mo at moderate usage)
2. **Polygon.io** — required for market data ($29/mo Starter; add Options tier for options features)
3. **One broker** — Alpaca paper trading is free and requires no funded account

Everything else is optional and can be added incrementally.

---

## License

Apache License 2.0 — see [LICENSE](LICENSE) for details.
