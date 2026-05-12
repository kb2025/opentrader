# ⚠️ Risk Disclosure

**Read this before connecting OpenTrader to any account holding real money.**

OpenTrader is free, open-source software for automated trading. It is provided "as is," with no warranties, no guarantees, and no professional oversight. **By using it, you accept full responsibility for every order it places and every dollar it moves.** If that is not acceptable to you, stop here and do not use this software.

This document summarizes the risks. It does not replace the [Terms of Use](./TERMS.md) or the [LICENSE](./LICENSE), which legally govern your use of the Software.

---

## 1. You Can Lose Money — Possibly All Of It

Trading financial instruments involves substantial risk. Automated trading amplifies that risk. With OpenTrader connected to a live account, it is possible — and in some scenarios likely — that you will lose part or all of your capital, and in some cases more than your initial deposit (e.g. margin, futures, or leveraged positions).

Losses can occur from:

- **Strategy risk** — your strategy is flawed, overfit to historical data, or unsuited to current market conditions.
- **Software risk** — bugs, race conditions, integer overflows, off-by-one errors, or unhandled edge cases in OpenTrader or its dependencies.
- **Configuration risk** — wrong position size, wrong symbol, wrong account, missing stop-loss, decimal-point error.
- **Infrastructure risk** — your computer crashes, your internet drops, your VPS reboots, your power fails mid-order.
- **Broker / exchange risk** — the venue rejects orders, executes at the wrong price, goes down, freezes withdrawals, or fails entirely.
- **Market risk** — flash crashes, gaps, halts, illiquidity, news events, manipulation, and other conditions no software can predict or react to in time.

**Past performance is not indicative of future results.** A backtest that shows steady profit can lose money the moment it runs live. A paper-trading session that goes well for a month can blow up in a day. Live markets behave differently than simulations in ways that are not always reproducible.

## 2. No Advice, No Recommendations, No Guarantees

The authors and contributors of OpenTrader are **not** investment advisers, broker-dealers, financial planners, or licensed professionals of any kind. Nothing in this repository — including the code, default parameters, example strategies, documentation, issues, discussions, or any communication from the maintainers — constitutes financial, investment, legal, or tax advice, or a recommendation to buy, sell, or hold anything.

There is no guarantee that OpenTrader will be profitable, accurate, secure, or even functional. There is no SLA, no support obligation, and no warranty.

## 3. You Are Responsible

When you run OpenTrader, **you** are the trader. The software is a tool, not a counterparty, agent, or advisor. You are responsible for:

- Choosing and validating your strategies.
- Reviewing the source code, or having someone you trust review it, before connecting it to real money.
- Testing thoroughly in paper-trading or simulation mode first.
- Setting appropriate position limits, stop-losses, and circuit breakers.
- Monitoring the software while it runs. **Do not set and forget.**
- Securing your API keys, credentials, and the machine OpenTrader runs on.
- Complying with all laws and regulations in your jurisdiction, and the terms of service of every broker, exchange, or data provider you connect to.
- Reporting and paying any taxes on your trading activity.

If OpenTrader does something you didn't expect, that is your problem to detect, stop, and remediate.

## 4. Algorithmic Trading Has Unique Failure Modes

Automated systems can fail in ways manual trading cannot:

- **Runaway loops** placing hundreds of orders per second.
- **Stale data** causing trades against prices that no longer exist.
- **Reconnect storms** that fill, cancel, and re-fill the same position repeatedly.
- **Silent failures** where the bot appears to be running but is not — or appears to be stopped but is not.
- **Time-zone, daylight-savings, and timestamp bugs** that fire trades at the wrong moment.
- **Dependency updates** that change behavior between runs.
- **Race conditions** between order submission, fill confirmation, and position tracking.

A small bug can cause large losses very quickly. By the time you notice, the damage may already be done and irreversible.

## 5. Backtests And Paper Trading Can Mislead You

A profitable backtest is not a guarantee — and not even strong evidence — of future profit. Common reasons backtests overstate real performance include lookahead bias, survivorship bias, overfitting, ignored slippage and commissions, unrealistic fill assumptions, and curve-fitting to noise. Paper trading omits real-world friction such as partial fills, queue position, latency, and the psychological effect of seeing real money move. **Treat both as exploratory tools, not as proof.**

## 6. Third-Party Brokers And Exchanges

OpenTrader connects to third-party brokers, exchanges, and data providers. The authors do not operate, control, or vouch for any of them. If a broker goes insolvent, freezes your account, executes a trade incorrectly, suffers an outage, or is hacked, that is between you and them. OpenTrader cannot recover funds, reverse trades, or compel a venue to act.

You are responsible for reading and complying with every Terms of Service for every venue you connect.

## 7. Security

Your API keys can move real money. Treat them like cash:

- **Never commit them to a repository.** Use environment variables, a secrets manager, or an encrypted config.
- **Restrict permissions on every key.** If your strategy doesn't need withdrawal permission, disable it. If it doesn't need margin, disable it. Whitelist IPs when the venue supports it.
- **Run on a machine you control and trust.** A shared, public, or compromised host can leak everything in seconds.
- **Rotate keys after any suspected exposure**, including any time you share logs or screenshots for support.

The authors are not responsible for losses from compromised keys, compromised machines, or leaked credentials.

## 8. Regulatory Risk

Trading is regulated, and the rules vary widely by country, state, asset class, and account type. Automated trading may have additional disclosure, registration, or compliance requirements where you live. Some strategies — even ones that seem harmless — may be illegal in your jurisdiction (wash trading, spoofing, layering, certain forms of market-making, certain crypto activity, etc.).

**You are responsible for knowing and following the law.** The authors of OpenTrader take no position on whether your specific use is legal where you live, and they cannot give you that advice.

## 9. No Support, No Promises

OpenTrader is a volunteer project. There is no help desk, no on-call engineer, no guaranteed response time. Issues may be ignored. Bugs may go unfixed. Features may be removed. The project may be abandoned. **Do not depend on OpenTrader for anything you cannot afford to lose.**

## 10. Only Use Money You Can Afford To Lose

This is the single most important sentence in this document. Treat any capital you give OpenTrader access to as money that may not come back. If losing it would harm you, your family, your business, your retirement, your rent, or your peace of mind — **do not use this software with that money**.

---

## Before You Go Live — Checklist

Before connecting OpenTrader to a live account, you should be able to honestly say "yes" to all of these:

- [ ] I have read the [Terms of Use](./TERMS.md) and the [LICENSE](./LICENSE) in full.
- [ ] I have read or reviewed the source code of the strategy I will run.
- [ ] I have tested the exact configuration I will run in paper trading or simulation, for a meaningful period.
- [ ] I have set position-size limits, daily loss limits, and a kill switch.
- [ ] I have restricted my API keys to the minimum permissions required.
- [ ] I have a plan for how to detect, halt, and recover from a malfunction.
- [ ] I can afford to lose the entire amount in the connected account.
- [ ] I am the only person responsible for what happens next.

If you cannot check every box, **do not enable live trading.**

---

**By using OpenTrader, you acknowledge that you have read and understood this Risk Disclosure and accept all risks of using the Software.**
