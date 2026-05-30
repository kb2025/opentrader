"""
Orchestrator Email Monitor
Polls the ot-orchestrator inbox for new messages, runs LLM analysis,
and delivers a structured research brief to the report recipient.
"""
import asyncio
import logging
import os

import aiohttp
import redis.asyncio as aioredis

from notifier.agentmail import Notifier

log = logging.getLogger(__name__)

POLL_INTERVAL_SEC   = int(os.getenv("EMAIL_MONITOR_INTERVAL_SEC", "300"))
REDIS_PROCESSED_KEY = "orchestrator:email:processed"


class EmailMonitor:
    """Polls the orchestrator inbox and delivers LLM analysis for each new message."""

    def __init__(self, redis: aioredis.Redis, notifier: Notifier):
        self.redis     = redis
        self.notifier  = notifier
        inbox_raw      = os.getenv("AGENTMAIL_ORCHESTRATOR_INBOX", "ot-orchestrator")
        self.inbox_id  = inbox_raw if "@" in inbox_raw else f"{inbox_raw}@agentmail.to"
        self.recipient = os.getenv("REPORT_RECIPIENT_EMAIL", "")
        _base          = os.getenv("AGENTMAIL_BASE_URL", "https://api.agentmail.to")
        _key           = os.getenv("AGENTMAIL_API_KEY", "")
        self.base_url  = _base.rstrip("/")
        self.headers   = {
            "Authorization": f"Bearer {_key}",
            "Content-Type":  "application/json",
        }

    async def run(self):
        log.info(f"email-monitor.started inbox={self.inbox_id} interval={POLL_INTERVAL_SEC}s")
        # Stagger start so orchestrator finishes its own init first
        await asyncio.sleep(30)
        while True:
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"email-monitor.poll_error error={e}")
            await asyncio.sleep(POLL_INTERVAL_SEC)

    async def _poll_once(self):
        messages = await self._fetch_unread()
        for msg in messages:
            msg_id = msg.get("message_id", "")
            if not msg_id:
                continue
            already = await self.redis.sismember(REDIS_PROCESSED_KEY, msg_id)
            if already:
                continue
            # Fetch full thread text (list endpoint only returns preview)
            full_text = await self._fetch_full_text(msg.get("thread_id", ""))
            await self._process(msg, full_text)
            await self.redis.sadd(REDIS_PROCESSED_KEY, msg_id)
            await self._mark_read(msg_id)

    async def _fetch_unread(self) -> list:
        try:
            async with aiohttp.ClientSession() as s:
                r = await s.get(
                    f"{self.base_url}/v0/inboxes/{self.inbox_id}/messages",
                    headers=self.headers,
                    timeout=aiohttp.ClientTimeout(total=15),
                )
                if r.status != 200:
                    log.warning(f"email-monitor.fetch_failed status={r.status}")
                    return []
                data = await r.json()
                msgs = data if isinstance(data, list) else data.get("messages", [])
                return [m for m in msgs if "unread" in m.get("labels", [])]
        except Exception as e:
            log.warning(f"email-monitor.fetch_error error={e}")
            return []

    async def _fetch_full_text(self, thread_id: str) -> str:
        if not thread_id:
            return ""
        try:
            async with aiohttp.ClientSession() as s:
                r = await s.get(
                    f"{self.base_url}/v0/inboxes/{self.inbox_id}/threads/{thread_id}",
                    headers=self.headers,
                    timeout=aiohttp.ClientTimeout(total=15),
                )
                if r.status != 200:
                    return ""
                data = await r.json()
                for m in data.get("messages", []):
                    text = m.get("text", "")
                    if text and text.strip():
                        return text
        except Exception as e:
            log.warning(f"email-monitor.thread_fetch_error error={e}")
        return ""

    async def _process(self, msg: dict, full_text: str):
        subject = msg.get("subject", "(no subject)")
        sender  = msg.get("from", "unknown")
        body    = (full_text or msg.get("preview", "")).strip()

        if not body:
            log.info(f"email-monitor.skip_empty subject={subject}")
            return

        log.info(f"email-monitor.analyzing subject={subject!r}")
        analysis = await self._llm_analyze(subject, sender, body[:6000])

        email_body = (
            f"From: {sender}\n"
            f"Subject: {subject}\n\n"
            f"{'─' * 60}\n"
            f"ANALYSIS\n"
            f"{'─' * 60}\n"
            f"{analysis}\n\n"
            f"{'─' * 60}\n"
            f"ORIGINAL (excerpt)\n"
            f"{'─' * 60}\n"
            f"{body[:1500]}"
        )
        await asyncio.gather(
            self.notifier.send_email(
                to      = self.recipient,
                subject = f"[OpenTrader Research] {subject}",
                body    = email_body,
            ),
            self.notifier.telegram(
                f"*[Inbox Analysis]*\n*{subject}*\n\n{analysis[:1500]}"
            ),
            return_exceptions=True,
        )
        log.info(f"email-monitor.delivered subject={subject!r}")

    async def _llm_analyze(self, subject: str, sender: str, body: str) -> str:
        try:
            from llm.connector import LLMConnector
            llm    = LLMConnector("review")
            prompt = f"""Analyze this email forwarded to the OpenTrader orchestrator research inbox.

Subject: {subject}
From: {sender}

Body:
{body}

Provide a structured analysis covering:

1. CONTENT TYPE — research / dividend / earnings / macro / news / trade directive / newsletter / other
2. KEY TICKERS — list each ticker with: direction bias (bullish / bearish / neutral) and one-line reason
3. ACTIONABLE INSIGHTS — what should a dividend and equity systematic trader do with this information
4. RISKS & CONCERNS — any red flags, caveats, or uncertainties raised
5. SUMMARY — 2-3 concise sentences capturing the core takeaway

Be specific and data-driven. Reference actual figures, percentages, and company names from the email."""

            return await llm.complete(
                prompt     = prompt,
                system     = (
                    "You are a buy-side trading research analyst. "
                    "Extract structured, actionable intelligence from financial emails. "
                    "Be precise, reference concrete data points, and focus on what matters for a systematic equity and dividend portfolio."
                ),
                max_tokens = 800,
            )
        except Exception as e:
            log.warning(f"email-monitor.llm_failed error={e}")
            return f"(LLM unavailable: {e})\n\nSubject: {subject}\nFrom: {sender}"

    async def _mark_read(self, message_id: str):
        try:
            async with aiohttp.ClientSession() as s:
                await s.patch(
                    f"{self.base_url}/v0/inboxes/{self.inbox_id}/messages/{message_id}",
                    headers=self.headers,
                    json={"labels": ["received"]},
                    timeout=aiohttp.ClientTimeout(total=10),
                )
        except Exception as e:
            log.debug(f"email-monitor.mark_read_failed error={e}")
