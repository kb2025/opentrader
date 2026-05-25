"""
OpenTrader Code Insights MCP Server
Exposes agent telemetry, event log, and Python codebase to Claude Desktop.
"""
import json
import os
import subprocess
from pathlib import Path
from typing import Optional

import psycopg2
import psycopg2.extras
from mcp.server.fastmcp import FastMCP

DB_URL    = os.getenv("DB_URL", "")
REPO_ROOT = Path(os.getenv("REPO_ROOT", "/repo/python"))

server = FastMCP(
    "opentrader-insights",
    instructions="""
# OpenTrader Code Insights

You have direct access to OpenTrader's agent telemetry stream and Python codebase.

Use this to:
- Diagnose agent errors and exceptions with full tracebacks
- Find slow MCP calls or degraded execution paths
- Read the actual Python source of any agent to propose fixes
- Search the codebase for patterns, function names, or error strings
- Mark events as resolved after fixing them

Workflow:
1. Call `get_summary` to orient yourself (which agents have errors)
2. Call `get_events` filtered by agent + severity to see what's failing
3. Call `get_event_detail` to get the full traceback for a specific event
4. Call `read_source_file` to read the relevant agent code
5. Propose a fix, then call `resolve_event` to close the event
""",
)


def _db():
    """Open a short-lived synchronous DB connection."""
    return psycopg2.connect(DB_URL)


# ── Tools ─────────────────────────────────────────────────────────────────────

@server.tool()
def get_summary(hours: int = 24) -> str:
    """
    Platform-wide telemetry summary: total events, breakdown by severity,
    and per-agent error/warn/total counts. Use this first to identify which
    agents are unhealthy.
    """
    if not DB_URL:
        return json.dumps({"error": "DB_URL not configured"})
    try:
        with _db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT agent, severity, COUNT(*) AS cnt, MAX(ts) AS last_seen
                    FROM execution_events
                    WHERE ts >= NOW() - INTERVAL '1 hour' * %s
                    GROUP BY agent, severity
                    ORDER BY agent, cnt DESC
                """, (hours,))
                rows = cur.fetchall()

        agents: dict = {}
        by_sev: dict = {}
        total = 0
        for r in rows:
            cnt = int(r["cnt"])
            total += cnt
            a, sv = r["agent"], r["severity"]
            agents.setdefault(a, {"total": 0, "errors": 0, "warns": 0, "last_seen": None})
            agents[a]["total"] += cnt
            if sv in ("error", "critical"):
                agents[a]["errors"] += cnt
            elif sv == "warn":
                agents[a]["warns"] += cnt
            ls = r["last_seen"].isoformat() if r["last_seen"] else None
            if not agents[a]["last_seen"] or (ls and ls > agents[a]["last_seen"]):
                agents[a]["last_seen"] = ls
            by_sev[sv] = by_sev.get(sv, 0) + cnt

        return json.dumps({
            "total_events": total,
            "hours":        hours,
            "by_severity":  by_sev,
            "agents":       [{"agent": k, **v} for k, v in sorted(
                             agents.items(), key=lambda x: -(x[1]["errors"] * 10 + x[1]["total"]))],
        }, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


@server.tool()
def list_agents(hours: int = 24) -> str:
    """
    List all agents that have emitted telemetry in the last N hours,
    ranked by error count. Returns agent name, error count, warn count,
    total events, and time of last event.
    """
    return get_summary(hours)


@server.tool()
def get_events(
    agent:      str = "",
    severity:   str = "",
    event_name: str = "",
    hours:      int = 24,
    limit:      int = 50,
    unresolved_only: bool = True,
) -> str:
    """
    Fetch recent execution events. Filter by agent name, severity
    (debug/info/warn/error/critical), or event_name. Set unresolved_only=False
    to include already-resolved events. Returns id, ts, agent, event_name,
    severity, duration_ms, and a payload summary.
    """
    if not DB_URL:
        return json.dumps({"error": "DB_URL not configured"})
    try:
        wheres = ["ts >= NOW() - INTERVAL '1 hour' * %s"]
        params = [hours]
        if agent:
            wheres.append("agent = %s");      params.append(agent)
        if severity:
            wheres.append("severity = %s");   params.append(severity)
        if event_name:
            wheres.append("event_name = %s"); params.append(event_name)
        if unresolved_only:
            wheres.append("resolved = FALSE")
        params.append(min(limit, 200))

        with _db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""SELECT id, ts, agent, event_name, severity,
                               duration_ms, payload, resolved
                        FROM execution_events
                        WHERE {' AND '.join(wheres)}
                        ORDER BY ts DESC LIMIT %s""",
                    params,
                )
                rows = cur.fetchall()

        events = []
        for r in rows:
            pl = r["payload"] or {}
            if isinstance(pl, str):
                try:
                    pl = json.loads(pl)
                except Exception:
                    pl = {}
            events.append({
                "id":          r["id"],
                "ts":          r["ts"].isoformat() if r["ts"] else None,
                "agent":       r["agent"],
                "event_name":  r["event_name"],
                "severity":    r["severity"],
                "duration_ms": round(r["duration_ms"], 1) if r["duration_ms"] else None,
                "payload":     pl,
                "resolved":    r["resolved"],
            })
        return json.dumps({"events": events, "count": len(events)}, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


@server.tool()
def get_event_detail(event_id: int) -> str:
    """
    Get the full detail for a specific event including the complete traceback.
    Use this after get_events to investigate a specific error.
    event_id comes from the 'id' field in get_events results.
    """
    if not DB_URL:
        return json.dumps({"error": "DB_URL not configured"})
    try:
        with _db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """SELECT id, ts, agent, event_name, severity,
                              duration_ms, payload, traceback_str, resolved, notes
                       FROM execution_events WHERE id = %s""",
                    (event_id,),
                )
                row = cur.fetchone()
        if not row:
            return json.dumps({"error": f"Event {event_id} not found"})
        pl = row["payload"] or {}
        if isinstance(pl, str):
            try:
                pl = json.loads(pl)
            except Exception:
                pl = {}
        return json.dumps({
            "id":            row["id"],
            "ts":            row["ts"].isoformat() if row["ts"] else None,
            "agent":         row["agent"],
            "event_name":    row["event_name"],
            "severity":      row["severity"],
            "duration_ms":   round(row["duration_ms"], 1) if row["duration_ms"] else None,
            "payload":       pl,
            "traceback":     row["traceback_str"] or "(no traceback)",
            "resolved":      row["resolved"],
            "notes":         row["notes"],
        }, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


@server.tool()
def resolve_event(event_id: int, notes: str = "") -> str:
    """
    Mark a telemetry event as resolved. Optionally add notes describing the fix.
    Call this after you have diagnosed and fixed the underlying issue.
    """
    if not DB_URL:
        return json.dumps({"error": "DB_URL not configured"})
    try:
        with _db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE execution_events SET resolved=TRUE, notes=%s WHERE id=%s",
                    (notes or None, event_id),
                )
            conn.commit()
        return json.dumps({"ok": True, "event_id": event_id})
    except Exception as e:
        return json.dumps({"error": str(e)})


@server.tool()
def read_source_file(path: str) -> str:
    """
    Read a Python source file from the OpenTrader codebase.
    path is relative to the python/ directory, e.g.:
      'shared/base_agent.py'
      'options_monitor/main.py'
      'webui/main.py'
      'scrapers/wsb/main.py'
    Returns the file contents with line numbers so you can reference specific lines.
    """
    try:
        # Prevent path traversal
        target = (REPO_ROOT / path).resolve()
        if not str(target).startswith(str(REPO_ROOT.resolve())):
            return json.dumps({"error": "Path outside repository"})
        if not target.exists():
            return json.dumps({"error": f"File not found: {path}"})
        if target.stat().st_size > 500_000:
            return json.dumps({"error": "File too large (>500KB). Use search_codebase to find specific sections."})
        lines = target.read_text(errors="replace").splitlines()
        numbered = "\n".join(f"{i+1:5}: {line}" for i, line in enumerate(lines))
        return f"# {path} ({len(lines)} lines)\n\n{numbered}"
    except Exception as e:
        return json.dumps({"error": str(e)})


@server.tool()
def search_codebase(pattern: str, path_filter: str = "", context_lines: int = 3) -> str:
    """
    Search the OpenTrader Python codebase (python/) for a pattern using grep.
    pattern: regex or literal string to search for
    path_filter: optional glob to narrow search, e.g. 'options_monitor/*.py' or 'shared/'
    context_lines: lines of context around each match (default 3)
    Returns matching file:line results with surrounding context.
    """
    try:
        search_root = str(REPO_ROOT)
        if path_filter:
            search_root = str(REPO_ROOT / path_filter.lstrip("/"))

        result = subprocess.run(
            ["grep", "-rn", f"-C{context_lines}", "--include=*.py", pattern, search_root],
            capture_output=True, text=True, timeout=15
        )
        output = result.stdout or result.stderr
        if not output.strip():
            return f"No matches found for '{pattern}'"
        # Trim to 8000 chars to avoid overwhelming context
        if len(output) > 8000:
            output = output[:8000] + "\n... (truncated — narrow search with path_filter)"
        return output
    except subprocess.TimeoutExpired:
        return json.dumps({"error": "Search timed out — use a more specific pattern"})
    except Exception as e:
        return json.dumps({"error": str(e)})


@server.tool()
def list_source_files(subdir: str = "") -> str:
    """
    List Python source files in the codebase.
    subdir: optional subdirectory, e.g. 'options_monitor', 'shared', 'scrapers'
    Returns a tree of .py files to help navigate the codebase.
    """
    try:
        root = (REPO_ROOT / subdir).resolve() if subdir else REPO_ROOT.resolve()
        if not str(root).startswith(str(REPO_ROOT.resolve())):
            return json.dumps({"error": "Path outside repository"})
        files = sorted(str(p.relative_to(REPO_ROOT)) for p in root.rglob("*.py")
                       if "__pycache__" not in str(p) and "test" not in str(p))
        return "\n".join(files) if files else "No Python files found."
    except Exception as e:
        return json.dumps({"error": str(e)})
