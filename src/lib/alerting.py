"""Alerting helpers for the SDP migration utility.

Run-level failures are best handled by Databricks Job notifications (configured
in the job YAML). This module adds an optional webhook (Slack/Teams) run-end
summary for per-table failures that do NOT fail the whole job.
"""
from __future__ import annotations

import json
import urllib.request
from typing import Dict, Optional


def format_summary(business_unit: str, run_id: str, counts: Dict[str, int]) -> str:
    total = sum(counts.values())
    parts = ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
    failed = counts.get("FAILED", 0) + counts.get("SKIPPED", 0)
    head = "✅ all clean" if failed == 0 else f"⚠️ {failed} need attention"
    return (f"[SDP migration] BU={business_unit} run={run_id} — {head}\n"
            f"tables={total} | {parts}")


def post_webhook(url: str, text: str, timeout: int = 10) -> bool:
    """POST a Slack/Teams-style {'text': ...} payload. Returns True on 2xx."""
    if not url:
        return False
    data = json.dumps({"text": text}).encode("utf-8")
    req = urllib.request.Request(url, data=data,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return 200 <= resp.status < 300
    except Exception:
        return False


def alert_if_needed(counts: Dict[str, int], business_unit: str, run_id: str,
                    webhook_url: Optional[str], logger=print) -> None:
    text = format_summary(business_unit, run_id, counts)
    logger(text)
    if webhook_url and (counts.get("FAILED", 0) or counts.get("SKIPPED", 0)):
        ok = post_webhook(webhook_url, text)
        logger(f"webhook alert {'sent' if ok else 'FAILED to send'}")
