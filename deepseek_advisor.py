"""DeepSeek API advisor for fleet config suggestions from decision_events."""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
import urllib.error
import urllib.request
from typing import Any

import db_analytics
import db_store

logger = logging.getLogger(__name__)

DEEPSEEK_ENABLED = os.getenv("DEEPSEEK_ENABLED", "").lower() in ("1", "true", "yes")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "").strip()
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com").rstrip("/")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")
DEEPSEEK_MAX_TOKENS = int(os.getenv("DEEPSEEK_MAX_TOKENS", "1800"))
DEEPSEEK_TIMEOUT = float(os.getenv("DEEPSEEK_TIMEOUT", "60"))
DEEPSEEK_SUGGESTIONS_COOLDOWN = int(os.getenv("DEEPSEEK_SUGGESTIONS_COOLDOWN", "300"))

SYSTEM_PROMPT = """You are a trading-bot configuration advisor for a Binance futures signal fleet.
The bot logs decision_events: valid_entry, valid_entry_blocked, indicator_blocked, order_skip, etc.

Analyze the JSON statistics and suggest practical config tuning.
Focus on: ADX/RSI filters, signal cooldown, HTF trend alignment, per-symbol overrides (MySQL symbol_config),
and execution settings when order_skip dominates.

Rules:
- Use only keys listed in overridable_keys for config_changes.
- Be specific: cite counts and averages from the data.
- If sample size is small (<20 events), say so and avoid aggressive changes.
- Do not suggest disabling risk filters entirely.
- Per-symbol overrides go in symbol_config table JSON, not .env.

Respond ONLY with valid JSON (no markdown):
{
  "summary": "2-3 sentences in Spanish",
  "suggestions": [
    {
      "priority": "high|medium|low",
      "area": "ADX|RSI|COOLDOWN|HTF|EXECUTION|SYMBOL|GENERAL",
      "title": "short title in Spanish",
      "detail": "actionable explanation in Spanish",
      "config_changes": {"KEY": "value"},
      "symbol": "ETHUSDT or null for fleet-wide"
    }
  ],
  "warnings": ["optional risks in Spanish"]
}

When config_changes is non-empty for a specific pair, symbol MUST be set (e.g. BTCUSDT)."""

_cache_lock = threading.Lock()
_cache: dict[str, tuple[float, dict[str, Any]]] = {}


def is_enabled() -> bool:
    return DEEPSEEK_ENABLED and bool(DEEPSEEK_API_KEY)


def is_configured() -> bool:
    return bool(DEEPSEEK_API_KEY)


def _cache_key(days: int | None, symbol: str | None) -> str:
    return f"{days or 'all'}:{(symbol or 'ALL').upper()}"


def _extract_json(text: str) -> dict[str, Any]:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        raise ValueError("DeepSeek response is not a JSON object")
    return parsed


def _chat_completion(user_content: str) -> str:
    payload = {
        "model": DEEPSEEK_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
        "temperature": 0.2,
        "max_tokens": DEEPSEEK_MAX_TOKENS,
        "response_format": {"type": "json_object"},
    }
    request = urllib.request.Request(
        f"{DEEPSEEK_BASE_URL}/v1/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=DEEPSEEK_TIMEOUT) as response:
            body = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"DeepSeek HTTP {exc.code}: {detail[:400]}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"DeepSeek connection failed: {exc.reason}") from exc

    choices = body.get("choices") or []
    if not choices:
        raise RuntimeError("DeepSeek returned no choices")
    message = choices[0].get("message") or {}
    content = message.get("content")
    if not content:
        raise RuntimeError("DeepSeek returned empty content")
    return str(content)


def generate_suggestions(
    *,
    days: int | None = 7,
    symbol: str | None = None,
    force: bool = False,
) -> dict[str, Any]:
    if not DEEPSEEK_ENABLED:
        return {
            "enabled": False,
            "error": "DEEPSEEK_ENABLED=false — set DEEPSEEK_ENABLED=true in .env",
        }
    if not DEEPSEEK_API_KEY:
        return {
            "enabled": False,
            "error": "DEEPSEEK_API_KEY missing in .env",
        }
    if not db_store.is_enabled():
        return {
            "enabled": True,
            "error": "DB_ENABLED=false — analytics data required for suggestions",
        }

    cache_key = _cache_key(days, symbol)
    now = time.time()
    if not force:
        with _cache_lock:
            cached = _cache.get(cache_key)
            if cached and now - cached[0] < DEEPSEEK_SUGGESTIONS_COOLDOWN:
                result = dict(cached[1])
                result["cached"] = True
                result["cached_seconds_ago"] = int(now - cached[0])
                return result

    context = db_analytics.build_suggestion_context(days, symbol)
    total = int((context.get("overview") or {}).get("total_events") or 0)
    if total == 0:
        return {
            "enabled": True,
            "error": "No decision_events in this range — let the fleet run longer",
            "context_events": 0,
        }

    user_prompt = (
        "Analyze this fleet decision log and propose config suggestions:\n\n"
        + json.dumps(context, default=str, ensure_ascii=False, indent=2)
    )

    try:
        raw = _chat_completion(user_prompt)
        parsed = _extract_json(raw)
    except (json.JSONDecodeError, ValueError) as exc:
        logger.warning("DeepSeek JSON parse failed: %s", exc)
        return {
            "enabled": True,
            "error": f"Invalid JSON from DeepSeek: {exc}",
        }
    except RuntimeError as exc:
        logger.warning("DeepSeek request failed: %s", exc)
        return {
            "enabled": True,
            "error": str(exc),
        }

    result = {
        "enabled": True,
        "cached": False,
        "model": DEEPSEEK_MODEL,
        "context_events": total,
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "summary": parsed.get("summary", ""),
        "suggestions": parsed.get("suggestions") or [],
        "warnings": parsed.get("warnings") or [],
    }

    with _cache_lock:
        _cache[cache_key] = (now, result)

    return result
