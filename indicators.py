"""ADX computation and optional RSI/MACD/ADX filters for valid entries."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass
class IndicatorFilterSettings:
    enabled: bool = False
    rsi_enabled: bool = True
    rsi_long_max: float = 70.0
    rsi_short_min: float = 30.0
    macd_enabled: bool = False
    adx_enabled: bool = True
    adx_min_trend: float = 25.0
    adx_use_htf: bool = True


@dataclass
class IndicatorFilterResult:
    allowed: bool
    confidence: int
    block_reason: str | None = None
    notes: str = ""


def compute_adx(df: pd.DataFrame, period: int = 14) -> float | None:
    """Wilder ADX from OHLC dataframe with columns h, l, c."""
    if df is None or len(df) < period * 2 + 1:
        return None
    required = {"h", "l", "c"}
    if not required.issubset(df.columns):
        return None

    high = df["h"].astype(float)
    low = df["l"].astype(float)
    close = df["c"].astype(float)

    prev_close = close.shift(1)
    tr = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

    alpha = 1 / period
    atr = tr.ewm(alpha=alpha, adjust=False).mean()
    plus_di = 100 * plus_dm.ewm(alpha=alpha, adjust=False).mean() / atr.replace(0, 1e-12)
    minus_di = 100 * minus_dm.ewm(alpha=alpha, adjust=False).mean() / atr.replace(0, 1e-12)
    dx = (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, 1e-12) * 100
    adx = dx.ewm(alpha=alpha, adjust=False).mean()
    value = float(adx.iloc[-1])
    if pd.isna(value):
        return None
    return value


def evaluate_indicator_filters(
    signal: str,
    confidence: int,
    analysis: dict[str, Any],
    settings: IndicatorFilterSettings,
) -> IndicatorFilterResult:
    if not settings.enabled or signal not in ("LONG", "SHORT"):
        return IndicatorFilterResult(True, confidence)

    notes: list[str] = []
    adjusted = confidence

    if settings.rsi_enabled:
        rsi = analysis.get("rsi")
        if rsi is not None:
            if signal == "LONG" and rsi > settings.rsi_long_max:
                return IndicatorFilterResult(
                    False,
                    adjusted,
                    block_reason="rsi_overbought",
                    notes=f"RSI {rsi:.1f} > {settings.rsi_long_max}",
                )
            if signal == "SHORT" and rsi < settings.rsi_short_min:
                return IndicatorFilterResult(
                    False,
                    adjusted,
                    block_reason="rsi_oversold",
                    notes=f"RSI {rsi:.1f} < {settings.rsi_short_min}",
                )

    if settings.macd_enabled:
        macd_hist = analysis.get("macd_hist")
        if macd_hist is not None:
            if signal == "LONG" and macd_hist < 0:
                return IndicatorFilterResult(
                    False,
                    adjusted,
                    block_reason="macd_bearish",
                    notes=f"MACD hist {macd_hist:+.4f}",
                )
            if signal == "SHORT" and macd_hist > 0:
                return IndicatorFilterResult(
                    False,
                    adjusted,
                    block_reason="macd_bullish",
                    notes=f"MACD hist {macd_hist:+.4f}",
                )

    if settings.adx_enabled:
        adx_key = "htf_adx" if settings.adx_use_htf else "adx"
        adx = analysis.get(adx_key)
        if adx is not None and adx < settings.adx_min_trend:
            return IndicatorFilterResult(
                False,
                adjusted,
                block_reason="adx_low",
                notes=f"ADX {adx:.1f} < {settings.adx_min_trend}",
            )

    return IndicatorFilterResult(True, adjusted, notes=" · ".join(notes))
