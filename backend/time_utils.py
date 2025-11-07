# time_utils.py
from __future__ import annotations
import time
import datetime
import logging

logger = logging.getLogger("AARIA.TimeUtils")

# Try to import dateutil & tzlocal for robust parsing/timezone detection
try:
    from dateutil import parser as _du_parser  # type: ignore
    from dateutil import tz as _du_tz  # type: ignore
    _HAS_DATEUTIL = True
except Exception:
    _HAS_DATEUTIL = False

try:
    import tzlocal as _tzlocal  # type: ignore
    _HAS_TZLOCAL = True
except Exception:
    _HAS_TZLOCAL = False

def now_ts() -> float:
    return time.time()

def now_utc() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)

def local_tz():
    """Return a tzinfo instance representing local timezone, best-effort."""
    if _HAS_TZLOCAL:
        try:
            return _tzlocal.get_localzone()
        except Exception:
            pass
    # fallback to dateutil tzlocal if available
    if _HAS_DATEUTIL:
        try:
            return _du_tz.tzlocal()
        except Exception:
            pass
    # final fallback: UTC
    return datetime.timezone.utc

def to_local_iso(ts: float | datetime.datetime) -> str:
    """Return ISO string in local timezone for a timestamp or datetime."""
    if isinstance(ts, (int, float)):
        dt = datetime.datetime.fromtimestamp(float(ts), tz=local_tz())
    elif isinstance(ts, datetime.datetime):
        if ts.tzinfo is None:
            dt = ts.replace(tzinfo=datetime.timezone.utc).astimezone(local_tz())
        else:
            dt = ts.astimezone(local_tz())
    else:
        raise TypeError("ts must be float|int|datetime")
    return dt.isoformat()

def parse_to_timestamp(text: str, prefer_future: bool = True) -> float | None:
    """
    Parse natural text to a unix timestamp (seconds).
    prefer_future: when ambiguous, prefer future occurrence (e.g., '5pm' -> next 5pm if already past).
    Returns None when parsing fails.
    """
    text = (text or "").strip()
    if not text:
        return None

    # Try dateutil first
    if _HAS_DATEUTIL:
        try:
            now = datetime.datetime.now(local_tz())
            dt = _du_parser.parse(text, fuzzy=True, default=now)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=local_tz())
            ts = dt.timestamp()
            if prefer_future and ts < time.time():
                # roll forward by days until in future (limit)
                attempts = 0
                while ts < time.time() and attempts < 365:
                    dt = dt + datetime.timedelta(days=1)
                    ts = dt.timestamp()
                    attempts += 1
            return ts
        except Exception:
            logger.debug("dateutil parse failed for: %r", text, exc_info=True)

    # Fallback naive parsing for 'in N seconds/minutes' and 'tomorrow at 5pm' and '5pm'
    try:
        import re
        m = re.search(r'in\s+(\d+)\s*(second|seconds|sec|s|minute|minutes|min|m|hour|hours|h)', text, flags=re.I)
        if m:
            val = int(m.group(1))
            unit = m.group(2).lower()
            if unit.startswith('s'):
                return time.time() + val
            if unit.startswith('m'):
                return time.time() + val * 60
            if unit.startswith('h'):
                return time.time() + val * 3600
        # tomorrow / today + time like 'tomorrow at 5pm' or '5pm'
        m2 = re.search(r'(today|tomorrow)?\s*(at)?\s*(\d{1,2})(:(\d{2}))?\s*(am|pm)?', text, flags=re.I)
        if m2:
            dayword = m2.group(1)
            hr = int(m2.group(3))
            mn = int(m2.group(5) or 0)
            ampm = (m2.group(6) or "").lower()
            if ampm == 'pm' and hr < 12:
                hr += 12
            if ampm == 'am' and hr == 12:
                hr = 0
            now_dt = datetime.datetime.now(local_tz())
            target_date = now_dt.date()
            if (dayword or "").lower() == "tomorrow":
                target_date = target_date + datetime.timedelta(days=1)
            dt = datetime.datetime(target_date.year, target_date.month, target_date.day, hr, mn, tzinfo=local_tz())
            ts = dt.timestamp()
            if prefer_future and ts < time.time():
                dt = dt + datetime.timedelta(days=1)
                ts = dt.timestamp()
            return ts
    except Exception:
        logger.debug("naive parse fallback failed for: %r", text, exc_info=True)

    return None