"""Domain model: order statuses, allowed transitions, validation, time helpers, hashing."""
from __future__ import annotations

import hashlib
import json
import re
import threading
import uuid
from datetime import datetime, timedelta, timezone

UTC = timezone.utc

# --------------------------------------------------------------------------- statuses
ACCEPTED = "ACCEPTED"          # durable, waiting for T-24h
PREPARING = "PREPARING"        # claimed by the scheduler, forward saga running
READY = "READY"                # all forward steps confirmed
COMPENSATING = "COMPENSATING"  # reverse saga running (failure or cancellation)
CANCELLED = "CANCELLED"        # final: cancelled, every effect confirmed undone
PREP_FAILED = "PREP_FAILED"    # final: preparation failed, every effect confirmed undone
MANUAL_REVIEW = "MANUAL_REVIEW"  # final: at least one compensation not confirmed

FINAL_STATUSES = frozenset({CANCELLED, PREP_FAILED, MANUAL_REVIEW})

TRANSITIONS = {
    ACCEPTED: frozenset({PREPARING, CANCELLED}),
    PREPARING: frozenset({READY, COMPENSATING}),
    READY: frozenset({COMPENSATING}),
    COMPENSATING: frozenset({CANCELLED, PREP_FAILED, MANUAL_REVIEW}),
}

# Action-ledger statuses (forward actions and compensations).
PENDING = "PENDING"
SUCCEEDED = "SUCCEEDED"
FAILED = "FAILED"
UNKNOWN = "UNKNOWN"          # ambiguous provider response; must reconcile before reissue
NOT_APPLIED = "NOT_APPLIED"  # reconciliation proved the effect never happened

PREPARATION_LEAD = timedelta(hours=24)


class ValidationError(ValueError):
    def __init__(self, message: str, field: str | None = None):
        super().__init__(message)
        self.field = field


class InvalidTransition(Exception):
    pass


def can_transition(current: str, new: str) -> bool:
    return new in TRANSITIONS.get(current, ())


def assert_transition(current: str, new: str) -> None:
    if not can_transition(current, new):
        raise InvalidTransition(f"transition {current} -> {new} is not allowed")


# --------------------------------------------------------------------------- time
def to_iso(dt: datetime) -> str:
    """UTC, second precision, fixed width: lexicographic order == chronological order."""
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def from_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


class ManualClock:
    """Controllable clock for demos and tests."""

    def __init__(self, start: datetime | None = None):
        self._now = (start or datetime.now(UTC)).astimezone(UTC).replace(microsecond=0)
        self._lock = threading.Lock()

    def now(self) -> datetime:
        with self._lock:
            return self._now

    def advance(self, delta: timedelta) -> datetime:
        with self._lock:
            self._now = self._now + delta
            return self._now

    def set(self, dt: datetime) -> datetime:
        with self._lock:
            self._now = dt.astimezone(UTC).replace(microsecond=0)
            return self._now


def parse_checkin(value) -> datetime:
    """Parse an ISO-8601 check-in time that MUST carry an explicit UTC offset."""
    if not isinstance(value, str) or not value.strip():
        raise ValidationError("checkIn is required", "checkIn")
    text = value.strip()
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00") if text.endswith("Z") else text)
    except ValueError:
        raise ValidationError("checkIn must be ISO-8601, e.g. 2026-11-20T15:00:00+08:00", "checkIn")
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValidationError(
            "checkIn must include an explicit UTC offset, e.g. 2026-11-20T15:00:00+08:00", "checkIn")
    return dt


def compute_due_at(checkin: datetime, now: datetime) -> datetime:
    """dueAt = check-in - 24h (UTC). If that moment already passed but check-in is still
    in the future, preparation is due immediately. Past check-ins are rejected."""
    checkin_utc = checkin.astimezone(UTC)
    now_utc = now.astimezone(UTC)
    if checkin_utc <= now_utc:
        raise ValidationError("checkIn is in the past", "checkIn")
    return max(checkin_utc - PREPARATION_LEAD, now_utc)


# --------------------------------------------------------------------------- validation
_FLIGHT_RE = re.compile(r"^[A-Z0-9]{2}[0-9]{1,4}[A-Z]?$")
_HOTEL_RE = re.compile(r"^[A-Za-z0-9_-]{2,40}$")
_ROOM_REF_RE = re.compile(r"^[A-Za-z0-9_/-]{1,64}$")
_FLAG_RE = re.compile(r"^[a-z][a-z0-9_-]{0,29}$")
_IDEM_RE = re.compile(r"^[A-Za-z0-9._:-]{1,100}$")


def _str_list(body, field, max_items, max_len, pattern=None):
    value = body.get(field, [])
    if value is None:
        return []
    if not isinstance(value, list) or len(value) > max_items:
        raise ValidationError(f"{field} must be a list of at most {max_items} strings", field)
    out = []
    for item in value:
        if not isinstance(item, str) or not item.strip() or len(item.strip()) > max_len:
            raise ValidationError(f"{field} entries must be non-empty strings (max {max_len} chars)", field)
        item = item.strip()
        if pattern and not pattern.match(item):
            raise ValidationError(f"invalid {field} entry: {item!r}", field)
        if item not in out:
            out.append(item)
    return out


def validate_profile(body) -> dict:
    if not isinstance(body, dict):
        raise ValidationError("profile body must be a JSON object")
    sizes = body.get("sizes", {}) or {}
    if not isinstance(sizes, dict) or len(sizes) > 10:
        raise ValidationError("sizes must be an object with at most 10 entries", "sizes")
    clean_sizes = {}
    for k, v in sizes.items():
        if not isinstance(k, str) or not _FLAG_RE.match(k) or not isinstance(v, str) or not 0 < len(v.strip()) <= 10:
            raise ValidationError(f"invalid size entry {k!r}", "sizes")
        clean_sizes[k] = v.strip().upper()
    name = body.get("displayName", "")
    if not isinstance(name, str) or len(name) > 60:
        raise ValidationError("displayName must be a string of at most 60 chars", "displayName")
    flags = body.get("allergyFlags", []) or []
    if isinstance(flags, list):
        flags = [f.strip().lower() if isinstance(f, str) else f for f in flags]
    return {
        "displayName": name.strip(),
        "sizes": clean_sizes,
        "allergyFlags": _str_list({"allergyFlags": flags}, "allergyFlags", 10, 30, _FLAG_RE),
        "preferredBrands": _str_list(body, "preferredBrands", 10, 40),
    }


def validate_order_request(body, scenarios) -> dict:
    """Shape validation only (time-independent) - the result is what gets hashed."""
    if not isinstance(body, dict):
        raise ValidationError("order body must be a JSON object")
    flight = str(body.get("flight", "")).strip().upper().replace(" ", "")
    if not _FLIGHT_RE.match(flight):
        raise ValidationError("flight must look like CX123", "flight")
    hotel = str(body.get("hotelId", "")).strip()
    if not _HOTEL_RE.match(hotel):
        raise ValidationError("hotelId must be 2-40 chars [A-Za-z0-9_-]", "hotelId")
    room_ref = str(body.get("roomReservationRef", "")).strip()
    if not _ROOM_REF_RE.match(room_ref):
        raise ValidationError("roomReservationRef must be 1-64 chars [A-Za-z0-9_/-]", "roomReservationRef")
    checkin_text = body.get("checkIn")
    parse_checkin(checkin_text)
    scenario = body.get("scenario", "none") or "none"
    if scenario not in scenarios:
        raise ValidationError(f"unknown scenario {scenario!r}", "scenario")
    return {
        "flight": flight,
        "hotelId": hotel,
        "roomReservationRef": room_ref,
        "checkIn": checkin_text.strip(),
        "scenario": scenario,
    }


def validate_idempotency_key(key) -> str:
    if not isinstance(key, str) or not _IDEM_RE.match(key.strip()):
        raise ValidationError("Idempotency-Key header is required (1-100 chars [A-Za-z0-9._:-])",
                              "Idempotency-Key")
    return key.strip()


# --------------------------------------------------------------------------- misc
def payload_hash(payload) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def new_id() -> str:
    return uuid.uuid4().hex
