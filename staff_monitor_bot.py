from __future__ import annotations

import asyncio
import json
import io
import logging
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

import discord
from aiohttp import web
from discord import app_commands
from discord.ext import commands, tasks

# =========================
# Configuration
# =========================

CONFIG_PATH = Path(os.getenv("STAFF_BOT_CONFIG", "config.json"))
DB_PATH = Path(os.getenv("STAFF_BOT_DB", "staff_monitor.db"))
TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")
BRIDGE_TOKEN = os.getenv("MINECRAFT_BRIDGE_TOKEN", "")
HTTP_HOST = os.getenv("BRIDGE_HOST", "0.0.0.0")
HTTP_PORT = int(os.getenv("BRIDGE_PORT", "8080"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("staff-monitor")


DEFAULT_CONFIG = {
    "guild_id": 0,
    "timezone": "America/Chicago",
    "staff_log_channel_id": 0,
    "weekly_report_channel_id": 0,
    "litebans_webhook_channel_id": 0,
    "loa_request_channel_id": 0,
    "offboard_log_channel_id": 0,
    "role_sync_log_channel_id": 0,
    "reminder_channel_id": 0,
    "reminder_days_before_loa_end": 2,
    "reminder_days_before_strike_expiry": 3,
    "management_ping_target_ids": [
        1016147595128754207,
        1016146618170495077,
        1016145868111482973,
        1016143751799918602
    ],
    "regular_staff_ping_target_ids": [
        1016154034392014869,
        1016151968068141067,
        1016150261342928916
    ],
    "management_role_ids": [],
    "auto_strike_role_ids": [],  # helper / jr mod / mod
    "upper_staff_role_ids": [],  # tracked but exempt from automatic strikes
    "minimum_weekly_hours": 3.0,
    "inactivity_days_for_strike": 3,
    "warning_text": "If you receive further strikes, you may risk getting fired.",
    "strike_decay_days": {
        "weekly_playtime": 14,
        "inactivity": 14,
        "manual_minor": 14,
        "manual_major": 30,
        "manual_severe": 45
    }
}



def _deep_merge_config(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for key, value in base.items():
        if isinstance(value, dict):
            merged[key] = value.copy()
        elif isinstance(value, list):
            merged[key] = value.copy()
        else:
            merged[key] = value
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            child = dict(merged[key])
            child.update(value)
            merged[key] = child
        else:
            merged[key] = value
    return merged


def _normalize_unique_int_list(values: Any) -> list[int]:
    out: list[int] = []
    seen: set[int] = set()
    if not isinstance(values, list):
        return out
    for raw in values:
        try:
            value = int(raw)
        except Exception:
            continue
        if value <= 0 or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def validate_config(config: dict[str, Any]) -> dict[str, Any]:
    required_positive_ints = [
        "guild_id",
        "staff_log_channel_id",
        "weekly_report_channel_id",
        "litebans_webhook_channel_id",
    ]
    for key in required_positive_ints:
        try:
            config[key] = int(config.get(key, 0) or 0)
        except Exception as exc:
            raise ValueError(f"Config value '{key}' must be an integer.") from exc
        if config[key] <= 0:
            raise ValueError(f"Config value '{key}' must be set to a valid Discord ID.")

    optional_zero_or_positive = [
        "loa_request_channel_id",
        "offboard_log_channel_id",
        "role_sync_log_channel_id",
        "reminder_channel_id",
        "no_lower_staff_activity_channel_id",
    ]
    for key in optional_zero_or_positive:
        try:
            config[key] = int(config.get(key, 0) or 0)
        except Exception as exc:
            raise ValueError(f"Config value '{key}' must be an integer.") from exc
        if config[key] < 0:
            raise ValueError(f"Config value '{key}' cannot be negative.")

    timezone_name = str(config.get("timezone", "")).strip()
    if not timezone_name:
        raise ValueError("Config value 'timezone' must be set.")
    ZoneInfo(timezone_name)
    config["timezone"] = timezone_name

    try:
        config["minimum_weekly_hours"] = float(config.get("minimum_weekly_hours", 0))
    except Exception as exc:
        raise ValueError("Config value 'minimum_weekly_hours' must be numeric.") from exc
    if config["minimum_weekly_hours"] < 0:
        raise ValueError("Config value 'minimum_weekly_hours' cannot be negative.")

    try:
        config["inactivity_days_for_strike"] = int(config.get("inactivity_days_for_strike", 0))
    except Exception as exc:
        raise ValueError("Config value 'inactivity_days_for_strike' must be an integer.") from exc
    if config["inactivity_days_for_strike"] < 0:
        raise ValueError("Config value 'inactivity_days_for_strike' cannot be negative.")

    try:
        config["reminder_days_before_loa_end"] = int(config.get("reminder_days_before_loa_end", 2))
    except Exception as exc:
        raise ValueError("Config value 'reminder_days_before_loa_end' must be an integer.") from exc
    if config["reminder_days_before_loa_end"] < 0:
        raise ValueError("Config value 'reminder_days_before_loa_end' cannot be negative.")

    try:
        config["reminder_days_before_strike_expiry"] = int(config.get("reminder_days_before_strike_expiry", 3))
    except Exception as exc:
        raise ValueError("Config value 'reminder_days_before_strike_expiry' must be an integer.") from exc
    if config["reminder_days_before_strike_expiry"] < 0:
        raise ValueError("Config value 'reminder_days_before_strike_expiry' cannot be negative.")

    config["management_role_ids"] = _normalize_unique_int_list(config.get("management_role_ids", []))
    config["auto_strike_role_ids"] = _normalize_unique_int_list(config.get("auto_strike_role_ids", []))
    config["upper_staff_role_ids"] = _normalize_unique_int_list(config.get("upper_staff_role_ids", []))
    config["management_ping_target_ids"] = _normalize_unique_int_list(config.get("management_ping_target_ids", []))
    config["regular_staff_ping_target_ids"] = _normalize_unique_int_list(config.get("regular_staff_ping_target_ids", []))

    decay_cfg = config.get("strike_decay_days", {})
    if not isinstance(decay_cfg, dict):
        decay_cfg = {}
    default_decay = DEFAULT_CONFIG.get("strike_decay_days", {})
    merged_decay = {
        "weekly_playtime": int(decay_cfg.get("weekly_playtime", default_decay.get("weekly_playtime", 14))),
        "inactivity": int(decay_cfg.get("inactivity", default_decay.get("inactivity", 14))),
        "manual_minor": int(decay_cfg.get("manual_minor", decay_cfg.get("minor", default_decay.get("manual_minor", 14)))),
        "manual_major": int(decay_cfg.get("manual_major", decay_cfg.get("major", default_decay.get("manual_major", 30)))),
        "manual_severe": int(decay_cfg.get("manual_severe", decay_cfg.get("severe", default_decay.get("manual_severe", 45)))),
    }
    for decay_key, days in merged_decay.items():
        if days < 0:
            raise ValueError(f"strike_decay_days.{decay_key} cannot be negative.")
    config["strike_decay_days"] = merged_decay

    warning_text = str(config.get("warning_text", DEFAULT_CONFIG["warning_text"])).strip()
    config["warning_text"] = warning_text or DEFAULT_CONFIG["warning_text"]

    return config


def load_config() -> dict[str, Any]:
    if not CONFIG_PATH.exists():
        CONFIG_PATH.write_text(json.dumps(DEFAULT_CONFIG, indent=2), encoding="utf-8")
        return validate_config(DEFAULT_CONFIG.copy())
    data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    merged = _deep_merge_config(DEFAULT_CONFIG, data)
    return validate_config(merged)


CONFIG = load_config()
TZ = ZoneInfo(CONFIG["timezone"])


# =========================
# Database
# =========================


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


SCHEMA = """
CREATE TABLE IF NOT EXISTS staff_members (
    discord_id INTEGER PRIMARY KEY,
    minecraft_name TEXT,
    strike_eligible INTEGER NOT NULL DEFAULT 0,
    notes TEXT,
    last_minecraft_login_at TEXT,
    inactivity_strike_anchor TEXT,
    is_active INTEGER NOT NULL DEFAULT 1,
    offboard_type TEXT,
    offboard_reason TEXT,
    offboard_evidence_url TEXT,
    offboarded_by INTEGER,
    offboarded_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    discord_id INTEGER NOT NULL,
    source TEXT NOT NULL CHECK(source IN ('minecraft', 'discord')),
    started_at TEXT NOT NULL,
    ended_at TEXT,
    FOREIGN KEY(discord_id) REFERENCES staff_members(discord_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS weekly_stats (
    week_key TEXT NOT NULL,
    discord_id INTEGER NOT NULL,
    minecraft_seconds INTEGER NOT NULL DEFAULT 0,
    discord_seconds INTEGER NOT NULL DEFAULT 0,
    discord_warns INTEGER NOT NULL DEFAULT 0,
    discord_kicks INTEGER NOT NULL DEFAULT 0,
    discord_bans INTEGER NOT NULL DEFAULT 0,
    discord_mutes INTEGER NOT NULL DEFAULT 0,
    mc_warns INTEGER NOT NULL DEFAULT 0,
    mc_kicks INTEGER NOT NULL DEFAULT 0,
    mc_bans INTEGER NOT NULL DEFAULT 0,
    mc_mutes INTEGER NOT NULL DEFAULT 0,
    weekly_checked INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (week_key, discord_id),
    FOREIGN KEY(discord_id) REFERENCES staff_members(discord_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS punishments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    discord_id INTEGER NOT NULL,
    moderator_discord_id INTEGER,
    source TEXT NOT NULL CHECK(source IN ('discord', 'minecraft')),
    action_type TEXT NOT NULL CHECK(action_type IN ('warn', 'kick', 'ban', 'mute')),
    target_name TEXT,
    reason TEXT,
    created_at TEXT NOT NULL,
    week_key TEXT NOT NULL,
    FOREIGN KEY(discord_id) REFERENCES staff_members(discord_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS strikes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    discord_id INTEGER NOT NULL,
    amount INTEGER NOT NULL,
    reason TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('manual', 'weekly_playtime', 'inactivity')),
    issued_by INTEGER,
    active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    evidence_url TEXT,
    is_permanent INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY(discord_id) REFERENCES staff_members(discord_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS incident_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subject_discord_id INTEGER,
    severity TEXT NOT NULL,
    summary TEXT NOT NULL,
    details TEXT,
    evidence_url TEXT,
    created_by INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(subject_discord_id) REFERENCES staff_members(discord_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS processed_webhook_messages (
    message_id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pending_dm_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    discord_id INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL,
    sent INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY(discord_id) REFERENCES staff_members(discord_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS loa_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    discord_id INTEGER NOT NULL,
    requested_by INTEGER NOT NULL,
    start_date TEXT NOT NULL,
    end_date TEXT NOT NULL,
    reason TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending', 'approved', 'denied', 'cancelled')),
    reviewer_id INTEGER,
    reviewer_note TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(discord_id) REFERENCES staff_members(discord_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS loa_periods (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    discord_id INTEGER NOT NULL,
    start_date TEXT NOT NULL,
    end_date TEXT NOT NULL,
    reason TEXT NOT NULL,
    approved_by INTEGER,
    source TEXT NOT NULL CHECK(source IN ('request', 'manual')),
    active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    FOREIGN KEY(discord_id) REFERENCES staff_members(discord_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS staff_note_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    discord_id INTEGER NOT NULL,
    action TEXT NOT NULL CHECK(action IN ('set', 'clear')),
    note_text TEXT,
    updated_by INTEGER,
    created_at TEXT NOT NULL,
    FOREIGN KEY(discord_id) REFERENCES staff_members(discord_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS staff_cases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subject_discord_id INTEGER,
    severity TEXT NOT NULL,
    summary TEXT NOT NULL,
    details TEXT,
    status TEXT NOT NULL CHECK(status IN ('open', 'under_review', 'resolved', 'closed')),
    outcome TEXT,
    evidence_url TEXT,
    created_by INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    closed_by INTEGER,
    closed_at TEXT,
    FOREIGN KEY(subject_discord_id) REFERENCES staff_members(discord_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS staff_case_updates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id INTEGER NOT NULL,
    update_text TEXT NOT NULL,
    status_after TEXT,
    evidence_url TEXT,
    updated_by INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(case_id) REFERENCES staff_cases(id) ON DELETE CASCADE
);
"""


def init_db() -> None:
    with get_db() as conn:
        conn.executescript(SCHEMA)
        try:
            conn.execute("ALTER TABLE strikes ADD COLUMN evidence_url TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE strikes ADD COLUMN is_permanent INTEGER NOT NULL DEFAULT 0")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE staff_members ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE staff_members ADD COLUMN offboard_type TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE staff_members ADD COLUMN offboard_reason TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE staff_members ADD COLUMN offboard_evidence_url TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE staff_members ADD COLUMN offboarded_by INTEGER")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE staff_members ADD COLUMN offboarded_at TEXT")
        except sqlite3.OperationalError:
            pass


# =========================
# Time helpers
# =========================


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def dt_to_str(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def str_to_dt(value: str | None) -> Optional[datetime]:
    if not value:
        return None
    return datetime.fromisoformat(value)


# Weeks reset every Sunday at 6:00 PM America/Chicago.
def current_week_start(now: Optional[datetime] = None) -> datetime:
    now = now or utcnow()
    local = now.astimezone(TZ)
    days_since_sunday = (local.weekday() + 1) % 7  # Mon=0 ... Sun=6 => Sun => 0
    candidate_date = (local - timedelta(days=days_since_sunday)).date()
    reset_dt = datetime(candidate_date.year, candidate_date.month, candidate_date.day, 18, 0, 0, tzinfo=TZ)
    if local < reset_dt:
        reset_dt -= timedelta(days=7)
    return reset_dt.astimezone(timezone.utc)


def week_key_for(now: Optional[datetime] = None) -> str:
    start = current_week_start(now)
    return start.astimezone(TZ).strftime("%Y-%m-%d_%H-%M")


def previous_week_key(now: Optional[datetime] = None) -> str:
    current = current_week_start(now)
    prev = current - timedelta(days=7)
    return prev.astimezone(TZ).strftime("%Y-%m-%d_%H-%M")


def next_reset_after(now: Optional[datetime] = None) -> datetime:
    return current_week_start(now) + timedelta(days=7)


def human_hours(seconds: int) -> str:
    total_seconds = max(0, int(seconds))
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    return f"{hours}h {minutes:02d}m"


def format_loa_row(row: sqlite3.Row | None) -> str:
    if not row:
        return "None"
    return f"{row['start_date']} → {row['end_date']}"


def status_emoji(status: str) -> str:
    return {
        "Excellent": "🟪",
        "Excused": "🟦",
        "Good Standing": "🟩",
        "Watchlist": "🟨",
        "Needs Review": "🟥",
        "Tracked": "⬜",
    }.get(status, "⬜")


SEVERITY_AMOUNTS = {
    "minor": 1,
    "major": 2,
    "severe": 3,
}

STRIKE_REASON_LABELS = {
    "activity_idling": "Activity / Idling",
    "professionalism": "Professionalism",
    "communication": "Communication",
    "judgment": "Poor Judgment",
    "disrespect": "Disrespect",
    "policy_violation": "Policy Violation",
    "false_punishment": "False Punishment",
    "abuse_of_power": "Abuse of Power",
    "insubordination": "Insubordination",
    "other": "Other",
}


def parse_date_input(value: str) -> date:
    return datetime.strptime(value.strip(), "%Y-%m-%d").date()


def local_today() -> date:
    return utcnow().astimezone(TZ).date()


def week_bounds_from_key(wk: str) -> tuple[date, date]:
    start_local = datetime.strptime(wk, "%Y-%m-%d_%H-%M").replace(tzinfo=TZ)
    start_date = start_local.date()
    end_date = start_date + timedelta(days=6)
    return start_date, end_date


def overlap_dates(start_a: date, end_a: date, start_b: date, end_b: date) -> bool:
    return max(start_a, start_b) <= min(end_a, end_b)


def format_manual_strike_reason(severity: str, reason_code: str, details: str | None) -> str:
    label = STRIKE_REASON_LABELS.get(reason_code, "Other")
    base_reason = f"[{severity.upper()}] {label}"
    if details and details.strip():
        return f"{base_reason} — {details.strip()}"
    return base_reason


def strike_decay_days_for(kind: str, reason: str) -> int:
    if kind != "manual":
        return int(CONFIG.get("strike_decay_days", {}).get(kind, 0) or 0)
    lower = (reason or "").lower()
    if lower.startswith("[minor]"):
        return int(CONFIG.get("strike_decay_days", {}).get("manual_minor", 0) or 0)
    if lower.startswith("[major]"):
        return int(CONFIG.get("strike_decay_days", {}).get("manual_major", 0) or 0)
    if lower.startswith("[severe]"):
        return int(CONFIG.get("strike_decay_days", {}).get("manual_severe", 0) or 0)
    return 0


def recent_week_keys(limit: int = 4) -> list[str]:
    keys: list[str] = []
    cursor = current_week_start()
    for _ in range(limit):
        keys.append(cursor.astimezone(TZ).strftime("%Y-%m-%d_%H-%M"))
        cursor -= timedelta(days=7)
    return keys


def fetch_member_week_rows(discord_id: int, limit: int = 4) -> list[sqlite3.Row]:
    keys = recent_week_keys(limit)
    placeholders = ",".join("?" for _ in keys)
    with get_db() as conn:
        rows = conn.execute(
            f"""
            SELECT ws.*, sm.strike_eligible
            FROM weekly_stats ws
            JOIN staff_members sm ON sm.discord_id = ws.discord_id
            WHERE ws.discord_id = ? AND ws.week_key IN ({placeholders})
            ORDER BY ws.week_key DESC
            """,
            (discord_id, *keys),
        ).fetchall()
        by_key = {str(r["week_key"]): r for r in rows}
        return [by_key[k] for k in keys if k in by_key]


def recommendation_snapshot(discord_id: int, mc_seconds: int, strike_eligible: bool, wk: str) -> tuple[str, str]:
    if has_loa_overlap_week(discord_id, wk):
        return ("Excused", "Approved LOA overlaps this review period.")

    strikes = active_strike_count(discord_id)
    minimum_seconds = int(float(CONFIG["minimum_weekly_hours"]) * 3600)
    recent_rows = fetch_member_week_rows(discord_id, 4)

    met_minimum_recent = 0
    for row in recent_rows:
        if int(row["minecraft_seconds"]) >= minimum_seconds:
            met_minimum_recent += 1

    total_punishments = 0
    if recent_rows:
        latest = recent_rows[0]
        total_punishments = (
            int(latest["discord_warns"]) + int(latest["discord_kicks"]) + int(latest["discord_bans"]) + int(latest["discord_mutes"]) +
            int(latest["mc_warns"]) + int(latest["mc_kicks"]) + int(latest["mc_bans"]) + int(latest["mc_mutes"])
        )

    if strikes >= 3:
        return ("Needs Review", "Three or more active strikes.")
    if strike_eligible and mc_seconds < int(minimum_seconds * 0.5):
        return ("Needs Review", "Weekly playtime is under half of the requirement.")
    if strikes >= 1:
        return ("Watchlist", "Active strikes are on record.")
    if strike_eligible and mc_seconds < minimum_seconds:
        return ("Watchlist", "Weekly playtime is below the requirement.")
    if mc_seconds >= minimum_seconds and met_minimum_recent >= 3 and total_punishments >= 1 and strikes == 0:
        return ("Excellent", "Strong recent consistency, requirement met, and clean strike record.")
    return ("Good Standing", "Meeting expectations with no active concerns.")


def status_label_for(discord_id: int, mc_seconds: int, strike_eligible: bool, wk: str) -> str:
    return recommendation_snapshot(discord_id, mc_seconds, strike_eligible, wk)[0]


def recommendation_reason_for(discord_id: int, mc_seconds: int, strike_eligible: bool, wk: str) -> str:
    return recommendation_snapshot(discord_id, mc_seconds, strike_eligible, wk)[1]


# =========================
# Data access
# =========================


def upsert_staff_member(discord_id: int, minecraft_name: str | None, strike_eligible: bool, notes: str | None = None) -> None:
    now = dt_to_str(utcnow())
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO staff_members (
                discord_id, minecraft_name, strike_eligible, notes, is_active,
                offboard_type, offboard_reason, offboard_evidence_url, offboarded_by, offboarded_at,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, 1, NULL, NULL, NULL, NULL, NULL, ?, ?)
            ON CONFLICT(discord_id) DO UPDATE SET
                minecraft_name = excluded.minecraft_name,
                strike_eligible = excluded.strike_eligible,
                notes = COALESCE(excluded.notes, staff_members.notes),
                is_active = 1,
                offboard_type = NULL,
                offboard_reason = NULL,
                offboard_evidence_url = NULL,
                offboarded_by = NULL,
                offboarded_at = NULL,
                updated_at = excluded.updated_at
            """,
            (discord_id, minecraft_name, 1 if strike_eligible else 0, notes, now, now),
        )


def get_staff_member(discord_id: int) -> Optional[sqlite3.Row]:
    with get_db() as conn:
        return conn.execute("SELECT * FROM staff_members WHERE discord_id = ?", (discord_id,)).fetchone()

def list_staff_members(*, include_inactive: bool = False) -> list[sqlite3.Row]:
    with get_db() as conn:
        if include_inactive:
            return conn.execute(
                "SELECT * FROM staff_members ORDER BY created_at ASC, discord_id ASC"
            ).fetchall()
        return conn.execute(
            "SELECT * FROM staff_members WHERE COALESCE(is_active, 1) = 1 ORDER BY created_at ASC, discord_id ASC"
        ).fetchall()


def is_staff_active_row(row: sqlite3.Row | None) -> bool:
    return bool(row) and int(row["is_active"] if "is_active" in row.keys() else 1) == 1


def offboard_staff_member(
    discord_id: int,
    *,
    offboard_type: str,
    reason: str,
    acted_by: int | None,
    evidence_url: str | None = None,
) -> Optional[sqlite3.Row]:
    now = dt_to_str(utcnow())
    with get_db() as conn:
        row = conn.execute("SELECT * FROM staff_members WHERE discord_id = ?", (discord_id,)).fetchone()
        if not row:
            return None
        conn.execute(
            """
            UPDATE staff_members
            SET is_active = 0,
                strike_eligible = 0,
                offboard_type = ?,
                offboard_reason = ?,
                offboard_evidence_url = ?,
                offboarded_by = ?,
                offboarded_at = ?,
                updated_at = ?
            WHERE discord_id = ?
            """,
            (offboard_type, reason, evidence_url, acted_by, now, now, discord_id),
        )
        conn.execute(
            "UPDATE sessions SET ended_at = ? WHERE discord_id = ? AND ended_at IS NULL",
            (now, discord_id),
        )
        return conn.execute("SELECT * FROM staff_members WHERE discord_id = ?", (discord_id,)).fetchone()


def reinstate_staff_member(discord_id: int, *, acted_by: int | None = None) -> Optional[sqlite3.Row]:
    now = dt_to_str(utcnow())
    with get_db() as conn:
        row = conn.execute("SELECT * FROM staff_members WHERE discord_id = ?", (discord_id,)).fetchone()
        if not row:
            return None
        conn.execute(
            """
            UPDATE staff_members
            SET is_active = 1,
                offboard_type = NULL,
                offboard_reason = NULL,
                offboard_evidence_url = NULL,
                offboarded_by = NULL,
                offboarded_at = NULL,
                updated_at = ?
            WHERE discord_id = ?
            """,
            (now, discord_id),
        )
        return conn.execute("SELECT * FROM staff_members WHERE discord_id = ?", (discord_id,)).fetchone()


def resolve_staff_by_mc_name(minecraft_name: str) -> Optional[sqlite3.Row]:
    with get_db() as conn:
        return conn.execute(
            "SELECT * FROM staff_members WHERE lower(minecraft_name) = lower(?) AND COALESCE(is_active, 1) = 1",
            (minecraft_name,),
        ).fetchone()


def has_processed_webhook_message(message_id: int) -> bool:
    with get_db() as conn:
        row = conn.execute(
            "SELECT 1 FROM processed_webhook_messages WHERE message_id = ?",
            (message_id,),
        ).fetchone()
        return row is not None


def mark_processed_webhook_message(message_id: int, source: str = "litebans") -> None:
    with get_db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO processed_webhook_messages (message_id, source, created_at) VALUES (?, ?, ?)",
            (message_id, source, dt_to_str(utcnow())),
        )


MC_NAME_PATTERN = r"[A-Za-z0-9_]{2,16}"


def normalize_ws(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def build_message_blob(message: discord.Message) -> str:
    parts: list[str] = []
    if message.content:
        parts.append(message.content)
    for embed in message.embeds:
        if embed.title:
            parts.append(embed.title)
        if embed.description:
            parts.append(embed.description)
        if embed.author and embed.author.name:
            parts.append(embed.author.name)
        if embed.footer and embed.footer.text:
            parts.append(embed.footer.text)
        for field in embed.fields:
            parts.append(field.name)
            parts.append(field.value)
    return normalize_ws(" ".join(parts))


def extract_action_type_from_blob(blob: str) -> Optional[str]:
    lower = blob.lower()
    if re.search(r"\bunwarn(?:ed)?\b", lower):
        return None
    if re.search(r"\bunmute(?:d)?\b", lower):
        return None
    if re.search(r"\bunban(?:ned)?\b", lower):
        return None

    patterns = [
        ("warn", r"\b(?:warn|warned|warning)\b"),
        ("kick", r"\b(?:kick|kicked)\b"),
        ("mute", r"\b(?:mute|muted|ipmute|temp mute|temporary mute)\b"),
        ("ban", r"\b(?:ban|banned|ipban|temp ban|temporary ban)\b"),
    ]
    for action_type, pattern in patterns:
        if re.search(pattern, lower):
            return action_type
    return None


def extract_executor_minecraft_name(message: discord.Message, blob: str) -> Optional[str]:
    for embed in message.embeds:
        for field in embed.fields:
            label = (field.name or "").lower()
            if any(key in label for key in ("executor", "staff", "moderator", "punisher", "by")):
                match = re.search(MC_NAME_PATTERN, field.value or "")
                if match:
                    return match.group(0)

    patterns = [
        rf"\bexecutor\b[^A-Za-z0-9_]{{0,20}}({MC_NAME_PATTERN})",
        rf"\bmoderator\b[^A-Za-z0-9_]{{0,20}}({MC_NAME_PATTERN})",
        rf"\bstaff\b[^A-Za-z0-9_]{{0,20}}({MC_NAME_PATTERN})",
        rf"\bby\b[^A-Za-z0-9_]{{1,20}}({MC_NAME_PATTERN})",
    ]
    for pattern in patterns:
        match = re.search(pattern, blob, flags=re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def extract_target_name(message: discord.Message, blob: str, executor_name: str | None) -> Optional[str]:
    for embed in message.embeds:
        for field in embed.fields:
            label = (field.name or "").lower()
            if any(key in label for key in ("player", "target", "user", "member", "punished")):
                match = re.search(MC_NAME_PATTERN, field.value or "")
                if match:
                    candidate = match.group(0)
                    if not executor_name or candidate.lower() != executor_name.lower():
                        return candidate

    patterns = [
        rf"\bplayer\b[^A-Za-z0-9_]{{0,20}}({MC_NAME_PATTERN})",
        rf"\btarget\b[^A-Za-z0-9_]{{0,20}}({MC_NAME_PATTERN})",
        rf"\buser\b[^A-Za-z0-9_]{{0,20}}({MC_NAME_PATTERN})",
        rf"\b(?:warned|kicked|muted|banned)\b[^A-Za-z0-9_]{{0,20}}({MC_NAME_PATTERN})",
    ]
    for pattern in patterns:
        match = re.search(pattern, blob, flags=re.IGNORECASE)
        if match:
            candidate = match.group(1)
            if not executor_name or candidate.lower() != executor_name.lower():
                return candidate
    return None


def extract_reason(message: discord.Message, blob: str) -> Optional[str]:
    for embed in message.embeds:
        for field in embed.fields:
            label = (field.name or "").lower()
            if "reason" in label:
                value = normalize_ws(field.value)
                return value[:500] if value else None

    match = re.search(r"\breason\b[^A-Za-z0-9_]{0,10}(.+)", blob, flags=re.IGNORECASE)
    if match:
        value = normalize_ws(match.group(1))
        return value[:500] if value else None
    return None


def ensure_week_row(discord_id: int, wk: Optional[str] = None) -> None:
    wk = wk or week_key_for()
    with get_db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO weekly_stats (week_key, discord_id) VALUES (?, ?)",
            (wk, discord_id),
        )


def add_playtime_seconds(discord_id: int, seconds: int, source: str, reference_time: Optional[datetime] = None) -> None:
    wk = week_key_for(reference_time)
    ensure_week_row(discord_id, wk)
    field = "minecraft_seconds" if source == "minecraft" else "discord_seconds"
    with get_db() as conn:
        conn.execute(
            f"UPDATE weekly_stats SET {field} = {field} + ? WHERE week_key = ? AND discord_id = ?",
            (seconds, wk, discord_id),
        )


def update_last_minecraft_login(discord_id: int, when: Optional[datetime] = None) -> None:
    when = when or utcnow()
    with get_db() as conn:
        conn.execute(
            "UPDATE staff_members SET last_minecraft_login_at = ?, inactivity_strike_anchor = NULL, updated_at = ? WHERE discord_id = ?",
            (dt_to_str(when), dt_to_str(when), discord_id),
        )


def open_session(discord_id: int, source: str) -> None:
    with get_db() as conn:
        existing = conn.execute(
            "SELECT id FROM sessions WHERE discord_id = ? AND source = ? AND ended_at IS NULL",
            (discord_id, source),
        ).fetchone()
        if existing:
            return
        conn.execute(
            "INSERT INTO sessions (discord_id, source, started_at) VALUES (?, ?, ?)",
            (discord_id, source, dt_to_str(utcnow())),
        )
    if source == "minecraft":
        update_last_minecraft_login(discord_id)


def close_session(discord_id: int, source: str) -> int:
    now = utcnow()
    with get_db() as conn:
        row = conn.execute(
            "SELECT id, started_at FROM sessions WHERE discord_id = ? AND source = ? AND ended_at IS NULL ORDER BY id DESC LIMIT 1",
            (discord_id, source),
        ).fetchone()
        if not row:
            return 0
        started = str_to_dt(row["started_at"])
        if not started:
            return 0
        seconds = max(0, int((now - started).total_seconds()))
        conn.execute("UPDATE sessions SET ended_at = ? WHERE id = ?", (dt_to_str(now), row["id"]))
    add_playtime_seconds(discord_id, seconds, source, now)
    return seconds


def close_all_open_sessions_for_member(discord_id: int) -> dict[str, int]:
    totals = {}
    for source in ("minecraft", "discord"):
        totals[source] = close_session(discord_id, source)
    return totals


def increment_punishment(staff_discord_id: int, source: str, action_type: str, moderator_discord_id: int | None, target_name: str | None, reason: str | None) -> None:
    wk = week_key_for()
    ensure_week_row(staff_discord_id, wk)
    field_map = {
        ("discord", "warn"): "discord_warns",
        ("discord", "kick"): "discord_kicks",
        ("discord", "ban"): "discord_bans",
        ("discord", "mute"): "discord_mutes",
        ("minecraft", "warn"): "mc_warns",
        ("minecraft", "kick"): "mc_kicks",
        ("minecraft", "ban"): "mc_bans",
        ("minecraft", "mute"): "mc_mutes",
    }
    field = field_map[(source, action_type)]
    now = dt_to_str(utcnow())
    with get_db() as conn:
        conn.execute(
            f"UPDATE weekly_stats SET {field} = {field} + 1 WHERE week_key = ? AND discord_id = ?",
            (wk, staff_discord_id),
        )
        conn.execute(
            """
            INSERT INTO punishments (discord_id, moderator_discord_id, source, action_type, target_name, reason, created_at, week_key)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (staff_discord_id, moderator_discord_id, source, action_type, target_name, reason, now, wk),
        )


def active_strike_count(discord_id: int) -> int:
    expire_decayed_strikes()
    with get_db() as conn:
        row = conn.execute(
            "SELECT COALESCE(SUM(amount), 0) AS total FROM strikes WHERE discord_id = ? AND active = 1",
            (discord_id,),
        ).fetchone()
        return int(row["total"] if row else 0)


def add_strike(
    discord_id: int,
    amount: int,
    reason: str,
    kind: str,
    issued_by: int | None = None,
    evidence_url: str | None = None,
    is_permanent: bool = False,
) -> int:
    now = dt_to_str(utcnow())
    with get_db() as conn:
        conn.execute(
            "INSERT INTO strikes (discord_id, amount, reason, kind, issued_by, created_at, evidence_url, is_permanent) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (discord_id, amount, reason, kind, issued_by, now, evidence_url, 1 if is_permanent else 0),
        )
    return active_strike_count(discord_id)


def add_incident_log(
    subject_discord_id: int | None,
    severity: str,
    summary: str,
    details: str | None,
    evidence_url: str | None,
    created_by: int,
) -> int:
    now = dt_to_str(utcnow())
    with get_db() as conn:
        cur = conn.execute(
            "INSERT INTO incident_logs (subject_discord_id, severity, summary, details, evidence_url, created_by, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (subject_discord_id, severity, summary, (details or None), (evidence_url or None), created_by, now),
        )
        return int(cur.lastrowid)


def get_incident_log(incident_id: int) -> sqlite3.Row | None:
    with get_db() as conn:
        return conn.execute("SELECT * FROM incident_logs WHERE id = ?", (incident_id,)).fetchone()


def remove_incident_log(incident_id: int) -> sqlite3.Row | None:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM incident_logs WHERE id = ?", (incident_id,)).fetchone()
        if row:
            conn.execute("DELETE FROM incident_logs WHERE id = ?", (incident_id,))
        return row


def remove_strikes(discord_id: int, amount: int) -> int:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, amount FROM strikes WHERE discord_id = ? AND active = 1 ORDER BY created_at DESC, id DESC",
            (discord_id,),
        ).fetchall()
        remaining = amount
        for row in rows:
            if remaining <= 0:
                break
            strike_amount = int(row["amount"])
            if strike_amount <= remaining:
                conn.execute("UPDATE strikes SET active = 0 WHERE id = ?", (row["id"],))
                remaining -= strike_amount
            else:
                conn.execute("UPDATE strikes SET amount = ? WHERE id = ?", (strike_amount - remaining, row["id"]))
                remaining = 0
    return active_strike_count(discord_id)


def fetch_weekly_stats(discord_id: int, wk: Optional[str] = None) -> sqlite3.Row | None:
    wk = wk or week_key_for()
    ensure_week_row(discord_id, wk)
    with get_db() as conn:
        return conn.execute(
            "SELECT * FROM weekly_stats WHERE week_key = ? AND discord_id = ?",
            (wk, discord_id),
        ).fetchone()


def fetch_all_previous_week_candidates(wk: str) -> list[sqlite3.Row]:
    with get_db() as conn:
        return conn.execute(
            """
            SELECT ws.*, sm.minecraft_name, sm.strike_eligible
            FROM weekly_stats ws
            JOIN staff_members sm ON sm.discord_id = ws.discord_id
            WHERE ws.week_key = ?
            """,
            (wk,),
        ).fetchall()


def mark_week_checked(discord_id: int, wk: str) -> None:
    with get_db() as conn:
        conn.execute(
            "UPDATE weekly_stats SET weekly_checked = 1 WHERE week_key = ? AND discord_id = ?",
            (wk, discord_id),
        )


def set_inactivity_anchor(discord_id: int, anchor: datetime) -> None:
    with get_db() as conn:
        conn.execute(
            "UPDATE staff_members SET inactivity_strike_anchor = ?, updated_at = ? WHERE discord_id = ?",
            (dt_to_str(anchor), dt_to_str(anchor), discord_id),
        )


def get_open_session_members(source: str) -> list[int]:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT DISTINCT discord_id FROM sessions WHERE source = ? AND ended_at IS NULL",
            (source,),
        ).fetchall()
        return [int(r["discord_id"]) for r in rows]


def get_registered_staff() -> list[sqlite3.Row]:
    with get_db() as conn:
        return conn.execute("SELECT * FROM staff_members ORDER BY discord_id").fetchall()



def set_staff_notes(discord_id: int, notes: str | None, updated_by: int | None = None) -> None:
    now = dt_to_str(utcnow())
    cleaned = (notes or "").strip() or None
    with get_db() as conn:
        conn.execute(
            "UPDATE staff_members SET notes = ?, updated_at = ? WHERE discord_id = ?",
            (cleaned, now, discord_id),
        )
        conn.execute(
            "INSERT INTO staff_note_history (discord_id, action, note_text, updated_by, created_at) VALUES (?, ?, ?, ?, ?)",
            (discord_id, "set" if cleaned else "clear", cleaned, updated_by, now),
        )




def list_recent_strike_history(discord_id: int, limit: int = 5) -> list[sqlite3.Row]:
    expire_decayed_strikes()
    with get_db() as conn:
        return conn.execute(
            "SELECT * FROM strikes WHERE discord_id = ? ORDER BY created_at DESC, id DESC LIMIT ?",
            (discord_id, limit),
        ).fetchall()


def create_case(subject_discord_id: int | None, severity: str, summary: str, details: str, evidence_url: str | None, created_by: int) -> int:
    now = dt_to_str(utcnow())
    with get_db() as conn:
        cur = conn.execute(
            """
            INSERT INTO staff_cases(subject_discord_id, severity, summary, details, status, outcome, evidence_url, created_by, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'open', NULL, ?, ?, ?, ?)
            """,
            (subject_discord_id, severity, summary, details, evidence_url, created_by, now, now),
        )
        return int(cur.lastrowid)


def get_case(case_id: int) -> Optional[sqlite3.Row]:
    with get_db() as conn:
        return conn.execute("SELECT * FROM staff_cases WHERE id = ?", (case_id,)).fetchone()


def add_case_update(case_id: int, update_text: str, updated_by: int, status_after: str | None = None, evidence_url: str | None = None) -> int:
    now = dt_to_str(utcnow())
    with get_db() as conn:
        cur = conn.execute(
            """
            INSERT INTO staff_case_updates(case_id, update_text, status_after, evidence_url, updated_by, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (case_id, update_text, status_after, evidence_url, updated_by, now),
        )
        if status_after:
            conn.execute("UPDATE staff_cases SET status = ?, updated_at = ? WHERE id = ?", (status_after, now, case_id))
        else:
            conn.execute("UPDATE staff_cases SET updated_at = ? WHERE id = ?", (now, case_id))
        return int(cur.lastrowid)


def close_case(case_id: int, outcome: str, closed_by: int, evidence_url: str | None = None) -> Optional[sqlite3.Row]:
    now = dt_to_str(utcnow())
    with get_db() as conn:
        row = conn.execute("SELECT * FROM staff_cases WHERE id = ?", (case_id,)).fetchone()
        if not row:
            return None
        conn.execute(
            "UPDATE staff_cases SET status = 'closed', outcome = ?, closed_by = ?, closed_at = ?, updated_at = ?, evidence_url = COALESCE(?, evidence_url) WHERE id = ?",
            (outcome, closed_by, now, now, evidence_url, case_id),
        )
        conn.execute(
            "INSERT INTO staff_case_updates(case_id, update_text, status_after, evidence_url, updated_by, created_at) VALUES (?, ?, 'closed', ?, ?, ?)",
            (case_id, outcome, evidence_url, closed_by, now),
        )
        return conn.execute("SELECT * FROM staff_cases WHERE id = ?", (case_id,)).fetchone()


def list_case_updates(case_id: int, limit: int = 10) -> list[sqlite3.Row]:
    with get_db() as conn:
        return conn.execute(
            "SELECT * FROM staff_case_updates WHERE case_id = ? ORDER BY created_at DESC, id DESC LIMIT ?",
            (case_id, limit),
        ).fetchall()


def list_cases_for_member(discord_id: int, limit: int = 10) -> list[sqlite3.Row]:
    with get_db() as conn:
        return conn.execute(
            "SELECT * FROM staff_cases WHERE subject_discord_id = ? ORDER BY updated_at DESC, id DESC LIMIT ?",
            (discord_id, limit),
        ).fetchall()


def build_case_embed(case_id: int) -> discord.Embed | None:
    row = get_case(case_id)
    if not row:
        return None
    colour_map = {
        'open': discord.Colour.orange(),
        'under_review': discord.Colour.gold(),
        'resolved': discord.Colour.green(),
        'closed': discord.Colour.dark_grey(),
    }
    embed = discord.Embed(title=f"Staff Case #{case_id}", colour=colour_map.get(str(row['status']), discord.Colour.blurple()))
    embed.add_field(name='Status', value=str(row['status']).replace('_', ' ').title(), inline=True)
    embed.add_field(name='Severity', value=str(row['severity']).title(), inline=True)
    embed.add_field(name='Opened By', value=f"<@{int(row['created_by'])}>", inline=True)
    if row['subject_discord_id']:
        embed.add_field(name='Staff', value=f"<@{int(row['subject_discord_id'])}>", inline=False)
    embed.add_field(name='Summary', value=str(row['summary'])[:1024], inline=False)
    if row['details']:
        embed.add_field(name='Details', value=str(row['details'])[:1024], inline=False)
    if row['outcome']:
        embed.add_field(name='Outcome', value=str(row['outcome'])[:1024], inline=False)
    if row['evidence_url']:
        embed.add_field(name='Evidence', value=str(row['evidence_url'])[:1024], inline=False)
    updates = list_case_updates(case_id, limit=5)
    if updates:
        lines=[]
        for u in updates[:5]:
            stamp = format_history_dt(u['created_at'])
            status_after = f" → {str(u['status_after']).replace('_',' ').title()}" if u['status_after'] else ''
            lines.append(f"• `{stamp}` by <@{int(u['updated_by'])}>{status_after}: {str(u['update_text'])[:120]}")
        embed.add_field(name='Recent Updates', value='\n'.join(lines), inline=False)
    embed.set_footer(text=f"Created {format_history_dt(row['created_at'])}")
    return embed


class CaseLogView(discord.ui.View):
    def __init__(self, case_id: int, *, discord_id: int | None = None, evidence_url: str | None = None, show_open_case: bool = True):
        super().__init__(timeout=None)
        self.case_id = case_id
        self.discord_id = discord_id
        if not show_open_case:
            self.remove_item(self.open_case)
        if discord_id is None:
            self.remove_item(self.view_stats)
        if evidence_url:
            self.add_item(discord.ui.Button(label='Open Evidence', style=discord.ButtonStyle.link, url=evidence_url))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not interaction.guild or not isinstance(interaction.user, discord.Member) or not is_allowed_guild(interaction.guild):
            await interaction.response.send_message('This button can only be used in the target server.', ephemeral=True)
            return False
        if not is_management(interaction.user):
            await interaction.response.send_message('You do not have permission to use this button.', ephemeral=True)
            return False
        return True

    @discord.ui.button(label='Open Case', style=discord.ButtonStyle.primary)
    async def open_case(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        embed = build_case_embed(self.case_id)
        if not embed:
            await interaction.response.send_message('That case could not be found.', ephemeral=True)
            return
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label='View Stats', style=discord.ButtonStyle.secondary)
    async def view_stats(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if self.discord_id is None:
            await interaction.response.send_message('This case is not linked to a staff member.', ephemeral=True)
            return
        member = interaction.guild.get_member(self.discord_id) if interaction.guild else None
        user_obj: discord.abc.User | None = member or bot.get_user(self.discord_id)
        if user_obj is None:
            try:
                user_obj = await bot.fetch_user(self.discord_id)
            except Exception:
                user_obj = None
        if user_obj is None:
            await interaction.response.send_message('Could not find that staff member.', ephemeral=True)
            return
        embed = build_staff_stats_embed(user_obj)
        if not embed:
            await interaction.response.send_message('That user is not registered in the tracker.', ephemeral=True)
            return
        await interaction.response.send_message(embed=embed, ephemeral=True)


def list_recent_incident_history(discord_id: int, limit: int = 5) -> list[sqlite3.Row]:
    with get_db() as conn:
        return conn.execute(
            "SELECT * FROM incident_logs WHERE subject_discord_id = ? ORDER BY created_at DESC, id DESC LIMIT ?",
            (discord_id, limit),
        ).fetchall()


def list_recent_loa_history(discord_id: int, limit: int = 5) -> list[sqlite3.Row]:
    with get_db() as conn:
        return conn.execute(
            "SELECT * FROM loa_periods WHERE discord_id = ? ORDER BY created_at DESC, id DESC LIMIT ?",
            (discord_id, limit),
        ).fetchall()


def list_recent_note_history(discord_id: int, limit: int = 5) -> list[sqlite3.Row]:
    with get_db() as conn:
        return conn.execute(
            "SELECT * FROM staff_note_history WHERE discord_id = ? ORDER BY created_at DESC, id DESC LIMIT ?",
            (discord_id, limit),
        ).fetchall()


def count_permanent_active_strikes(discord_id: int) -> int:
    expire_decayed_strikes()
    with get_db() as conn:
        row = conn.execute(
            "SELECT COALESCE(SUM(amount), 0) AS total FROM strikes WHERE discord_id = ? AND active = 1 AND is_permanent = 1",
            (discord_id,),
        ).fetchone()
        return int(row["total"] if row else 0)


def total_lifetime_strikes(discord_id: int) -> int:
    with get_db() as conn:
        row = conn.execute(
            "SELECT COALESCE(SUM(amount), 0) AS total FROM strikes WHERE discord_id = ?",
            (discord_id,),
        ).fetchone()
        return int(row["total"] if row else 0)


def role_category_for_member(member: discord.Member, staff: sqlite3.Row) -> str:
    if is_upper_staff(member):
        return "Upper Staff"
    if is_auto_strike_role(member) or bool(staff["strike_eligible"]):
        return "Lower Staff"
    return "Tracked Staff"


def format_history_dt(value: str | None) -> str:
    dt = str_to_dt(value) if value else None
    if not dt:
        return "Unknown"
    return dt.astimezone(TZ).strftime("%Y-%m-%d %I:%M %p")


def build_staff_profile_embed(member: discord.Member) -> discord.Embed | None:
    staff = get_staff_member(member.id)
    if not staff:
        return None

    current_stats = fetch_weekly_stats(member.id)
    current_status = status_label_for(member.id, int(current_stats["minecraft_seconds"]), bool(staff["strike_eligible"]), week_key_for())
    current_reason = recommendation_reason_for(member.id, int(current_stats["minecraft_seconds"]), bool(staff["strike_eligible"]), week_key_for())
    active_loa = get_active_loa_for_member(member.id)
    lifetime_incidents = len(list_recent_incident_history(member.id, limit=999))
    lifetime_loas = len(list_recent_loa_history(member.id, limit=999))
    note_entries = len(list_recent_note_history(member.id, limit=999))

    embed = discord.Embed(title=f"Staff Profile • {member}", colour=discord.Colour.dark_teal())
    embed.add_field(
        name="Identity",
        value=(
            f"**Discord:** {member.mention}\n"
            f"**Minecraft:** {staff['minecraft_name'] or 'Not set'}\n"
            f"**Category:** {role_category_for_member(member, staff)}\n"
            f"**Auto-Strike Eligible:** {'Yes' if bool(staff['strike_eligible']) else 'No'}"
        ),
        inline=False,
    )
    embed.add_field(
        name="Standing",
        value=(
            f"**Status:** {status_emoji(current_status)} {current_status}\n"
            f"**Status Note:** {current_reason}\n"
            f"**Active Strikes:** {active_strike_count(member.id)}\n"
            f"**Permanent Active Strikes:** {count_permanent_active_strikes(member.id)}"
        ),
        inline=False,
    )
    embed.add_field(
        name="Record",
        value=(
            f"**Registered:** {format_history_dt(staff['created_at'])}\n"
            f"**Last MC Login:** {format_history_dt(staff['last_minecraft_login_at'])}\n"
            f"**Lifetime Strikes:** {total_lifetime_strikes(member.id)}\n"
            f"**Current LOA:** {format_loa_row(active_loa)}"
        ),
        inline=False,
    )
    embed.add_field(
        name="Management Summary",
        value=(
            f"**Incident Logs:** {lifetime_incidents}\n"
            f"**LOA Entries:** {lifetime_loas}\n"
            f"**Note Updates:** {note_entries}"
        ),
        inline=True,
    )
    if staff["notes"]:
        embed.add_field(name="Current Management Note", value=str(staff["notes"])[:1024], inline=False)
    return embed


def build_staff_history_embed(member: discord.Member) -> discord.Embed | None:
    staff = get_staff_member(member.id)
    if not staff:
        return None

    strikes = list_recent_strike_history(member.id, limit=4)
    incidents = list_recent_incident_history(member.id, limit=4)
    loas = list_recent_loa_history(member.id, limit=4)
    notes = list_recent_note_history(member.id, limit=4)

    embed = discord.Embed(title=f"Staff History • {member}", colour=discord.Colour.dark_gold())

    if strikes:
        strike_lines = []
        for row in strikes:
            state = "Active" if int(row["active"]) else "Inactive"
            permanent = " • Permanent" if int(row["is_permanent"]) else ""
            strike_lines.append(
                f"`#{row['id']}` +{row['amount']} • {row['kind']} • {row['reason'][:45]} • {state}{permanent} • {format_history_dt(row['created_at'])}"
            )
        embed.add_field(name="Recent Strikes", value="\n".join(strike_lines)[:1024], inline=False)
    else:
        embed.add_field(name="Recent Strikes", value="No strike history.", inline=False)

    if incidents:
        incident_lines = []
        for row in incidents:
            incident_lines.append(
                f"`#{row['id']}` {str(row['severity']).title()} • {str(row['summary'])[:55]} • {format_history_dt(row['created_at'])}"
            )
        embed.add_field(name="Recent Incidents", value="\n".join(incident_lines)[:1024], inline=False)
    else:
        embed.add_field(name="Recent Incidents", value="No incident history.", inline=False)

    if loas:
        loa_lines = []
        for row in loas:
            state = "Active" if int(row["active"]) else "Ended"
            loa_lines.append(
                f"`#{row['id']}` {row['start_date']} → {row['end_date']} • {state} • {str(row['reason'])[:40]}"
            )
        embed.add_field(name="Recent LOA", value="\n".join(loa_lines)[:1024], inline=False)
    else:
        embed.add_field(name="Recent LOA", value="No LOA history.", inline=False)

    if notes:
        note_lines = []
        for row in notes:
            action = "Set" if str(row["action"]) == "set" else "Cleared"
            by = f"<@{row['updated_by']}>" if row["updated_by"] else "System"
            snippet = (row["note_text"] or "No note text")[:32]
            note_lines.append(
                f"`#{row['id']}` {action} by {by} • {snippet} • {format_history_dt(row['created_at'])}"
            )
        embed.add_field(name="Recent Note Updates", value="\n".join(note_lines)[:1024], inline=False)
    else:
        embed.add_field(name="Recent Note Updates", value="No note history.", inline=False)

    return embed


def build_staff_activity_history_embed(member: discord.Member) -> discord.Embed | None:
    staff = get_staff_member(member.id)
    if not staff:
        return None

    rows = fetch_member_week_rows(member.id, 4)
    minimum_seconds = int(float(CONFIG["minimum_weekly_hours"]) * 3600)
    current_stats = fetch_weekly_stats(member.id)
    status = status_label_for(member.id, int(current_stats["minecraft_seconds"]), bool(staff["strike_eligible"]), week_key_for())

    embed = discord.Embed(title=f"Activity History • {member}", colour=discord.Colour.dark_blue())
    if not rows:
        embed.description = "No weekly history exists yet."
        return embed

    lines = []
    total_mc = 0
    total_actions = 0
    met_weeks = 0
    for row in rows:
        week = str(row["week_key"])
        mc = int(row["minecraft_seconds"])
        actions = int(row["discord_warns"]) + int(row["discord_kicks"]) + int(row["discord_bans"]) + int(row["discord_mutes"]) + int(row["mc_warns"]) + int(row["mc_kicks"]) + int(row["mc_bans"]) + int(row["mc_mutes"])
        total_mc += mc
        total_actions += actions
        met = mc >= minimum_seconds
        if met:
            met_weeks += 1
        loa = " • Excused" if has_loa_overlap_week(member.id, week) else ""
        lines.append(f"`{week}` • MC {human_hours(mc)} • Actions {actions} • Req {'✅' if met else '❌'}{loa}")

    average_mc = total_mc // max(len(rows), 1)
    average_actions = total_actions / max(len(rows), 1)
    embed.add_field(name="Recent Weeks", value="\n".join(lines)[:1024], inline=False)
    embed.add_field(
        name="Trend Summary",
        value=(
            f"**Average MC Playtime:** {human_hours(average_mc)}\n"
            f"**Average Actions:** {average_actions:.1f}\n"
            f"**Weeks Meeting Requirement:** {met_weeks}/{len(rows)}"
        ),
        inline=False,
    )
    embed.add_field(
        name="Current Standing",
        value=(
            f"**Current Status:** {status_emoji(status)} {status}\n"
            f"**Current Active Strikes:** {active_strike_count(member.id)}\n"
            f"**Current Requirement:** {CONFIG['minimum_weekly_hours']}h"
        ),
        inline=False,
    )
    return embed


def list_active_strikes(discord_id: int) -> list[sqlite3.Row]:
    with get_db() as conn:
        return conn.execute(
            "SELECT * FROM strikes WHERE discord_id = ? AND active = 1 ORDER BY created_at DESC, id DESC",
            (discord_id,),
        ).fetchall()


def create_loa_request(discord_id: int, requested_by: int, start_date: date, end_date: date, reason: str) -> int:
    now = dt_to_str(utcnow())
    with get_db() as conn:
        cur = conn.execute(
            """
            INSERT INTO loa_requests (discord_id, requested_by, start_date, end_date, reason, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
            """,
            (discord_id, requested_by, start_date.isoformat(), end_date.isoformat(), reason, now, now),
        )
        return int(cur.lastrowid)


def get_loa_request(request_id: int) -> Optional[sqlite3.Row]:
    with get_db() as conn:
        return conn.execute("SELECT * FROM loa_requests WHERE id = ?", (request_id,)).fetchone()


def list_loa_requests(status: str | None = None) -> list[sqlite3.Row]:
    with get_db() as conn:
        if status and status != "all":
            return conn.execute(
                "SELECT * FROM loa_requests WHERE status = ? ORDER BY created_at DESC, id DESC",
                (status,),
            ).fetchall()
        return conn.execute(
            "SELECT * FROM loa_requests ORDER BY created_at DESC, id DESC"
        ).fetchall()


def add_loa_period(discord_id: int, start_date: date, end_date: date, reason: str, approved_by: int | None, source: str) -> int:
    with get_db() as conn:
        cur = conn.execute(
            """
            INSERT INTO loa_periods (discord_id, start_date, end_date, reason, approved_by, source, active, created_at)
            VALUES (?, ?, ?, ?, ?, ?, 1, ?)
            """,
            (discord_id, start_date.isoformat(), end_date.isoformat(), reason, approved_by, source, dt_to_str(utcnow())),
        )
        return int(cur.lastrowid)


def approve_loa_request(request_id: int, reviewer_id: int) -> Optional[sqlite3.Row]:
    row = get_loa_request(request_id)
    if not row or row["status"] != "pending":
        return None
    start_date = parse_date_input(row["start_date"])
    end_date = parse_date_input(row["end_date"])
    add_loa_period(int(row["discord_id"]), start_date, end_date, str(row["reason"]), reviewer_id, "request")
    now = dt_to_str(utcnow())
    with get_db() as conn:
        conn.execute(
            "UPDATE loa_requests SET status = 'approved', reviewer_id = ?, updated_at = ? WHERE id = ?",
            (reviewer_id, now, request_id),
        )
    return get_loa_request(request_id)


def deny_loa_request(request_id: int, reviewer_id: int, reviewer_note: str) -> Optional[sqlite3.Row]:
    row = get_loa_request(request_id)
    if not row or row["status"] != "pending":
        return None
    now = dt_to_str(utcnow())
    with get_db() as conn:
        conn.execute(
            "UPDATE loa_requests SET status = 'denied', reviewer_id = ?, reviewer_note = ?, updated_at = ? WHERE id = ?",
            (reviewer_id, reviewer_note, now, request_id),
        )
    return get_loa_request(request_id)


def list_loa_periods(active_only: bool = True) -> list[sqlite3.Row]:
    with get_db() as conn:
        if active_only:
            return conn.execute(
                "SELECT * FROM loa_periods WHERE active = 1 ORDER BY start_date ASC, id ASC"
            ).fetchall()
        return conn.execute(
            "SELECT * FROM loa_periods ORDER BY start_date DESC, id DESC"
        ).fetchall()


def get_active_loa_for_member(discord_id: int, on_date: date | None = None) -> Optional[sqlite3.Row]:
    on_date = on_date or local_today()
    with get_db() as conn:
        return conn.execute(
            """
            SELECT * FROM loa_periods
            WHERE discord_id = ? AND active = 1 AND start_date <= ? AND end_date >= ?
            ORDER BY start_date ASC, id ASC
            LIMIT 1
            """,
            (discord_id, on_date.isoformat(), on_date.isoformat()),
        ).fetchone()


def end_loa_period(period_id: int, ended_by: int | None = None) -> Optional[sqlite3.Row]:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM loa_periods WHERE id = ? AND active = 1", (period_id,)).fetchone()
        if not row:
            return None
        conn.execute("UPDATE loa_periods SET active = 0 WHERE id = ?", (period_id,))
        return row


def end_active_loa_for_member(discord_id: int) -> Optional[sqlite3.Row]:
    row = get_active_loa_for_member(discord_id)
    if not row:
        return None
    return end_loa_period(int(row["id"]))


def get_pending_loa_request_for_member(discord_id: int) -> Optional[sqlite3.Row]:
    with get_db() as conn:
        return conn.execute(
            "SELECT * FROM loa_requests WHERE discord_id = ? AND status = 'pending' ORDER BY id DESC LIMIT 1",
            (discord_id,),
        ).fetchone()


def cancel_pending_loa_request(request_id: int, reviewer_id: int | None = None, note: str = "Cancelled by requester") -> Optional[sqlite3.Row]:
    row = get_loa_request(request_id)
    if not row or row["status"] != "pending":
        return None
    now = dt_to_str(utcnow())
    with get_db() as conn:
        conn.execute(
            "UPDATE loa_requests SET status = 'cancelled', reviewer_id = ?, reviewer_note = ?, updated_at = ? WHERE id = ?",
            (reviewer_id, note, now, request_id),
        )
    return get_loa_request(request_id)


def has_loa_overlap_week(discord_id: int, wk: str) -> bool:
    week_start, week_end = week_bounds_from_key(wk)
    with get_db() as conn:
        rows = conn.execute(
            "SELECT start_date, end_date FROM loa_periods WHERE discord_id = ? AND active = 1",
            (discord_id,),
        ).fetchall()
    for row in rows:
        start_date = parse_date_input(row["start_date"])
        end_date = parse_date_input(row["end_date"])
        if overlap_dates(start_date, end_date, week_start, week_end):
            return True
    return False


def fetch_all_week_rows(wk: str) -> list[sqlite3.Row]:
    with get_db() as conn:
        return conn.execute(
            """
            SELECT ws.*, sm.minecraft_name, sm.strike_eligible, sm.notes, sm.last_minecraft_login_at
            FROM weekly_stats ws
            JOIN staff_members sm ON sm.discord_id = ws.discord_id
            WHERE ws.week_key = ? AND COALESCE(sm.is_active, 1) = 1
            ORDER BY ws.discord_id
            """,
            (wk,),
        ).fetchall()


def expire_decayed_strikes() -> int:
    expired_rows = get_expired_strike_rows()
    if not expired_rows:
        return 0

    changed = 0
    with get_db() as conn:
        for row in expired_rows:
            conn.execute("UPDATE strikes SET active = 0 WHERE id = ?", (row["id"],))
            conn.execute(
                "INSERT INTO pending_dm_events (discord_id, event_type, payload, created_at, sent) VALUES (?, ?, ?, ?, 0)",
                (
                    int(row["discord_id"]),
                    "strike_expired",
                    json.dumps({
                        "amount": int(row["amount"]),
                        "reason": str(row["reason"]),
                    }),
                    dt_to_str(utcnow()),
                ),
            )
            changed += 1
    return changed



def fetch_unsent_dm_events(limit: int = 25) -> list[sqlite3.Row]:
    with get_db() as conn:
        return conn.execute(
            "SELECT * FROM pending_dm_events WHERE sent = 0 ORDER BY id ASC LIMIT ?",
            (limit,),
        ).fetchall()


def mark_dm_event_sent(event_id: int) -> None:
    with get_db() as conn:
        conn.execute("UPDATE pending_dm_events SET sent = 1 WHERE id = ?", (event_id,))


def get_expired_strike_rows() -> list[sqlite3.Row]:
    now = utcnow()
    rows_out: list[sqlite3.Row] = []
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, discord_id, amount, kind, reason, created_at, is_permanent FROM strikes WHERE active = 1"
        ).fetchall()
        for row in rows:
            if int(row["is_permanent"] or 0) == 1:
                continue
            created = str_to_dt(row["created_at"])
            if not created:
                continue
            days = strike_decay_days_for(str(row["kind"]), str(row["reason"]))
            if days <= 0:
                continue
            if created + timedelta(days=days) <= now:
                rows_out.append(row)
    return rows_out


# =========================
# Discord bot
# =========================


intents = discord.Intents.default()
intents.members = True
intents.guilds = True
intents.moderation = True
intents.message_content = True
intents.presences = False

bot = commands.Bot(command_prefix="!", intents=intents)
TREE = bot.tree
TARGET_GUILD_ID = int(CONFIG.get("guild_id", 0) or 0)
LAST_SENT_ALERT_KEYS: set[str] = set()


def is_allowed_guild(guild: discord.Guild | None) -> bool:
    if not TARGET_GUILD_ID:
        return True
    return guild is not None and guild.id == TARGET_GUILD_ID


def is_management(member: discord.Member) -> bool:
    management_ids = set(CONFIG["management_role_ids"])

    if member.guild_permissions.administrator:
        return True

    if member.guild.owner_id == member.id:
        return True

    return any(role.id in management_ids for role in member.roles)


def is_auto_strike_role(member: discord.Member) -> bool:
    role_ids = set(CONFIG["auto_strike_role_ids"])
    return any(role.id in role_ids for role in member.roles)


def is_upper_staff(member: discord.Member) -> bool:
    role_ids = set(CONFIG["upper_staff_role_ids"])
    return any(role.id in role_ids for role in member.roles)


def resolve_channel_id(config_key: str, fallback_key: str | None = None) -> int:
    value = int(CONFIG.get(config_key, 0) or 0)
    if value:
        return value
    if fallback_key:
        return int(CONFIG.get(fallback_key, 0) or 0)
    return 0


PING_ID_ALIASES = {
    101614586811482973: 1016145868111482973,
}


def build_ping_text_from_ids(ids: list[int] | list[str], channel: discord.abc.GuildChannel | None) -> str | None:
    guild = channel.guild if isinstance(channel, discord.abc.GuildChannel) else None
    parts: list[str] = []
    for raw_id in ids:
        try:
            target_id = int(raw_id)
        except Exception:
            continue
        target_id = PING_ID_ALIASES.get(target_id, target_id)
        mention = None
        if guild:
            role = guild.get_role(target_id)
            if role:
                mention = role.mention
            else:
                member = guild.get_member(target_id)
                if member:
                    mention = member.mention
        if mention is None:
            mention = f"<@&{target_id}>"
        parts.append(mention)
    return " ".join(parts) if parts else None


def build_management_ping_text(channel: discord.abc.GuildChannel | None) -> str | None:
    return build_ping_text_from_ids(CONFIG.get("management_ping_target_ids", []), channel)


def build_regular_staff_ping_text(channel: discord.abc.GuildChannel | None) -> str | None:
    return build_ping_text_from_ids(CONFIG.get("regular_staff_ping_target_ids", []), channel)


async def send_embed_to_channel(
    channel_id: int,
    embed: discord.Embed,
    *,
    mention_management: bool = False,
    mention_regular_staff: bool = False,
    view: discord.ui.View | None = None,
) -> None:
    if not channel_id:
        return
    channel = bot.get_channel(channel_id)
    if isinstance(channel, discord.abc.Messageable):
        content_parts: list[str] = []
        guild_channel = channel if isinstance(channel, discord.abc.GuildChannel) else None
        if mention_management:
            mgmt = build_management_ping_text(guild_channel)
            if mgmt:
                content_parts.append(mgmt)
        if mention_regular_staff:
            regular = build_regular_staff_ping_text(guild_channel)
            if regular:
                content_parts.append(regular)
        content = " ".join(content_parts) if content_parts else None
        await channel.send(content=content, embed=embed, view=view, allowed_mentions=discord.AllowedMentions(everyone=False, users=True, roles=True, replied_user=False))


async def send_log(embed: discord.Embed, *, view: discord.ui.View | None = None) -> None:
    await send_embed_to_channel(resolve_channel_id("staff_log_channel_id"), embed, mention_management=True, view=view)


async def send_loa_log(embed: discord.Embed, *, view: discord.ui.View | None = None) -> None:
    await send_embed_to_channel(resolve_channel_id("loa_request_channel_id", "staff_log_channel_id"), embed, mention_management=True, view=view)


async def send_offboard_log(embed: discord.Embed, *, view: discord.ui.View | None = None) -> None:
    await send_embed_to_channel(resolve_channel_id("offboard_log_channel_id", "staff_log_channel_id"), embed, mention_management=True, view=view)


async def send_role_sync_log(embed: discord.Embed, *, view: discord.ui.View | None = None) -> None:
    await send_embed_to_channel(resolve_channel_id("role_sync_log_channel_id", "staff_log_channel_id"), embed, mention_management=True, view=view)


async def send_reminder_log(embed: discord.Embed, *, view: discord.ui.View | None = None) -> None:
    await send_embed_to_channel(resolve_channel_id("reminder_channel_id", "staff_log_channel_id"), embed, mention_management=True, view=view)




def compute_role_sync_issues(guild: discord.Guild) -> list[dict[str, str]]:
    staff_rows = list_staff_members()
    lower_role_ids = set(CONFIG.get("auto_strike_role_ids", []))
    upper_role_ids = set(CONFIG.get("upper_staff_role_ids", []))
    management_role_ids = set(CONFIG.get("management_role_ids", []))
    tracked_role_ids = lower_role_ids | upper_role_ids | management_role_ids

    issues: list[dict[str, str]] = []
    registered_ids: set[int] = set()

    for row in staff_rows:
        discord_id = int(row["discord_id"])
        registered_ids.add(discord_id)
        member = guild.get_member(discord_id)
        is_active = is_staff_active_row(row)

        if member is None:
            issues.append({
                "type": "Missing from guild",
                "who": f"<@{discord_id}>",
                "details": "Registered in database but no longer in the Discord server.",
            })
            continue

        member_role_ids = {role.id for role in member.roles}
        has_lower = bool(member_role_ids & lower_role_ids)
        has_upper = bool(member_role_ids & upper_role_ids)
        has_management = bool(member_role_ids & management_role_ids)
        has_any_staff_role = bool(member_role_ids & tracked_role_ids)

        if is_active and not has_any_staff_role:
            issues.append({
                "type": "Active but missing staff role",
                "who": member.mention,
                "details": "Marked active in the bot but has no tracked staff role in Discord.",
            })

        if is_active and bool(row["strike_eligible"]) and not has_lower and not has_upper and not has_management:
            issues.append({
                "type": "Lower-staff mismatch",
                "who": member.mention,
                "details": "Marked strike-eligible in the bot but does not have a tracked lower-staff role.",
            })

        if is_active and (not bool(row["strike_eligible"])) and has_lower and not has_upper and not has_management:
            issues.append({
                "type": "Eligibility mismatch",
                "who": member.mention,
                "details": "Has a lower staff role in Discord but is not strike-eligible in the bot.",
            })

        if (not is_active) and has_any_staff_role:
            issues.append({
                "type": "Offboarded but still has role",
                "who": member.mention,
                "details": "Marked inactive/offboarded in the bot but still has tracked staff roles.",
            })

    for member in guild.members:
        if member.bot:
            continue
        member_role_ids = {role.id for role in member.roles}
        if member.id not in registered_ids and (member_role_ids & tracked_role_ids):
            issues.append({
                "type": "Unregistered staff-role member",
                "who": member.mention,
                "details": "Has a tracked staff role in Discord but is not registered in the bot.",
            })

    return issues


def build_role_sync_embed(guild: discord.Guild) -> discord.Embed:
    issues = compute_role_sync_issues(guild)
    if not issues:
        embed = discord.Embed(title="Role Sync Audit", colour=discord.Colour.green())
        embed.description = "No role sync issues detected."
        return embed

    embed = discord.Embed(title="Role Sync Audit", colour=discord.Colour.orange())
    embed.description = f"Found **{len(issues)}** role/database mismatch issue(s)."
    chunks: list[str] = []
    for item in issues[:15]:
        chunks.append(f"**{item['type']}** — {item['who']}\n{item['details']}")
    embed.add_field(name="Issues", value="\n\n".join(chunks), inline=False)
    if len(issues) > 15:
        embed.set_footer(text=f"Showing 15 of {len(issues)} issues.")
    return embed


def get_upcoming_loa_endings(days_ahead: int) -> list[dict[str, Any]]:
    today = local_today()
    results: list[dict[str, Any]] = []
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM loa_periods WHERE active = 1 ORDER BY end_date ASC, id ASC"
        ).fetchall()
    for row in rows:
        try:
            end_date = date.fromisoformat(str(row["end_date"]))
        except Exception:
            continue
        days_left = (end_date - today).days
        if 0 <= days_left <= days_ahead:
            results.append({"row": row, "days_left": days_left})
    return results


def get_upcoming_strike_expiries(days_ahead: int) -> list[dict[str, Any]]:
    today = local_today()
    results: list[dict[str, Any]] = []
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM strikes WHERE active = 1 AND COALESCE(is_permanent, 0) = 0 ORDER BY created_at ASC, id ASC"
        ).fetchall()
    for row in rows:
        decay_days = strike_decay_days_for(str(row["kind"]), str(row["reason"]))
        if decay_days <= 0:
            continue
        try:
            created = datetime.fromisoformat(str(row["created_at"]))
        except Exception:
            continue
        expiry_date = (created.astimezone(TZ) + timedelta(days=decay_days)).date()
        days_left = (expiry_date - today).days
        if 0 <= days_left <= days_ahead:
            results.append({"row": row, "days_left": days_left, "expiry_date": expiry_date})
    return results


def build_management_reminders_embed(guild: discord.Guild, wk: str) -> discord.Embed | None:
    loa_days = int(CONFIG.get("reminder_days_before_loa_end", 2) or 2)
    strike_days = int(CONFIG.get("reminder_days_before_strike_expiry", 3) or 3)
    overview = compute_overview_groups(guild, wk)
    zero_activity = overview.get("zero_activity", [])
    loa_endings = get_upcoming_loa_endings(loa_days)
    strike_expiries = get_upcoming_strike_expiries(strike_days)

    if not zero_activity and not loa_endings and not strike_expiries:
        return None

    embed = discord.Embed(title="Management Reminders", colour=discord.Colour.blurple())
    if loa_endings:
        lines = []
        for item in loa_endings[:10]:
            row = item["row"]
            lines.append(f"<@{int(row['discord_id'])}> — LOA ends `{row['end_date']}` ({item['days_left']} day(s))")
        embed.add_field(name="LOA Ending Soon", value="\n".join(lines), inline=False)
    if strike_expiries:
        lines = []
        for item in strike_expiries[:10]:
            row = item["row"]
            lines.append(
                f"<@{int(row['discord_id'])}> — `{row['amount']}` strike(s) expire `{item['expiry_date'].isoformat()}` ({item['days_left']} day(s))"
            )
        embed.add_field(name="Strikes Expiring Soon", value="\n".join(lines), inline=False)
    if zero_activity:
        lines = []
        for item in zero_activity[:10]:
            lines.append(f"{item['mention']} — `0h 00m` this week")
        embed.add_field(name="Lower Staff With 0 Activity", value="\n".join(lines), inline=False)
    embed.set_footer(text=f"Week key: {wk}")
    return embed


async def maybe_send_daily_management_checks() -> None:
    if not TARGET_GUILD_ID:
        return
    guild = bot.get_guild(TARGET_GUILD_ID)
    if guild is None:
        return

    now_local = utcnow().astimezone(TZ)
    today_key = now_local.date().isoformat()

    if now_local.hour >= 9:
        remind_key = f"reminders:{today_key}"
        if remind_key not in LAST_SENT_ALERT_KEYS:
            reminders = build_management_reminders_embed(guild, week_key_for())
            if reminders is not None:
                await send_reminder_log(reminders)
            LAST_SENT_ALERT_KEYS.add(remind_key)

    if now_local.hour >= 12:
        audit_key = f"rolesync:{today_key}"
        if audit_key not in LAST_SENT_ALERT_KEYS:
            role_sync = build_role_sync_embed(guild)
            if role_sync.description != "No role sync issues detected.":
                await send_role_sync_log(role_sync)
            LAST_SENT_ALERT_KEYS.add(audit_key)


def build_export_text_for_member(discord_id: int) -> str:
    staff = get_staff_member(discord_id)
    if not staff:
        return "Staff member not found."

    lines: list[str] = []
    lines.append(f"discord_id: {discord_id}")
    lines.append(f"minecraft_name: {staff['minecraft_name'] or 'None'}")
    lines.append(f"is_active: {int(staff['is_active']) if 'is_active' in staff.keys() else 1}")
    lines.append(f"strike_eligible: {int(staff['strike_eligible'])}")
    lines.append(f"registered_at: {staff['created_at']}")
    lines.append(f"last_minecraft_login_at: {staff['last_minecraft_login_at'] or 'None'}")
    if 'offboard_type' in staff.keys() and staff['offboard_type']:
        lines.append(f"offboard_type: {staff['offboard_type']}")
        lines.append(f"offboard_reason: {staff['offboard_reason'] or ''}")
        lines.append(f"offboarded_at: {staff['offboarded_at'] or ''}")

    current_stats = fetch_weekly_stats(discord_id)
    lines.append("")
    lines.append("=== Current Snapshot ===")
    lines.append(f"current_week_key: {week_key_for()}")
    lines.append(f"current_week_playtime: {human_hours(int(current_stats['minecraft_seconds']))}")
    lines.append(
        "current_week_punishments: "
        + str(
            int(current_stats["discord_warns"]) + int(current_stats["discord_kicks"]) + int(current_stats["discord_bans"]) + int(current_stats["discord_mutes"])
            + int(current_stats["mc_warns"]) + int(current_stats["mc_kicks"]) + int(current_stats["mc_bans"]) + int(current_stats["mc_mutes"])
        )
    )
    lines.append(f"active_strikes: {active_strike_count(discord_id)}")
    lines.append(f"permanent_active_strikes: {count_permanent_active_strikes(discord_id)}")
    lines.append(f"recommendation: {status_label_for(discord_id, int(current_stats['minecraft_seconds']), bool(staff['strike_eligible']), week_key_for())}")

    lines.append("")
    lines.append("=== Recent Weekly Snapshots ===")
    with get_db() as conn:
        recent_weeks = conn.execute(
            "SELECT * FROM weekly_stats WHERE discord_id = ? ORDER BY week_key DESC LIMIT 6",
            (discord_id,),
        ).fetchall()
    for row in recent_weeks:
        total_actions = (
            int(row["discord_warns"]) + int(row["discord_kicks"]) + int(row["discord_bans"]) + int(row["discord_mutes"])
            + int(row["mc_warns"]) + int(row["mc_kicks"]) + int(row["mc_bans"]) + int(row["mc_mutes"])
        )
        lines.append(
            f"{row['week_key']} | playtime={human_hours(int(row['minecraft_seconds']))} | punishments={total_actions} | strike_eligible={int(row['strike_eligible'])}"
        )

    lines.append("")
    lines.append("=== Recent Strikes ===")
    for row in list_recent_strike_history(discord_id, limit=10):
        lines.append(
            f"#{row['id']} | +{row['amount']} | {row['kind']} | {row['reason']} | active={row['active']} | permanent={row['is_permanent']} | created_at={row['created_at']}"
        )

    lines.append("")
    lines.append("=== Recent Incidents ===")
    for row in list_recent_incident_history(discord_id, limit=10):
        lines.append(
            f"#{row['id']} | {row['severity']} | {row['summary']} | created_at={row['created_at']} | evidence={row['evidence_url'] or 'None'}"
        )

    lines.append("")
    lines.append("=== Recent LOA ===")
    for row in list_recent_loa_history(discord_id, limit=10):
        lines.append(
            f"#{row['id']} | {row['start_date']} -> {row['end_date']} | reason={row['reason']} | active={row['active']}"
        )

    lines.append("")
    lines.append("=== Recent Notes ===")
    for row in list_recent_note_history(discord_id, limit=10):
        lines.append(
            f"#{row['id']} | {row['action']} | {row['note_text'] or ''} | created_at={row['created_at']}"
        )

    lines.append("")
    lines.append("=== Recent Weekly Activity ===")
    for row in fetch_member_week_rows(discord_id, 8):
        total_p = int(row["discord_warns"]) + int(row["discord_kicks"]) + int(row["discord_bans"]) + int(row["discord_mutes"]) + int(row["mc_warns"]) + int(row["mc_kicks"]) + int(row["mc_bans"]) + int(row["mc_mutes"])
        lines.append(
            f"{row['week_key']} | minecraft={human_hours(int(row['minecraft_seconds']))} | punishments={total_p} | strike_eligible={row['strike_eligible']}"
        )

    return "\n".join(lines)


def build_archive_embeds(limit: int = 8) -> list[discord.Embed]:
    with get_db() as conn:
        weeks = [r["week_key"] for r in conn.execute("SELECT DISTINCT week_key FROM weekly_stats ORDER BY week_key DESC LIMIT ?", (limit,)).fetchall()]
    embeds: list[discord.Embed] = []
    for wk in weeks:
        rows = fetch_all_week_rows(str(wk))
        if not rows:
            continue
        total_mc = sum(int(r["minecraft_seconds"]) for r in rows)
        total_actions = 0
        for r in rows:
            total_actions += int(r["discord_warns"]) + int(r["discord_kicks"]) + int(r["discord_bans"]) + int(r["discord_mutes"]) + int(r["mc_warns"]) + int(r["mc_kicks"]) + int(r["mc_bans"]) + int(r["mc_mutes"])
        embed = discord.Embed(title=f"Weekly Archive • {wk}", colour=discord.Colour.dark_teal())
        embed.description = (
            f"**Tracked Staff Rows:** {len(rows)}\n"
            f"**Total MC Playtime:** {human_hours(total_mc)}\n"
            f"**Total Punishments Logged:** {total_actions}"
        )
        embeds.append(embed)
    return embeds


def compute_overview_groups(guild: discord.Guild, wk: str) -> dict[str, Any]:
    staff_rows = list_staff_members()
    lower_role_ids = set(CONFIG.get("auto_strike_role_ids", []))
    scored: list[dict[str, Any]] = []
    for staff in staff_rows:
        discord_id = int(staff["discord_id"])
        member = guild.get_member(discord_id)
        stats = fetch_weekly_stats(discord_id)
        mc_seconds = int(stats["minecraft_seconds"])
        punishments = int(stats["discord_warns"]) + int(stats["discord_kicks"]) + int(stats["discord_bans"]) + int(stats["discord_mutes"]) + int(stats["mc_warns"]) + int(stats["mc_kicks"]) + int(stats["mc_bans"]) + int(stats["mc_mutes"])
        strikes = active_strike_count(discord_id)
        streak = consistency_streak_weeks(discord_id, 4)
        active_loa = get_active_loa_for_member(discord_id)
        status = status_label_for(discord_id, mc_seconds, bool(staff["strike_eligible"]), wk)
        is_lower = bool(staff["strike_eligible"])
        if member is not None and lower_role_ids:
            is_lower = any(role.id in lower_role_ids for role in member.roles)
        scored.append({
            "discord_id": discord_id,
            "mention": member.mention if member else f"<@{discord_id}>",
            "mc_seconds": mc_seconds,
            "punishments": punishments,
            "strikes": strikes,
            "streak": streak,
            "status": status,
            "is_lower": is_lower,
            "active_loa": active_loa,
            "needs_attention": status in {"Watchlist", "Needs Review"} or strikes > 0,
            "zero_activity": mc_seconds == 0 and is_lower and active_loa is None,
        })
    data = {
        "watchlist": [x for x in scored if x["status"] == "Watchlist"],
        "needs_review": [x for x in scored if x["status"] == "Needs Review"],
        "excused": [x for x in scored if x["status"] == "Excused"],
        "zero_activity": [x for x in scored if x["zero_activity"]],
        "top_performers": sorted([x for x in scored if x["status"] in {"Excellent","Good Standing"}], key=lambda x: (-x["mc_seconds"], x["strikes"], -x["streak"], -x["punishments"]))[:10],
        "scored": scored,
    }
    return data


def build_overview_section_embed(guild: discord.Guild, wk: str, section: str) -> discord.Embed:
    data = compute_overview_groups(guild, wk)
    title_map = {
        "watchlist": "Overview • Watchlist / Needs Review",
        "loa": "Overview • Excused / LOA",
        "performers": "Overview • Top Performers",
        "zero": "Overview • Lower Staff With 0 Activity",
    }
    embed = discord.Embed(title=title_map.get(section, "Overview"), colour=discord.Colour.dark_purple())
    entries: list[str] = []
    if section == "watchlist":
        items = data["needs_review"] + [x for x in data["watchlist"] if x not in data["needs_review"]]
        for item in items[:10]:
            entries.append(f"{item['mention']} — {status_emoji(item['status'])} {item['status']} • `{item['strikes']}` strike(s) • `{human_hours(item['mc_seconds'])}`")
    elif section == "loa":
        for item in data["excused"][:10]:
            loa = item["active_loa"]
            if loa is not None:
                entries.append(f"{item['mention']} — Excused until `{loa['end_date']}`")
            else:
                entries.append(f"{item['mention']} — Excused")
    elif section == "performers":
        for item in data["top_performers"][:10]:
            entries.append(f"{item['mention']} — `{human_hours(item['mc_seconds'])}` • `{item['streak']}` week streak • {status_emoji(item['status'])} {item['status']}")
    elif section == "zero":
        for item in data["zero_activity"][:10]:
            entries.append(f"{item['mention']} — `0h 00m` this week")
    embed.description = "\n".join(entries) if entries else "None."
    embed.set_footer(text=f"Week key: {wk}")
    return embed


async def dm_strike_notice(member: discord.abc.User, total_strikes: int, reason: str) -> None:
    reason_text = (reason or "No reason provided.").strip()
    severity = "Standard"
    clean_reason = reason_text

    match = re.match(r"^\[(MINOR|MAJOR|SEVERE)\]\s*(.+)$", reason_text, flags=re.IGNORECASE)
    if match:
        severity = match.group(1).title()
        clean_reason = match.group(2).strip()

    embed = discord.Embed(
        title="Staff Strike Issued",
        description="This is an official staff management notice.",
        colour=discord.Colour.red(),
    )
    embed.add_field(name="Severity", value=severity, inline=True)
    embed.add_field(name="Active Strikes", value=str(total_strikes), inline=True)
    embed.add_field(name="Reason", value=clean_reason[:1024], inline=False)

    warning_text = str(CONFIG.get("warning_text", "")).strip()
    if warning_text:
        embed.add_field(name="Important", value=warning_text[:1024], inline=False)

    embed.set_footer(text="Contact upper management if you believe this was issued in error.")

    try:
        await member.send(embed=embed)
    except discord.Forbidden:
        log.warning("Could not DM strike notice to %s", member.id)


async def resolve_user_for_dm(discord_id: int) -> discord.abc.User | None:
    user = bot.get_user(discord_id)
    if user is not None:
        return user
    try:
        return await bot.fetch_user(discord_id)
    except Exception:
        return None


async def dm_strike_removed_notice(member: discord.abc.User, removed_amount: int, total_strikes: int, *, removed_by: int | None = None) -> None:
    embed = discord.Embed(
        title="Staff Strike Removed",
        description="A strike on your staff record was removed.",
        colour=discord.Colour.green(),
    )
    embed.add_field(name="Removed", value=str(removed_amount), inline=True)
    embed.add_field(name="Active Strikes", value=str(total_strikes), inline=True)
    if removed_by:
        embed.add_field(name="Removed By", value=f"<@{removed_by}>", inline=False)
    embed.set_footer(text="This update was made by management.")
    try:
        await member.send(embed=embed)
    except discord.Forbidden:
        log.warning("Could not DM strike removal notice to %s", member.id)


async def dm_strike_expired_notice(member: discord.abc.User, expired_amount: int, total_strikes: int, reason: str) -> None:
    reason_text = (reason or "No reason provided.").strip()
    embed = discord.Embed(
        title="Staff Strike Expired",
        description="A strike on your staff record expired automatically.",
        colour=discord.Colour.green(),
    )
    embed.add_field(name="Expired", value=str(expired_amount), inline=True)
    embed.add_field(name="Active Strikes", value=str(total_strikes), inline=True)
    embed.add_field(name="Reason", value=reason_text[:1024], inline=False)
    embed.set_footer(text="This was handled automatically by the staff management system.")
    try:
        await member.send(embed=embed)
    except discord.Forbidden:
        log.warning("Could not DM strike expiry notice to %s", member.id)


async def dm_loa_removed_notice(member: discord.abc.User, *, removed_by: int | None = None, note: str | None = None) -> None:
    embed = discord.Embed(
        title="Leave of Absence Removed",
        description="Management ended your active LOA early.",
        colour=discord.Colour.orange(),
    )
    if removed_by:
        embed.add_field(name="Removed By", value=f"<@{removed_by}>", inline=False)
    if note and note.strip():
        embed.add_field(name="Note", value=note.strip()[:1024], inline=False)
    embed.set_footer(text="Contact upper management if you believe this was removed in error.")
    try:
        await member.send(embed=embed)
    except discord.Forbidden:
        log.warning("Could not DM LOA removal notice to %s", member.id)


async def process_pending_dm_events() -> None:
    events = fetch_unsent_dm_events(50)
    for event in events:
        try:
            payload = json.loads(str(event["payload"]))
        except Exception:
            payload = {}
        discord_id = int(event["discord_id"])
        member = await resolve_user_for_dm(discord_id)
        total = active_strike_count(discord_id)
        if member is not None and event["event_type"] == "strike_expired":
            await dm_strike_expired_notice(
                member,
                int(payload.get("amount", 1)),
                total,
                str(payload.get("reason", "No reason provided.")),
            )
        mark_dm_event_sent(int(event["id"]))


async def apply_strike_and_notify(
    member: discord.Member | discord.User,
    amount: int,
    reason: str,
    kind: str,
    issued_by: int | None = None,
    send_dm: bool = True,
    evidence_url: str | None = None,
    is_permanent: bool = False,
) -> int:
    total = add_strike(member.id, amount, reason, kind, issued_by, evidence_url=evidence_url, is_permanent=is_permanent)
    if send_dm:
        await dm_strike_notice(member, total, reason)

    embed = discord.Embed(title="Strike Issued", colour=discord.Colour.red())
    embed.add_field(name="Staff", value=f"{member} ({member.id})", inline=False)
    embed.add_field(name="Amount", value=str(amount))
    embed.add_field(name="Reason", value=reason, inline=False)
    if evidence_url:
        embed.add_field(name="Evidence", value=evidence_url, inline=False)
    embed.add_field(name="Type", value=kind)
    embed.add_field(name="Permanent", value="Yes" if is_permanent else "No")
    embed.add_field(name="Active Strikes", value=str(total))
    await send_log(embed, view=StaffStatsButtonView(member.id, allow_remove_strike=True, evidence_url=evidence_url))
    return total


async def management_predicate(interaction: discord.Interaction) -> bool:
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        raise app_commands.CheckFailure("This command must be used in the server.")

    if not is_allowed_guild(interaction.guild):
        raise app_commands.CheckFailure("This bot is locked to a different server.")

    if is_management(interaction.user):
        return True

    raise app_commands.CheckFailure("You do not have permission to use this command.")


management_check = app_commands.check(management_predicate)


async def sync_commands_once() -> None:
    if TARGET_GUILD_ID:
        guild_obj = discord.Object(id=TARGET_GUILD_ID)
        try:
            TREE.clear_commands(guild=guild_obj)
            TREE.copy_global_to(guild=guild_obj)
            synced = await TREE.sync(guild=guild_obj)
            log.info("Synced %s guild command(s) to target guild %s", len(synced), TARGET_GUILD_ID)
        except Exception:
            log.exception("Failed syncing guild commands to target guild %s", TARGET_GUILD_ID)
    else:
        try:
            synced = await TREE.sync()
            log.info("Synced %s global command(s)", len(synced))
        except Exception:
            log.exception("Failed syncing global commands")


def log_startup_summary() -> None:
    log.info(
        "Startup summary | guild=%s | logs=%s | weekly_reports=%s | litebans_channel=%s | timezone=%s | db=%s",
        CONFIG.get("guild_id"),
        CONFIG.get("staff_log_channel_id"),
        CONFIG.get("weekly_report_channel_id"),
        CONFIG.get("litebans_webhook_channel_id"),
        CONFIG.get("timezone"),
        DB_PATH,
    )
    log.info(
        "Startup summary | management_roles=%s | lower_staff_roles=%s | upper_staff_roles=%s",
        len(CONFIG.get("management_role_ids", [])),
        len(CONFIG.get("auto_strike_role_ids", [])),
        len(CONFIG.get("upper_staff_role_ids", [])),
    )


@bot.event
async def on_ready() -> None:
    log.info("Logged in as %s (%s)", bot.user, bot.user.id if bot.user else "?")
    log_startup_summary()
    expire_decayed_strikes()
    await process_pending_dm_events()

    await sync_commands_once()

    if TARGET_GUILD_ID:
        for guild in list(bot.guilds):
            if guild.id != TARGET_GUILD_ID:
                log.warning("Leaving unauthorized guild: %s (%s)", guild.name, guild.id)
                try:
                    await guild.leave()
                except Exception:
                    log.exception("Failed leaving unauthorized guild %s", guild.id)

    if not background_loops.is_running():
        background_loops.start()
    if not weekly_reset_loop.is_running():
        weekly_reset_loop.start()


@bot.event
async def on_guild_join(guild: discord.Guild) -> None:
    if is_allowed_guild(guild):
        return
    log.warning("Joined unauthorized guild: %s (%s). Leaving.", guild.name, guild.id)
    try:
        await guild.leave()
    except Exception:
        log.exception("Failed to leave unauthorized guild %s", guild.id)


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
    if isinstance(error, app_commands.CheckFailure):
        message = str(error) or "You cannot use that command here."
        try:
            if interaction.response.is_done():
                await interaction.followup.send(message, ephemeral=True)
            else:
                await interaction.response.send_message(message, ephemeral=True)
        except Exception:
            log.exception("Failed sending app command error response")
        return

    log.exception("Unhandled app command error", exc_info=error)
    try:
        if interaction.response.is_done():
            await interaction.followup.send("Something went wrong while running that command.", ephemeral=True)
        else:
            await interaction.response.send_message("Something went wrong while running that command.", ephemeral=True)
    except Exception:
        log.exception("Failed sending generic app command error response")


@bot.event
async def on_message(message: discord.Message) -> None:
    if message.guild and not is_allowed_guild(message.guild):
        return
    try:
        webhook_channel_id = int(CONFIG.get("litebans_webhook_channel_id", 0) or 0)
        if (
            webhook_channel_id
            and message.channel.id == webhook_channel_id
            and message.webhook_id is not None
            and not has_processed_webhook_message(message.id)
        ):
            blob = build_message_blob(message)
            action_type = extract_action_type_from_blob(blob)
            if action_type:
                executor_name = extract_executor_minecraft_name(message, blob)
                if executor_name:
                    staff_row = resolve_staff_by_mc_name(executor_name)
                    if staff_row:
                        target_name = extract_target_name(message, blob, executor_name)
                        reason = extract_reason(message, blob)
                        increment_punishment(int(staff_row["discord_id"]), "minecraft", action_type, None, target_name, reason)
                        mark_processed_webhook_message(message.id, "litebans")
                        log.info("Imported LiteBans webhook punishment: %s by %s", action_type, executor_name)
                    else:
                        log.warning("LiteBans webhook executor not mapped to a registered staff member: %s", executor_name)
                else:
                    log.warning("Could not parse executor from LiteBans webhook message %s", message.id)
    finally:
        await bot.process_commands(message)


@bot.event
async def on_audit_log_entry_create(entry: discord.AuditLogEntry) -> None:
    if not is_allowed_guild(entry.guild):
        return
    # Optional auto-tracking for actions done outside bot commands.
    # Warns do not exist natively on Discord, so those must be bot-issued.
    try:
        if not entry.user or not isinstance(entry.user, discord.Member):
            return
        staff = get_staff_member(entry.user.id)
        if not staff:
            return
        action_map = {
            discord.AuditLogAction.kick: "kick",
            discord.AuditLogAction.ban: "ban",
            discord.AuditLogAction.member_update: None,  # timeout checked below
        }
        action = action_map.get(entry.action)
        if entry.action == discord.AuditLogAction.member_update:
            # Timeout changes show up as member_update; we only count them when communication_disabled_until changed.
            before = getattr(entry.before, "timed_out_until", None)
            after = getattr(entry.after, "timed_out_until", None)
            if after and before != after:
                action = "mute"
        if not action:
            return
        target_name = str(entry.target) if entry.target else None
        increment_punishment(entry.user.id, "discord", action, entry.user.id, target_name, entry.reason)
        embed = discord.Embed(title="Discord Punishment Tracked", colour=discord.Colour.orange())
        embed.add_field(name="Moderator", value=f"{entry.user} ({entry.user.id})", inline=False)
        embed.add_field(name="Action", value=action)
        embed.add_field(name="Target", value=target_name or "Unknown", inline=False)
        embed.add_field(name="Reason", value=entry.reason or "No reason provided", inline=False)
        await send_log(embed, view=StaffStatsButtonView(entry.user.id, allow_remove_strike=False))
    except Exception:
        log.exception("Error processing audit log entry")


# =========================
# Slash commands
# =========================


staff_group = app_commands.Group(name="staff", description="Staff management commands")
mod_group = app_commands.Group(name="mod", description="Moderation commands tracked by the bot")
admin_group = app_commands.Group(name="admin", description="Administrative tracking helpers")
loa_group = app_commands.Group(name="loa", description="Leave of absence commands")


@staff_group.command(name="register", description="Register a new staff member in the tracker")
@management_check
@app_commands.describe(member="Discord staff member", minecraft_name="Minecraft username", strike_eligible="Auto strike eligible (helper/jr mod/mod)")
async def register_staff(interaction: discord.Interaction, member: discord.Member, minecraft_name: str, strike_eligible: bool) -> None:
    await interaction.response.defer(ephemeral=True)

    existing = get_staff_member(member.id)
    if existing:
        if is_staff_active_row(existing):
            await interaction.followup.send(
                f"{member.mention} is already registered in the staff database. Use the existing profile instead of registering again.",
                ephemeral=True,
            )
            return
        await interaction.followup.send(
            f"{member.mention} already exists in the database but is inactive/offboarded. Use `/staff reinstate` instead.",
            ephemeral=True,
        )
        return

    upsert_staff_member(member.id, minecraft_name, strike_eligible)
    ensure_week_row(member.id)

    dm_sent = False
    try:
        reg_embed = discord.Embed(
            title="You were added to the staff database",
            description=(
                "Congratulations. You have been added to the staff management database for this server."
            ),
            colour=discord.Colour.green(),
        )
        reg_embed.add_field(name="Minecraft Name", value=f"`{minecraft_name}`", inline=True)
        reg_embed.add_field(name="Auto-Strike Eligible", value="Yes" if strike_eligible else "No", inline=True)
        reg_embed.set_footer(text="Your weekly activity, playtime, and staff records can now be tracked.")
        await member.send(embed=reg_embed)
        dm_sent = True
    except discord.Forbidden:
        dm_sent = False
    except Exception:
        dm_sent = False

    msg = f"Registered {member.mention} with Minecraft name `{minecraft_name}`. Strike eligible: `{strike_eligible}`."
    if dm_sent:
        msg += " DM sent."
    else:
        msg += " Could not send DM."

    await interaction.followup.send(msg, ephemeral=True)


def build_staff_stats_embed(member: discord.Member, staff: sqlite3.Row, stats: sqlite3.Row) -> discord.Embed:
    strikes = active_strike_count(member.id)
    status = status_label_for(member.id, int(stats["minecraft_seconds"]), bool(staff["strike_eligible"]), week_key_for())
    loa_row = get_active_loa_for_member(member.id)
    total_punishments = int(stats["discord_warns"]) + int(stats["discord_kicks"]) + int(stats["discord_bans"]) + int(stats["discord_mutes"]) + int(stats["mc_warns"]) + int(stats["mc_kicks"]) + int(stats["mc_bans"]) + int(stats["mc_mutes"])

    embed = discord.Embed(title=f"Staff Review • {member}", colour=discord.Colour.blurple())
    embed.add_field(
        name="Overview",
        value=(
            f"**Status:** {status_emoji(status)} {status}\n"
            f"**Active Strikes:** {strikes}\n"
            f"**LOA:** {format_loa_row(loa_row)}"
        ),
        inline=True,
    )
    embed.add_field(
        name="Activity",
        value=(
            f"**Minecraft:** {human_hours(int(stats['minecraft_seconds']))}\n"
            f"**Discord:** {human_hours(int(stats['discord_seconds']))}\n"
            f"**Total Punishments:** {total_punishments}"
        ),
        inline=True,
    )
    embed.add_field(
        name="Profile",
        value=(
            f"**Minecraft Name:** {staff['minecraft_name'] or 'Not set'}\n"
            f"**Auto-Strike Eligible:** {'Yes' if bool(staff['strike_eligible']) else 'No'}\n"
            f"**Last MC Login:** {str(staff['last_minecraft_login_at'] or 'Unknown')[:19]}"
        ),
        inline=True,
    )
    embed.add_field(
        name="Minecraft Punishments",
        value=(
            f"Warns: {stats['mc_warns']}\n"
            f"Kicks: {stats['mc_kicks']}\n"
            f"Bans: {stats['mc_bans']}\n"
            f"Mutes: {stats['mc_mutes']}"
        ),
        inline=True,
    )
    embed.add_field(
        name="Discord Punishments",
        value=(
            f"Warns: {stats['discord_warns']}\n"
            f"Kicks: {stats['discord_kicks']}\n"
            f"Bans: {stats['discord_bans']}\n"
            f"Mutes: {stats['discord_mutes']}"
        ),
        inline=True,
    )
    embed.add_field(
        name="Weekly Summary",
        value=(
            f"Minimum required: {CONFIG['minimum_weekly_hours']}h\n"
            f"Review window: {week_key_for()}\n"
            f"Member ID: {member.id}"
        ),
        inline=True,
    )
    if staff['notes']:
        embed.add_field(name="Management Notes", value=str(staff['notes'])[:1024], inline=False)
    return embed


def build_loa_request_embed(row: sqlite3.Row) -> discord.Embed:
    embed = discord.Embed(title=f"LOA Request #{row['id']}", colour=discord.Colour.gold())
    embed.add_field(name="Staff", value=f"<@{row['discord_id']}>", inline=True)
    embed.add_field(name="Dates", value=f"{row['start_date']} → {row['end_date']}", inline=True)
    embed.add_field(name="Status", value=str(row['status']).title(), inline=True)
    embed.add_field(name="Reason", value=str(row['reason'])[:1024], inline=False)
    embed.add_field(name="Review", value="Use the buttons below to approve, deny, or view stats.", inline=False)
    return embed


def build_loa_update_embed(title: str, row: sqlite3.Row, note: str | None = None) -> discord.Embed:
    embed = discord.Embed(title=title, colour=discord.Colour.orange())
    embed.add_field(name="Staff", value=f"<@{row['discord_id']}>", inline=True)
    if 'start_date' in row.keys() and 'end_date' in row.keys():
        embed.add_field(name="Dates", value=f"{row['start_date']} → {row['end_date']}", inline=True)
    if note:
        embed.add_field(name="Note", value=note[:1024], inline=False)
    return embed


class OverviewDashboardView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=600)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not interaction.guild or not isinstance(interaction.user, discord.Member) or not is_allowed_guild(interaction.guild):
            await interaction.response.send_message("This button can only be used in the target server.", ephemeral=True)
            return False
        if not is_management(interaction.user):
            await interaction.response.send_message("You do not have permission to use this button.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="View Watchlist", style=discord.ButtonStyle.secondary, row=0)
    async def view_watchlist(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        embed = build_overview_section_embed(interaction.guild, week_key_for(), "watchlist")
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="View LOA", style=discord.ButtonStyle.secondary, row=0)
    async def view_loa(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        embed = build_overview_section_embed(interaction.guild, week_key_for(), "loa")
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="Top Performers", style=discord.ButtonStyle.secondary, row=1)
    async def view_performers(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        embed = build_overview_section_embed(interaction.guild, week_key_for(), "performers")
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="0 Activity", style=discord.ButtonStyle.secondary, row=1)
    async def view_zero(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        embed = build_overview_section_embed(interaction.guild, week_key_for(), "zero")
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="Open Leaderboard", style=discord.ButtonStyle.primary, row=2)
    async def open_leaderboard(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        embed = build_private_leaderboard_embed(interaction.guild, week_key_for())
        if not embed:
            await interaction.response.send_message("No weekly staff data exists yet.", ephemeral=True)
            return
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="Role Sync", style=discord.ButtonStyle.secondary, row=2)
    async def open_role_sync(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        embed = build_role_sync_embed(interaction.guild)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="Reminders", style=discord.ButtonStyle.secondary, row=2)
    async def open_reminders(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        embed = build_management_reminders_embed(interaction.guild, week_key_for())
        if not embed:
            await interaction.response.send_message("No management reminders are active right now.", ephemeral=True)
            return
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="Weekly Archive", style=discord.ButtonStyle.secondary, row=3)
    async def open_archive(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        embeds = build_archive_embeds(8)
        if not embeds:
            await interaction.response.send_message("No archived weekly data exists yet.", ephemeral=True)
            return
        await interaction.response.send_message(embed=embeds[0], ephemeral=True)
        for embed in embeds[1:3]:
            await interaction.followup.send(embed=embed, ephemeral=True)


class LOARequestView(discord.ui.View):
    def __init__(self, request_id: int):
        super().__init__(timeout=None)
        self.request_id = request_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not interaction.guild or not isinstance(interaction.user, discord.Member) or not is_allowed_guild(interaction.guild):
            await interaction.response.send_message("This button can only be used in the target server.", ephemeral=True)
            return False
        if not is_management(interaction.user):
            await interaction.response.send_message("You do not have permission to use this button.", ephemeral=True)
            return False
        return True

    async def _disable_self(self, interaction: discord.Interaction) -> None:
        for child in self.children:
            child.disabled = True
        try:
            await interaction.message.edit(view=self)
        except Exception:
            pass

    @discord.ui.button(label="View Stats", style=discord.ButtonStyle.primary, row=0)
    async def view_stats(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        row = get_loa_request(self.request_id)
        if not row:
            await interaction.response.send_message("Could not find that LOA request.", ephemeral=True)
            return
        member = interaction.guild.get_member(int(row["discord_id"])) if interaction.guild else None
        user_obj = member or bot.get_user(int(row["discord_id"]))
        if user_obj is None:
            try:
                user_obj = await bot.fetch_user(int(row["discord_id"]))
            except Exception:
                user_obj = None
        if user_obj is None:
            await interaction.response.send_message("Could not find that staff member.", ephemeral=True)
            return
        embed = build_staff_stats_embed(user_obj)
        if not embed:
            await interaction.response.send_message("That user is not registered in the tracker.", ephemeral=True)
            return
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="Approve", style=discord.ButtonStyle.success, row=0)
    async def approve(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        row = approve_loa_request(self.request_id, interaction.user.id)
        if not row:
            await interaction.response.send_message("That LOA request is no longer pending.", ephemeral=True)
            await self._disable_self(interaction)
            return
        await interaction.response.send_message(f"Approved LOA request #{self.request_id} for <@{row['discord_id']}>.", ephemeral=True)
        await self._disable_self(interaction)
        await send_loa_log(build_loa_update_embed(f"LOA Approved • Request #{self.request_id}", row), view=ActiveLOAView(int(row["discord_id"])))

    @discord.ui.button(label="Deny", style=discord.ButtonStyle.danger)
    async def deny(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        note = f"Denied via button by <@{interaction.user.id}>"
        row = deny_loa_request(self.request_id, interaction.user.id, note)
        if not row:
            await interaction.response.send_message("That LOA request is no longer pending.", ephemeral=True)
            await self._disable_self(interaction)
            return
        await interaction.response.send_message(f"Denied LOA request #{self.request_id} for <@{row['discord_id']}>.", ephemeral=True)
        await self._disable_self(interaction)
        await send_loa_log(build_loa_update_embed(f"LOA Denied • Request #{self.request_id}", row, note))


class ActiveLOAView(discord.ui.View):
    def __init__(self, discord_id: int):
        super().__init__(timeout=None)
        self.discord_id = discord_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not interaction.guild or not isinstance(interaction.user, discord.Member) or not is_allowed_guild(interaction.guild):
            await interaction.response.send_message("This button can only be used in the target server.", ephemeral=True)
            return False
        if not is_management(interaction.user):
            await interaction.response.send_message("You do not have permission to use this button.", ephemeral=True)
            return False
        return True

    async def _disable_self(self, interaction: discord.Interaction) -> None:
        for child in self.children:
            child.disabled = True
        try:
            await interaction.message.edit(view=self)
        except Exception:
            pass

    @discord.ui.button(label="View Stats", style=discord.ButtonStyle.primary, row=0)
    async def view_stats(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        member = interaction.guild.get_member(self.discord_id) if interaction.guild else None
        user_obj = member or bot.get_user(self.discord_id)
        if user_obj is None:
            try:
                user_obj = await bot.fetch_user(self.discord_id)
            except Exception:
                user_obj = None
        if user_obj is None:
            await interaction.response.send_message("Could not find that staff member.", ephemeral=True)
            return
        embed = build_staff_stats_embed(user_obj)
        if not embed:
            await interaction.response.send_message("That user is not registered in the tracker.", ephemeral=True)
            return
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="Remove LOA Early", style=discord.ButtonStyle.secondary, row=0)
    async def remove(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        row = end_active_loa_for_member(self.discord_id)
        if not row:
            await interaction.response.send_message("That staff member does not have an active LOA.", ephemeral=True)
            await self._disable_self(interaction)
            return
        dm_target = interaction.guild.get_member(self.discord_id) if interaction.guild else None
        if dm_target is None:
            dm_target = await resolve_user_for_dm(self.discord_id)
        if dm_target is not None:
            await dm_loa_removed_notice(dm_target, removed_by=interaction.user.id)
        await interaction.response.send_message(f"Removed <@{self.discord_id}>'s active LOA early.", ephemeral=True)
        await self._disable_self(interaction)
        await send_loa_log(build_loa_update_embed("LOA Removed Early", row, f"Removed via button by <@{interaction.user.id}>"))


def build_staff_stats_embed(member: discord.Member | discord.User) -> discord.Embed | None:
    staff = get_staff_member(member.id)
    if not staff:
        return None
    stats = fetch_weekly_stats(member.id)
    strikes = active_strike_count(member.id)
    status = status_label_for(member.id, int(stats["minecraft_seconds"]), bool(staff["strike_eligible"]), week_key_for())
    status_reason = recommendation_reason_for(member.id, int(stats["minecraft_seconds"]), bool(staff["strike_eligible"]), week_key_for())
    loa_row = get_active_loa_for_member(member.id)
    embed = discord.Embed(title=f"Weekly stats for {member}", colour=discord.Colour.blurple())
    embed.add_field(name="Minecraft Playtime", value=human_hours(stats["minecraft_seconds"]), inline=True)
    embed.add_field(name="Discord Activity", value=human_hours(stats["discord_seconds"]), inline=True)
    embed.add_field(name="Active Strikes", value=str(strikes), inline=True)
    embed.add_field(name="Status", value=f"{status_emoji(status)} {status}", inline=True)
    embed.add_field(name="Recommendation Note", value=status_reason[:1024], inline=False)
    embed.add_field(name="LOA", value=(f"{loa_row['start_date']} → {loa_row['end_date']}" if loa_row else "None"), inline=True)
    embed.add_field(
        name="Discord Punishments",
        value=(
            f"Warns: {stats['discord_warns']}\n"
            f"Kicks: {stats['discord_kicks']}\n"
            f"Bans: {stats['discord_bans']}\n"
            f"Mutes: {stats['discord_mutes']}"
        ),
        inline=True,
    )
    embed.add_field(
        name="Minecraft Punishments",
        value=(
            f"Warns: {stats['mc_warns']}\n"
            f"Kicks: {stats['mc_kicks']}\n"
            f"Bans: {stats['mc_bans']}\n"
            f"Mutes: {stats['mc_mutes']}"
        ),
        inline=True,
    )
    embed.add_field(name="Minecraft Name", value=staff["minecraft_name"] or "Not set", inline=True)
    if staff["notes"]:
        embed.add_field(name="Notes", value=str(staff["notes"])[:1024], inline=False)
    return embed



class StaffStatsButtonView(discord.ui.View):
    def __init__(self, discord_id: int, *, allow_remove_strike: bool = False, evidence_url: str | None = None):
        super().__init__(timeout=None)
        self.discord_id = discord_id
        if not allow_remove_strike:
            self.remove_item(self.remove_one)
        if evidence_url:
            self.add_item(discord.ui.Button(label="Open Evidence", style=discord.ButtonStyle.link, url=evidence_url))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not interaction.guild or not isinstance(interaction.user, discord.Member) or not is_allowed_guild(interaction.guild):
            await interaction.response.send_message("This button can only be used in the target server.", ephemeral=True)
            return False
        if not is_management(interaction.user):
            await interaction.response.send_message("You do not have permission to use this button.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="View Stats", style=discord.ButtonStyle.primary)
    async def view_stats(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        member = interaction.guild.get_member(self.discord_id) if interaction.guild else None
        user_obj: discord.abc.User | None = member or bot.get_user(self.discord_id)
        if user_obj is None:
            try:
                user_obj = await bot.fetch_user(self.discord_id)
            except Exception:
                user_obj = None
        if user_obj is None:
            await interaction.response.send_message("Could not find that staff member.", ephemeral=True)
            return
        embed = build_staff_stats_embed(user_obj)
        if not embed:
            await interaction.response.send_message("That user is not registered in the tracker.", ephemeral=True)
            return
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="Remove 1 Strike", style=discord.ButtonStyle.secondary)
    async def remove_one(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        total = remove_strikes(self.discord_id, 1)
        member = interaction.guild.get_member(self.discord_id) if interaction.guild else None
        display = member.mention if member else f"<@{self.discord_id}>"
        dm_target = member or await resolve_user_for_dm(self.discord_id)
        if dm_target is not None:
            await dm_strike_removed_notice(dm_target, 1, total, removed_by=interaction.user.id)
        embed = discord.Embed(title="Strike Removed", colour=discord.Colour.green())
        embed.add_field(name="Staff", value=display, inline=False)
        embed.add_field(name="Removed", value="1")
        embed.add_field(name="Active Strikes", value=str(total))
        embed.add_field(name="Removed By", value=f"<@{interaction.user.id}>", inline=False)
        await send_log(embed, view=StaffStatsButtonView(self.discord_id, allow_remove_strike=False))
        await interaction.response.send_message(f"Removed 1 strike from {display}. Active strikes: {total}.", ephemeral=True)


class IncidentLogView(discord.ui.View):
    def __init__(self, incident_id: int, *, discord_id: int | None = None, evidence_url: str | None = None):
        super().__init__(timeout=None)
        self.incident_id = incident_id
        self.discord_id = discord_id
        self.view_stats.disabled = discord_id is None
        if evidence_url:
            self.add_item(discord.ui.Button(label="Open Evidence", style=discord.ButtonStyle.link, url=evidence_url))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not interaction.guild or not isinstance(interaction.user, discord.Member) or not is_allowed_guild(interaction.guild):
            await interaction.response.send_message("This button can only be used in the target server.", ephemeral=True)
            return False
        if not is_management(interaction.user):
            await interaction.response.send_message("You do not have permission to use this button.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="View Stats", style=discord.ButtonStyle.primary)
    async def view_stats(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if self.discord_id is None:
            await interaction.response.send_message("This incident is not linked to a staff member.", ephemeral=True)
            return
        member = interaction.guild.get_member(self.discord_id) if interaction.guild else None
        user_obj: discord.abc.User | None = member or bot.get_user(self.discord_id)
        if user_obj is None:
            try:
                user_obj = await bot.fetch_user(self.discord_id)
            except Exception:
                user_obj = None
        if user_obj is None:
            await interaction.response.send_message("Could not find that staff member.", ephemeral=True)
            return
        embed = build_staff_stats_embed(user_obj)
        if not embed:
            await interaction.response.send_message("That user is not registered in the tracker.", ephemeral=True)
            return
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="Remove Incident", style=discord.ButtonStyle.secondary)
    async def remove_incident(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        row = remove_incident_log(self.incident_id)
        if not row:
            await interaction.response.send_message("That incident was already removed or could not be found.", ephemeral=True)
            return
        embed = discord.Embed(title="Staff Incident Removed", colour=discord.Colour.green())
        embed.add_field(name="Incident ID", value=str(self.incident_id), inline=True)
        embed.add_field(name="Severity", value=str(row["severity"]).title(), inline=True)
        embed.add_field(name="Removed By", value=f"<@{interaction.user.id}>", inline=True)
        embed.add_field(name="Summary", value=str(row["summary"]), inline=False)
        if row["subject_discord_id"]:
            embed.add_field(name="Staff", value=f"<@{int(row['subject_discord_id'])}>", inline=False)
        await send_log(embed)
        for child in self.children:
            child.disabled = True if isinstance(child, discord.ui.Button) and child.style is not discord.ButtonStyle.link else child.disabled
        try:
            if interaction.message and interaction.message.embeds:
                removed_embed = interaction.message.embeds[0].copy()
                removed_embed.title = f"Staff Incident Removed #{self.incident_id}"
                removed_embed.colour = discord.Colour.dark_grey()
                await interaction.response.edit_message(embed=removed_embed, view=self)
            else:
                await interaction.response.send_message(f"Removed incident #{self.incident_id}.", ephemeral=True)
        except Exception:
            await interaction.response.send_message(f"Removed incident #{self.incident_id}.", ephemeral=True)

@staff_group.command(name="stats", description="Show this week's staff stats")
@management_check
@app_commands.describe(member="Staff member to inspect")
async def staff_stats(interaction: discord.Interaction, member: discord.Member) -> None:
    embed = build_staff_stats_embed(member)
    if not embed:
        await interaction.response.send_message("That user is not registered in the tracker.", ephemeral=True)
        return
    await interaction.response.send_message(embed=embed, ephemeral=True)


@staff_group.command(name="profile", description="Show staff identity, standing, and record summary")
@management_check
@app_commands.describe(member="Staff member to inspect")
async def staff_profile(interaction: discord.Interaction, member: discord.Member) -> None:
    embed = build_staff_profile_embed(member)
    if not embed:
        await interaction.response.send_message("That user is not registered in the tracker.", ephemeral=True)
        return
    await interaction.response.send_message(embed=embed, ephemeral=True)


@staff_group.command(name="history", description="Show recent staff record history")
@management_check
@app_commands.describe(member="Staff member to inspect")
async def staff_history(interaction: discord.Interaction, member: discord.Member) -> None:
    embed = build_staff_history_embed(member)
    if not embed:
        await interaction.response.send_message("That user is not registered in the tracker.", ephemeral=True)
        return
    await interaction.response.send_message(embed=embed, ephemeral=True)


@staff_group.command(name="activity_history", description="Show multi-week activity history and trends")
@management_check
@app_commands.describe(member="Staff member to inspect")
async def activity_history(interaction: discord.Interaction, member: discord.Member) -> None:
    embed = build_staff_activity_history_embed(member)
    if not embed:
        await interaction.response.send_message("That user is not registered in the tracker.", ephemeral=True)
        return
    await interaction.response.send_message(embed=embed, ephemeral=True)



@staff_group.command(name="strike_add", description="Manually issue a strike and DM the staff member")
@management_check
@app_commands.describe(
    member="Staff member",
    severity="Minor, major, or severe",
    reason_code="Preset strike reason",
    details="Optional extra details",
    amount_override="Optional custom strike amount",
    evidence_url="Optional proof link or clip URL",
    permanent="Keep this strike from auto-expiring",
    send_dm="Whether to DM the user"
)
@app_commands.choices(
    severity=[
        app_commands.Choice(name="minor", value="minor"),
        app_commands.Choice(name="major", value="major"),
        app_commands.Choice(name="severe", value="severe"),
    ],
    reason_code=[
        app_commands.Choice(name="Activity / Idling", value="activity_idling"),
        app_commands.Choice(name="Professionalism", value="professionalism"),
        app_commands.Choice(name="Communication", value="communication"),
        app_commands.Choice(name="Poor Judgment", value="judgment"),
        app_commands.Choice(name="Disrespect", value="disrespect"),
        app_commands.Choice(name="Policy Violation", value="policy_violation"),
        app_commands.Choice(name="False Punishment", value="false_punishment"),
        app_commands.Choice(name="Abuse of Power", value="abuse_of_power"),
        app_commands.Choice(name="Insubordination", value="insubordination"),
        app_commands.Choice(name="Other", value="other"),
    ],
)
async def strike_add(
    interaction: discord.Interaction,
    member: discord.Member,
    severity: app_commands.Choice[str],
    reason_code: app_commands.Choice[str],
    details: str = "",
    amount_override: app_commands.Range[int, 0, 10] = 0,
    evidence_url: str = "",
    permanent: bool = False,
    send_dm: bool = True,
) -> None:
    amount = int(amount_override if amount_override > 0 else SEVERITY_AMOUNTS[severity.value])
    reason = format_manual_strike_reason(severity.value, reason_code.value, details)
    evidence = evidence_url.strip() or None
    total = await apply_strike_and_notify(
        member,
        amount,
        reason,
        kind="manual",
        issued_by=interaction.user.id,
        send_dm=send_dm,
        evidence_url=evidence,
        is_permanent=permanent,
    )
    extra = f" Evidence: {evidence}" if evidence else ""
    permanence_note = " Permanent." if permanent else " Auto-expires."
    await interaction.response.send_message(
        f"Issued {amount} strike(s) to {member.mention}. Reason: {reason}. Active strikes: {total}.{extra}{permanence_note}",
        ephemeral=True,
    )





@staff_group.command(name="incident_log", description="Log a serious staff incident with optional evidence")
@management_check
@app_commands.describe(
    member="Optional staff member involved",
    severity="How serious the incident is",
    summary="Short incident title",
    details="What happened",
    evidence_url="Optional proof link or clip URL",
)
@app_commands.choices(
    severity=[
        app_commands.Choice(name="info", value="info"),
        app_commands.Choice(name="concern", value="concern"),
        app_commands.Choice(name="major", value="major"),
        app_commands.Choice(name="severe", value="severe"),
    ],
)
async def incident_log(
    interaction: discord.Interaction,
    severity: app_commands.Choice[str],
    summary: str,
    details: str,
    member: Optional[discord.Member] = None,
    evidence_url: str = "",
) -> None:
    subject_id = member.id if member else None
    evidence = evidence_url.strip() or None
    incident_id = add_incident_log(subject_id, severity.value, summary.strip(), details.strip(), evidence, interaction.user.id)

    colour_map = {
        "info": discord.Colour.blurple(),
        "concern": discord.Colour.gold(),
        "major": discord.Colour.orange(),
        "severe": discord.Colour.red(),
    }
    embed = discord.Embed(title=f"Staff Incident Logged #{incident_id}", colour=colour_map.get(severity.value, discord.Colour.blurple()))
    embed.add_field(name="Severity", value=severity.value.title(), inline=True)
    embed.add_field(name="Summary", value=summary.strip(), inline=True)
    embed.add_field(name="Logged By", value=f"<@{interaction.user.id}>", inline=True)
    if member:
        embed.add_field(name="Staff", value=f"{member.mention} ({member.id})", inline=False)
    embed.add_field(name="Details", value=details.strip()[:1024], inline=False)
    if evidence:
        embed.add_field(name="Evidence", value=evidence, inline=False)

    view = IncidentLogView(incident_id, discord_id=member.id if member else None, evidence_url=evidence)
    await send_log(embed, view=view)
    await interaction.response.send_message(f"Logged incident #{incident_id}.", ephemeral=True)


@staff_group.command(name="note_set", description="Set or replace private management notes for a staff member")
@management_check
@app_commands.describe(member="Staff member", notes="Private note text")
async def note_set(interaction: discord.Interaction, member: discord.Member, notes: str) -> None:
    if not get_staff_member(member.id):
        await interaction.response.send_message("That user is not registered in the tracker.", ephemeral=True)
        return
    set_staff_notes(member.id, notes.strip(), interaction.user.id)
    await interaction.response.send_message(f"Updated notes for {member.mention}.", ephemeral=True)


@staff_group.command(name="note_view", description="View private management notes for a staff member")
@management_check
@app_commands.describe(member="Staff member")
async def note_view(interaction: discord.Interaction, member: discord.Member) -> None:
    staff = get_staff_member(member.id)
    if not staff:
        await interaction.response.send_message("That user is not registered in the tracker.", ephemeral=True)
        return
    notes = (staff["notes"] or "").strip()
    await interaction.response.send_message(notes or "No notes stored for that staff member.", ephemeral=True)


@staff_group.command(name="note_clear", description="Clear private management notes for a staff member")
@management_check
@app_commands.describe(member="Staff member")
async def note_clear(interaction: discord.Interaction, member: discord.Member) -> None:
    if not get_staff_member(member.id):
        await interaction.response.send_message("That user is not registered in the tracker.", ephemeral=True)
        return
    set_staff_notes(member.id, None, interaction.user.id)
    await interaction.response.send_message(f"Cleared notes for {member.mention}.", ephemeral=True)



@staff_group.command(name="recommendation", description="Show current recommendation status and why")
@management_check
@app_commands.describe(member="Staff member to evaluate")
async def recommendation_view(interaction: discord.Interaction, member: discord.Member) -> None:
    staff = get_staff_member(member.id)
    if not staff:
        await interaction.response.send_message("That user is not registered in the tracker.", ephemeral=True)
        return
    stats = fetch_weekly_stats(member.id)
    status = status_label_for(member.id, int(stats["minecraft_seconds"]), bool(staff["strike_eligible"]), week_key_for())
    reason = recommendation_reason_for(member.id, int(stats["minecraft_seconds"]), bool(staff["strike_eligible"]), week_key_for())
    rows = fetch_member_week_rows(member.id, 4)
    streak = 0
    minimum_seconds = int(float(CONFIG["minimum_weekly_hours"]) * 3600)
    for row in rows:
        if int(row["minecraft_seconds"]) >= minimum_seconds:
            streak += 1
        else:
            break
    embed = discord.Embed(title=f"Recommendation Status • {member}", colour=discord.Colour.blurple())
    embed.add_field(name="Status", value=f"{status_emoji(status)} {status}", inline=True)
    embed.add_field(name="Current MC Playtime", value=human_hours(int(stats["minecraft_seconds"])), inline=True)
    embed.add_field(name="Active Strikes", value=str(active_strike_count(member.id)), inline=True)
    embed.add_field(name="Reason", value=reason[:1024], inline=False)
    embed.add_field(name="Consistency", value=f"{streak} week(s) currently meeting requirement", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)




def member_display_for_leaderboard(guild: discord.Guild, discord_id: int) -> str:
    member = guild.get_member(discord_id)
    if member:
        return member.mention
    return f"<@{discord_id}>"


def consistency_streak_weeks(discord_id: int, limit: int = 4) -> int:
    rows = fetch_member_week_rows(discord_id, limit)
    minimum_seconds = int(float(CONFIG["minimum_weekly_hours"]) * 3600)
    streak = 0
    for row in rows:
        if int(row["minecraft_seconds"]) >= minimum_seconds:
            streak += 1
        else:
            break
    return streak


def build_private_leaderboard_embed(guild: discord.Guild, wk: str) -> discord.Embed | None:
    rows = fetch_all_week_rows(wk)
    if not rows:
        return None

    scored: list[dict[str, Any]] = []
    for row in rows:
        discord_id = int(row["discord_id"])
        mc_seconds = int(row["minecraft_seconds"])
        strikes = active_strike_count(discord_id)
        streak = consistency_streak_weeks(discord_id, 4)
        status = status_label_for(discord_id, mc_seconds, bool(row["strike_eligible"]), wk)
        total_punishments = (
            int(row["discord_warns"]) + int(row["discord_kicks"]) + int(row["discord_bans"]) + int(row["discord_mutes"])
            + int(row["mc_warns"]) + int(row["mc_kicks"]) + int(row["mc_bans"]) + int(row["mc_mutes"])
        )
        scored.append({
            "discord_id": discord_id,
            "mc_seconds": mc_seconds,
            "strikes": strikes,
            "streak": streak,
            "status": status,
            "punishments": total_punishments,
        })

    if not scored:
        return None

    eligible = [x for x in scored if x["status"] != "Needs Review"]
    top_activity = sorted(eligible, key=lambda x: (-x["mc_seconds"], x["strikes"], -x["streak"]))[:5]
    top_clean = sorted(
        [x for x in eligible if x["strikes"] == 0],
        key=lambda x: (-x["streak"], -x["mc_seconds"], -x["punishments"])
    )[:5]
    if not top_clean:
        top_clean = sorted(eligible, key=lambda x: (x["strikes"], -x["streak"], -x["mc_seconds"]))[:5]
    top_consistency = sorted(eligible, key=lambda x: (-x["streak"], -x["mc_seconds"], x["strikes"]))[:5]

    def format_block(entries: list[dict[str, Any]], mode: str) -> str:
        lines: list[str] = []
        for idx, item in enumerate(entries, start=1):
            member_txt = member_display_for_leaderboard(guild, int(item["discord_id"]))
            if mode == "activity":
                lines.append(
                    f"**{idx}.** {member_txt} — `{human_hours(int(item['mc_seconds']))}` • {status_emoji(str(item['status']))} {item['status']}"
                )
            elif mode == "clean":
                lines.append(
                    f"**{idx}.** {member_txt} — `{int(item['strikes'])} strike(s)` • `{int(item['streak'])}` week streak • {status_emoji(str(item['status']))} {item['status']}"
                )
            else:
                lines.append(
                    f"**{idx}.** {member_txt} — `{int(item['streak'])}` week streak • `{human_hours(int(item['mc_seconds']))}`"
                )
        return "\n".join(lines) if lines else "None"

    embed = discord.Embed(
        title="Staff Leaderboard",
        description="Current week snapshot.",
        colour=discord.Colour.gold(),
    )
    embed.add_field(name="Top Activity", value=format_block(top_activity, "activity"), inline=False)
    embed.add_field(name="Cleanest Record", value=format_block(top_clean, "clean"), inline=False)
    embed.add_field(name="Best Consistency", value=format_block(top_consistency, "streak"), inline=False)
    embed.set_footer(text=f"Week key: {wk}")
    return embed




def build_management_overview_embed(guild: discord.Guild, wk: str) -> discord.Embed | None:
    staff_rows = list_staff_members()
    if not staff_rows:
        return None

    minimum_seconds = int(float(CONFIG["minimum_weekly_hours"]) * 3600)
    lower_role_ids = set(CONFIG.get("auto_strike_role_ids", []))

    scored: list[dict[str, Any]] = []
    for staff in staff_rows:
        discord_id = int(staff["discord_id"])
        member = guild.get_member(discord_id)
        stats = fetch_weekly_stats(discord_id)
        mc_seconds = int(stats["minecraft_seconds"])
        punishments = (
            int(stats["discord_warns"]) + int(stats["discord_kicks"]) + int(stats["discord_bans"]) + int(stats["discord_mutes"])
            + int(stats["mc_warns"]) + int(stats["mc_kicks"]) + int(stats["mc_bans"]) + int(stats["mc_mutes"])
        )
        strikes = active_strike_count(discord_id)
        permanent_strikes = count_permanent_active_strikes(discord_id)
        streak = consistency_streak_weeks(discord_id, 4)
        active_loa = get_active_loa_for_member(discord_id)
        status = status_label_for(discord_id, mc_seconds, bool(staff["strike_eligible"]), wk)

        is_lower = bool(staff["strike_eligible"])
        if member is not None and lower_role_ids:
            is_lower = any(role.id in lower_role_ids for role in member.roles)

        scored.append({
            "discord_id": discord_id,
            "mention": member.mention if member else f"<@{discord_id}>",
            "display": member_display_for_leaderboard(guild, discord_id),
            "mc_seconds": mc_seconds,
            "punishments": punishments,
            "strikes": strikes,
            "permanent_strikes": permanent_strikes,
            "streak": streak,
            "status": status,
            "is_lower": is_lower,
            "active_loa": active_loa,
            "needs_attention": status in {"Watchlist", "Needs Review"} or strikes > 0,
            "zero_activity": mc_seconds == 0 and is_lower and active_loa is None,
        })

    lower_staff = [x for x in scored if x["is_lower"]]
    watchlist = [x for x in scored if x["status"] == "Watchlist"]
    needs_review = [x for x in scored if x["status"] == "Needs Review"]
    excused = [x for x in scored if x["status"] == "Excused"]
    zero_activity = [x for x in lower_staff if x["zero_activity"]]
    top_performers = sorted(
        [x for x in scored if x["status"] in {"Excellent", "Good Standing"}],
        key=lambda x: (-x["mc_seconds"], x["strikes"], -x["streak"], -x["punishments"])
    )[:5]
    top_concerns = sorted(
        [x for x in scored if x["needs_attention"]],
        key=lambda x: (0 if x["status"] == "Needs Review" else 1, -x["strikes"], x["mc_seconds"])
    )[:5]

    total_active_strike_members = sum(1 for x in scored if x["strikes"] > 0)
    total_loa = sum(1 for x in scored if x["active_loa"] is not None)
    total_zero_activity = len(zero_activity)
    total_needs_attention = len(watchlist) + len(needs_review)

    def fmt(entries: list[dict[str, Any]], mode: str) -> str:
        if not entries:
            return "None."
        lines: list[str] = []
        for item in entries[:5]:
            mention = item["mention"]
            if mode == "concern":
                lines.append(
                    f"{mention} — {status_emoji(item['status'])} {item['status']} • `{item['strikes']}` strike(s) • `{human_hours(item['mc_seconds'])}`"
                )
            elif mode == "excused":
                loa = item["active_loa"]
                if loa is not None:
                    lines.append(f"{mention} — Excused until `{loa['end_date']}`")
                else:
                    lines.append(f"{mention} — Excused")
            elif mode == "zero":
                lines.append(f"{mention} — `0h 00m` this week")
            else:
                lines.append(
                    f"{mention} — `{human_hours(item['mc_seconds'])}` • `{item['streak']}` week streak • {status_emoji(item['status'])} {item['status']}"
                )
        return "\n".join(lines)

    embed = discord.Embed(title="Staff Management Overview", colour=discord.Colour.dark_purple())
    embed.add_field(
        name="Quick Totals",
        value=(
            f"**Registered Staff:** {len(scored)}\n"
            f"**Lower Staff Tracked:** {len(lower_staff)}\n"
            f"**On Active LOA:** {total_loa}\n"
            f"**With Active Strikes:** {total_active_strike_members}\n"
            f"**Needs Attention:** {total_needs_attention}\n"
            f"**Zero Activity (Lower Staff):** {total_zero_activity}"
        ),
        inline=False,
    )
    embed.add_field(name="Top Concerns", value=fmt(top_concerns, "concern"), inline=False)
    embed.add_field(name="Excused / LOA", value=fmt(excused, "excused"), inline=False)
    embed.add_field(name="Top Performers This Week", value=fmt(top_performers, "performers"), inline=False)
    embed.add_field(name="Lower Staff With 0 Activity", value=fmt(zero_activity, "zero"), inline=False)
    embed.set_footer(text=f"Week key: {wk} • Requirement: {human_hours(minimum_seconds)}")
    return embed

@staff_group.command(name="leaderboard", description="Show the private staff leaderboard")
@management_check
async def leaderboard_view(interaction: discord.Interaction) -> None:
    embed = build_private_leaderboard_embed(interaction.guild, week_key_for())
    if not embed:
        await interaction.response.send_message("No weekly staff data exists yet.", ephemeral=True)
        return
    await interaction.response.send_message(embed=embed, ephemeral=True)

@staff_group.command(name="overview", description="Show the main management overview panel")
@management_check
async def staff_overview(interaction: discord.Interaction) -> None:
    if interaction.guild is None:
        await interaction.response.send_message("This command must be used in the server.", ephemeral=True)
        return
    embed = build_management_overview_embed(interaction.guild, week_key_for())
    if not embed:
        await interaction.response.send_message("No registered staff data exists yet.", ephemeral=True)
        return
    await interaction.response.send_message(embed=embed, view=OverviewDashboardView(), ephemeral=True)



@staff_group.command(name="offboard", description="Offboard a staff member and stop active tracking")
@management_check
@app_commands.describe(member="Staff member to offboard", offboard_type="Type of offboarding", reason="Reason", evidence_url="Optional evidence link", dm_member="Whether to DM the member")
@app_commands.choices(
    offboard_type=[
        app_commands.Choice(name="Resignation", value="resignation"),
        app_commands.Choice(name="Removal", value="removal"),
        app_commands.Choice(name="Demotion Out", value="demotion"),
        app_commands.Choice(name="Blacklist", value="blacklist"),
        app_commands.Choice(name="Archive", value="archive"),
    ]
)
async def offboard_staff_command(
    interaction: discord.Interaction,
    member: discord.Member,
    offboard_type: app_commands.Choice[str],
    reason: str,
    evidence_url: str | None = None,
    dm_member: bool = True,
) -> None:
    row = offboard_staff_member(member.id, offboard_type=offboard_type.value, reason=reason, acted_by=interaction.user.id, evidence_url=evidence_url)
    if not row:
        await interaction.response.send_message("That staff member is not registered.", ephemeral=True)
        return
    try:
        if dm_member:
            embed = discord.Embed(title="Staff Status Updated", colour=discord.Colour.orange())
            embed.description = f"You were marked as **{offboard_type.name}** in the staff database."
            embed.add_field(name="Reason", value=reason, inline=False)
            if evidence_url:
                embed.add_field(name="Evidence", value=evidence_url, inline=False)
            await member.send(embed=embed)
    except Exception:
        pass

    try:
        create_incident_log(member.id, "severe", f"Offboarded • {offboard_type.name}", reason, evidence_url, interaction.user.id)
    except Exception:
        pass

    embed = discord.Embed(title="Staff Offboarded", colour=discord.Colour.orange())
    embed.add_field(name="Staff", value=member.mention, inline=False)
    embed.add_field(name="Type", value=offboard_type.name, inline=True)
    embed.add_field(name="Reason", value=reason, inline=False)
    if evidence_url:
        embed.add_field(name="Evidence", value=evidence_url, inline=False)
    await send_offboard_log(embed, view=StaffStatsButtonView(member.id, allow_remove_strike=False, evidence_url=evidence_url))
    await interaction.response.send_message(f"Offboarded {member.mention} as **{offboard_type.name}**.", ephemeral=True)


@staff_group.command(name="reinstate", description="Reinstate an offboarded staff member")
@management_check
@app_commands.describe(member="Staff member to reinstate", minecraft_name="Minecraft username to restore", strike_eligible="Auto strike eligible after reinstating")
async def reinstate_staff_command(
    interaction: discord.Interaction,
    member: discord.Member,
    minecraft_name: str | None = None,
    strike_eligible: bool = False,
) -> None:
    existing = get_staff_member(member.id)
    if not existing:
        await interaction.response.send_message("That staff member is not registered.", ephemeral=True)
        return
    reinstate_staff_member(member.id, acted_by=interaction.user.id)
    upsert_staff_member(member.id, minecraft_name or existing["minecraft_name"], strike_eligible, existing["notes"])
    ensure_week_row(member.id)
    embed = discord.Embed(title="Staff Reinstated", colour=discord.Colour.green())
    embed.add_field(name="Staff", value=member.mention, inline=False)
    embed.add_field(name="Minecraft Name", value=f"`{minecraft_name or existing['minecraft_name'] or 'None'}`", inline=True)
    embed.add_field(name="Strike Eligible", value="Yes" if strike_eligible else "No", inline=True)
    await send_offboard_log(embed, view=StaffStatsButtonView(member.id))
    await interaction.response.send_message(f"Reinstated {member.mention}.", ephemeral=True)


@staff_group.command(name="role_sync_audit", description="Audit role/database mismatches")
@management_check
async def role_sync_audit(interaction: discord.Interaction) -> None:
    if interaction.guild is None:
        await interaction.response.send_message("This command must be used in the server.", ephemeral=True)
        return
    embed = build_role_sync_embed(interaction.guild)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@staff_group.command(name="reminders", description="Show current management reminders")
@management_check
async def staff_reminders(interaction: discord.Interaction) -> None:
    if interaction.guild is None:
        await interaction.response.send_message("This command must be used in the server.", ephemeral=True)
        return
    embed = build_management_reminders_embed(interaction.guild, week_key_for())
    if not embed:
        await interaction.response.send_message("No management reminders are active right now.", ephemeral=True)
        return
    await interaction.response.send_message(embed=embed, ephemeral=True)




@staff_group.command(name="weekly_archive", description="Show archived weekly report summaries")
@management_check
async def weekly_archive_command(interaction: discord.Interaction) -> None:
    embeds = build_archive_embeds(8)
    if not embeds:
        await interaction.response.send_message("No archived weekly data exists yet.", ephemeral=True)
        return
    await interaction.response.send_message(embed=embeds[0], ephemeral=True)
    for embed in embeds[1:]:
        await interaction.followup.send(embed=embed, ephemeral=True)

@staff_group.command(name="report_now", description="Preview the current weekly report")
@management_check
async def report_now(interaction: discord.Interaction) -> None:
    embeds = build_weekly_report_embeds(week_key_for(), "Current Weekly Staff Report")
    if not embeds:
        await interaction.response.send_message("No weekly staff data exists yet.", ephemeral=True)
        return
    await interaction.response.send_message(embed=embeds[0], ephemeral=True)
    for embed in embeds[1:]:
        await interaction.followup.send(embed=embed, ephemeral=True)


@staff_group.command(name="strike_remove", description="Remove active strikes from a staff member")
@management_check
@app_commands.describe(member="Staff member", amount="How many active strikes to remove")
async def strike_remove(interaction: discord.Interaction, member: discord.Member, amount: app_commands.Range[int, 1, 10]) -> None:
    total = remove_strikes(member.id, amount)
    await dm_strike_removed_notice(member, amount, total, removed_by=interaction.user.id)
    embed = discord.Embed(title="Strikes Removed", colour=discord.Colour.green())
    embed.add_field(name="Staff", value=f"{member} ({member.id})", inline=False)
    embed.add_field(name="Removed", value=str(amount))
    embed.add_field(name="Active Strikes", value=str(total))
    await send_log(embed, view=StaffStatsButtonView(member.id, allow_remove_strike=False))
    await interaction.response.send_message(
        f"Removed up to {amount} strike(s) from {member.mention}. Active strikes: {total}.",
        ephemeral=True,
    )


@mod_group.command(name="warn", description="Warn a member and track it for the moderator")
@management_check
async def mod_warn(interaction: discord.Interaction, target: discord.Member, moderator: discord.Member, reason: str) -> None:
    if not get_staff_member(moderator.id):
        await interaction.response.send_message("That moderator is not registered in the tracker.", ephemeral=True)
        return
    increment_punishment(moderator.id, "discord", "warn", interaction.user.id, str(target), reason)
    try:
        await target.send(f"You were warned in {interaction.guild.name}. Reason: {reason}")
    except discord.Forbidden:
        pass
    await interaction.response.send_message(
        f"Tracked warn for {moderator.mention} against {target.mention}.",
        ephemeral=True,
    )


@mod_group.command(name="kick", description="Kick a member and track it for the moderator")
@management_check
async def mod_kick(interaction: discord.Interaction, target: discord.Member, moderator: discord.Member, reason: str) -> None:
    if not get_staff_member(moderator.id):
        await interaction.response.send_message("That moderator is not registered in the tracker.", ephemeral=True)
        return
    await interaction.guild.kick(target, reason=reason)
    increment_punishment(moderator.id, "discord", "kick", interaction.user.id, str(target), reason)
    await interaction.response.send_message(
        f"Kicked {target.mention} and tracked the action for {moderator.mention}.",
        ephemeral=True,
    )


@mod_group.command(name="mute", description="Timeout a member and track it for the moderator")
@management_check
@app_commands.describe(duration_minutes="Timeout duration in minutes")
async def mod_mute(interaction: discord.Interaction, target: discord.Member, moderator: discord.Member, duration_minutes: app_commands.Range[int, 1, 40320], reason: str) -> None:
    if not get_staff_member(moderator.id):
        await interaction.response.send_message("That moderator is not registered in the tracker.", ephemeral=True)
        return
    until = discord.utils.utcnow() + timedelta(minutes=duration_minutes)
    await target.edit(timed_out_until=until, reason=reason)
    increment_punishment(moderator.id, "discord", "mute", interaction.user.id, str(target), reason)
    await interaction.response.send_message(
        f"Timed out {target.mention} and tracked the action for {moderator.mention}.",
        ephemeral=True,
    )


@mod_group.command(name="ban", description="Ban a member and track it for the moderator")
@management_check
async def mod_ban(interaction: discord.Interaction, target: discord.Member, moderator: discord.Member, reason: str) -> None:
    if not get_staff_member(moderator.id):
        await interaction.response.send_message("That moderator is not registered in the tracker.", ephemeral=True)
        return
    await interaction.guild.ban(target, reason=reason, delete_message_seconds=0)
    increment_punishment(moderator.id, "discord", "ban", interaction.user.id, str(target), reason)
    await interaction.response.send_message(
        f"Banned {target.mention} and tracked the action for {moderator.mention}.",
        ephemeral=True,
    )


@admin_group.command(name="log_mc_punishment", description="Manually log an in-game punishment for a staff member")
@management_check
@app_commands.describe(action_type="warn/kick/ban/mute")
@app_commands.choices(action_type=[
    app_commands.Choice(name="warn", value="warn"),
    app_commands.Choice(name="kick", value="kick"),
    app_commands.Choice(name="ban", value="ban"),
    app_commands.Choice(name="mute", value="mute"),
])
async def log_mc_punishment(interaction: discord.Interaction, moderator: discord.Member, action_type: app_commands.Choice[str], target_name: str, reason: str) -> None:
    if not get_staff_member(moderator.id):
        await interaction.response.send_message("That moderator is not registered in the tracker.", ephemeral=True)
        return
    increment_punishment(moderator.id, "minecraft", action_type.value, interaction.user.id, target_name, reason)
    await interaction.response.send_message(
        f"Logged Minecraft {action_type.value} for {moderator.mention}.",
        ephemeral=True,
    )


@admin_group.command(name="mc_login", description="Manually start a Minecraft session for a staff member")
@management_check
async def mc_login(interaction: discord.Interaction, member: discord.Member) -> None:
    if not get_staff_member(member.id):
        await interaction.response.send_message("That user is not registered in the tracker.", ephemeral=True)
        return
    open_session(member.id, "minecraft")
    await interaction.response.send_message(f"Opened Minecraft session for {member.mention}.", ephemeral=True)


@admin_group.command(name="mc_logout", description="Manually end a Minecraft session for a staff member")
@management_check
async def mc_logout(interaction: discord.Interaction, member: discord.Member) -> None:
    if not get_staff_member(member.id):
        await interaction.response.send_message("That user is not registered in the tracker.", ephemeral=True)
        return
    seconds = close_session(member.id, "minecraft")
    await interaction.response.send_message(
        f"Closed Minecraft session for {member.mention}. Added {human_hours(seconds)} this week.",
        ephemeral=True,
    )


@admin_group.command(name="add_playtime", description="Manually add playtime to a staff member")
@management_check
@app_commands.describe(minutes="Minutes to add", source="minecraft or discord")
@app_commands.choices(source=[
    app_commands.Choice(name="minecraft", value="minecraft"),
    app_commands.Choice(name="discord", value="discord"),
])
async def add_playtime(interaction: discord.Interaction, member: discord.Member, minutes: app_commands.Range[int, 1, 10080], source: app_commands.Choice[str]) -> None:
    if not get_staff_member(member.id):
        await interaction.response.send_message("That user is not registered in the tracker.", ephemeral=True)
        return
    add_playtime_seconds(member.id, minutes * 60, source.value)
    await interaction.response.send_message(
        f"Added {minutes} minutes of {source.value} activity to {member.mention}.",
        ephemeral=True,
    )




@loa_group.command(name="request", description="Request an LOA")
@app_commands.describe(start_date="YYYY-MM-DD", end_date="YYYY-MM-DD", reason="Why you need the LOA")
async def loa_request(interaction: discord.Interaction, start_date: str, end_date: str, reason: str) -> None:
    if not interaction.guild or not is_allowed_guild(interaction.guild):
        await interaction.response.send_message("This bot is locked to a different server.", ephemeral=True)
        return
    if not get_staff_member(interaction.user.id):
        await interaction.response.send_message("You are not registered as staff in the tracker.", ephemeral=True)
        return
    try:
        start = parse_date_input(start_date)
        end = parse_date_input(end_date)
    except ValueError:
        await interaction.response.send_message("Use dates in YYYY-MM-DD format.", ephemeral=True)
        return
    if end < start:
        await interaction.response.send_message("End date cannot be before start date.", ephemeral=True)
        return
    request_id = create_loa_request(interaction.user.id, interaction.user.id, start, end, reason.strip())
    row = get_loa_request(request_id)
    await interaction.response.send_message(
        f"LOA request #{request_id} submitted for {start.isoformat()} to {end.isoformat()}.",
        ephemeral=True,
    )
    if row:
        await send_loa_log(build_loa_request_embed(row), view=LOARequestView(int(row["id"])))


@loa_group.command(name="approve", description="Approve a pending LOA request")
@management_check
@app_commands.describe(request_id="Pending LOA request ID")
async def loa_approve(interaction: discord.Interaction, request_id: int) -> None:
    row = approve_loa_request(request_id, interaction.user.id)
    if not row:
        await interaction.response.send_message("That LOA request was not found or is no longer pending.", ephemeral=True)
        return
    await interaction.response.send_message(
        f"Approved LOA request #{request_id} for <@{row['discord_id']}>.",
        ephemeral=True,
    )
    await send_loa_log(build_loa_update_embed(f"LOA Approved • Request #{request_id}", row), view=ActiveLOAView(int(row["discord_id"])))


@loa_group.command(name="deny", description="Deny a pending LOA request")
@management_check
@app_commands.describe(request_id="Pending LOA request ID", reviewer_note="Reason for denial")
async def loa_deny(interaction: discord.Interaction, request_id: int, reviewer_note: str) -> None:
    row = deny_loa_request(request_id, interaction.user.id, reviewer_note.strip())
    if not row:
        await interaction.response.send_message("That LOA request was not found or is no longer pending.", ephemeral=True)
        return
    await interaction.response.send_message(
        f"Denied LOA request #{request_id} for <@{row['discord_id']}>.",
        ephemeral=True,
    )
    await send_loa_log(build_loa_update_embed(f"LOA Denied • Request #{request_id}", row, reviewer_note.strip()))


@loa_group.command(name="add", description="Directly add an approved LOA period")
@management_check
@app_commands.describe(member="Staff member", start_date="YYYY-MM-DD", end_date="YYYY-MM-DD", reason="LOA reason")
async def loa_add(interaction: discord.Interaction, member: discord.Member, start_date: str, end_date: str, reason: str) -> None:
    if not get_staff_member(member.id):
        await interaction.response.send_message("That user is not registered in the tracker.", ephemeral=True)
        return
    try:
        start = parse_date_input(start_date)
        end = parse_date_input(end_date)
    except ValueError:
        await interaction.response.send_message("Use dates in YYYY-MM-DD format.", ephemeral=True)
        return
    if end < start:
        await interaction.response.send_message("End date cannot be before start date.", ephemeral=True)
        return
    add_loa_period(member.id, start, end, reason.strip(), interaction.user.id, "manual")
    await interaction.response.send_message(
        f"Added LOA for {member.mention} from {start.isoformat()} to {end.isoformat()}.",
        ephemeral=True,
    )
    embed = discord.Embed(title="LOA Added Manually", colour=discord.Colour.orange())
    embed.add_field(name="Staff", value=member.mention, inline=True)
    embed.add_field(name="Dates", value=f"{start.isoformat()} → {end.isoformat()}", inline=True)
    embed.add_field(name="Reason", value=reason.strip()[:1024], inline=False)
    await send_loa_log(embed, view=ActiveLOAView(member.id))


@loa_group.command(name="end", description="End your active LOA early")
async def loa_end(interaction: discord.Interaction) -> None:
    if not interaction.guild or not is_allowed_guild(interaction.guild):
        await interaction.response.send_message("This bot is locked to a different server.", ephemeral=True)
        return
    row = end_active_loa_for_member(interaction.user.id)
    if not row:
        pending = get_pending_loa_request_for_member(interaction.user.id)
        if pending:
            cancel_pending_loa_request(int(pending['id']), interaction.user.id, "Cancelled by requester")
            await interaction.response.send_message(f"Cancelled pending LOA request #{pending['id']}.", ephemeral=True)
            await send_loa_log(build_loa_update_embed(f"LOA Request Cancelled • #{pending['id']}", pending, "Cancelled by requester"))
            return
        await interaction.response.send_message("You do not have an active LOA or pending request to end.", ephemeral=True)
        return
    await interaction.response.send_message("Your active LOA was ended early.", ephemeral=True)
    await send_loa_log(build_loa_update_embed("LOA Ended Early", row, f"Ended by <@{interaction.user.id}>"))


@loa_group.command(name="remove", description="Remove a staff member's active LOA early")
@management_check
@app_commands.describe(member="Staff member", note="Optional reason for ending the LOA early")
async def loa_remove(interaction: discord.Interaction, member: discord.Member, note: str | None = None) -> None:
    row = end_active_loa_for_member(member.id)
    if not row:
        await interaction.response.send_message("That staff member does not have an active LOA.", ephemeral=True)
        return
    await dm_loa_removed_notice(member, removed_by=interaction.user.id, note=note)
    await interaction.response.send_message(f"Ended {member.mention}'s active LOA early.", ephemeral=True)
    await send_loa_log(build_loa_update_embed("LOA Removed Early", row, note or f"Removed by <@{interaction.user.id}>"))


@loa_group.command(name="list", description="List LOA requests and active periods")
@management_check
@app_commands.describe(view="pending, active, or all")
@app_commands.choices(view=[
    app_commands.Choice(name="pending", value="pending"),
    app_commands.Choice(name="active", value="active"),
    app_commands.Choice(name="all", value="all"),
])
async def loa_list(interaction: discord.Interaction, view: app_commands.Choice[str]) -> None:
    lines: list[str] = []
    if view.value in {"pending", "all"}:
        pending = list_loa_requests("pending")
        if pending:
            lines.append("**Pending Requests**")
            for row in pending[:15]:
                lines.append(
                    f"#{row['id']} • <@{row['discord_id']}> • {row['start_date']} → {row['end_date']} • {row['reason']}"
                )
    if view.value in {"active", "all"}:
        active_rows = list_loa_periods(active_only=True)
        if active_rows:
            if lines:
                lines.append("")
            lines.append("**Active / Scheduled LOA**")
            for row in active_rows[:15]:
                lines.append(
                    f"#{row['id']} • <@{row['discord_id']}> • {row['start_date']} → {row['end_date']} • {row['reason']}"
                )
    if not lines:
        lines = ["No matching LOA items found."]
    await interaction.response.send_message("\n".join(lines)[:1900], ephemeral=True)


@staff_group.command(name="case_open", description="Open a staff case file for review or investigation")
@management_check
@app_commands.describe(
    member="Optional staff member involved",
    severity="Case severity",
    summary="Short case title",
    details="Opening details",
    evidence_url="Optional evidence link",
)
@app_commands.choices(
    severity=[
        app_commands.Choice(name="concern", value="concern"),
        app_commands.Choice(name="major", value="major"),
        app_commands.Choice(name="severe", value="severe"),
    ],
)
async def case_open_command(
    interaction: discord.Interaction,
    severity: app_commands.Choice[str],
    summary: str,
    details: str,
    member: Optional[discord.Member] = None,
    evidence_url: str = "",
) -> None:
    subject_id = member.id if member else None
    evidence = evidence_url.strip() or None
    case_id = create_case(subject_id, severity.value, summary.strip(), details.strip(), evidence, interaction.user.id)
    embed = build_case_embed(case_id)
    view = CaseLogView(case_id, discord_id=subject_id, evidence_url=evidence)
    if embed:
        await send_log(embed, view=view)
    await interaction.response.send_message(f"Opened case #{case_id}.", ephemeral=True)


@staff_group.command(name="case_update", description="Add an update to a staff case")
@management_check
@app_commands.describe(case_id="Case ID", update_text="Update text", status="Optional new status", evidence_url="Optional evidence link")
@app_commands.choices(
    status=[
        app_commands.Choice(name="Keep Current", value=""),
        app_commands.Choice(name="Open", value="open"),
        app_commands.Choice(name="Under Review", value="under_review"),
        app_commands.Choice(name="Resolved", value="resolved"),
        app_commands.Choice(name="Closed", value="closed"),
    ]
)
async def case_update_command(
    interaction: discord.Interaction,
    case_id: int,
    update_text: str,
    status: app_commands.Choice[str] | None = None,
    evidence_url: str = "",
) -> None:
    existing = get_case(case_id)
    if not existing:
        await interaction.response.send_message("That case does not exist.", ephemeral=True)
        return
    status_after = (status.value.strip() if status else "") or None
    evidence = evidence_url.strip() or None
    add_case_update(case_id, update_text.strip(), interaction.user.id, status_after=status_after, evidence_url=evidence)
    row = get_case(case_id)
    update_embed = discord.Embed(title=f"Staff Case Updated #{case_id}", colour=discord.Colour.blurple())
    update_embed.add_field(name="Updated By", value=f"<@{interaction.user.id}>", inline=True)
    update_embed.add_field(name="Status", value=str(row['status']).replace('_', ' ').title(), inline=True)
    update_embed.add_field(name="Update", value=update_text.strip()[:1024], inline=False)
    if row and row['subject_discord_id']:
        update_embed.add_field(name="Staff", value=f"<@{int(row['subject_discord_id'])}>", inline=False)
    if evidence:
        update_embed.add_field(name="Evidence", value=evidence, inline=False)
    await send_log(update_embed, view=CaseLogView(case_id, discord_id=int(row['subject_discord_id']) if row and row['subject_discord_id'] else None, evidence_url=evidence or (str(row['evidence_url']) if row and row['evidence_url'] else None)))
    await interaction.response.send_message(f"Updated case #{case_id}.", ephemeral=True)


@staff_group.command(name="case_view", description="View a staff case file")
@management_check
@app_commands.describe(case_id="Case ID")
async def case_view_command(interaction: discord.Interaction, case_id: int) -> None:
    embed = build_case_embed(case_id)
    if not embed:
        await interaction.response.send_message("That case does not exist.", ephemeral=True)
        return
    row = get_case(case_id)
    view = CaseLogView(
        case_id,
        discord_id=int(row['subject_discord_id']) if row and row['subject_discord_id'] else None,
        evidence_url=(str(row['evidence_url']) if row and row['evidence_url'] else None),
        show_open_case=False,
    )
    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


@staff_group.command(name="case_close", description="Close a staff case file")
@management_check
@app_commands.describe(case_id="Case ID", outcome="Final outcome or conclusion", evidence_url="Optional evidence link")
async def case_close_command(interaction: discord.Interaction, case_id: int, outcome: str, evidence_url: str = "") -> None:
    evidence = evidence_url.strip() or None
    row = close_case(case_id, outcome.strip(), interaction.user.id, evidence_url=evidence)
    if not row:
        await interaction.response.send_message("That case does not exist.", ephemeral=True)
        return
    embed = discord.Embed(title=f"Staff Case Closed #{case_id}", colour=discord.Colour.green())
    embed.add_field(name="Closed By", value=f"<@{interaction.user.id}>", inline=True)
    embed.add_field(name="Severity", value=str(row['severity']).title(), inline=True)
    if row['subject_discord_id']:
        embed.add_field(name="Staff", value=f"<@{int(row['subject_discord_id'])}>", inline=False)
    embed.add_field(name="Summary", value=str(row['summary'])[:1024], inline=False)
    embed.add_field(name="Outcome", value=outcome.strip()[:1024], inline=False)
    if evidence or row['evidence_url']:
        embed.add_field(name="Evidence", value=(evidence or str(row['evidence_url']))[:1024], inline=False)
    await send_log(embed, view=CaseLogView(case_id, discord_id=int(row['subject_discord_id']) if row['subject_discord_id'] else None, evidence_url=evidence or (str(row['evidence_url']) if row['evidence_url'] else None)))
    await interaction.response.send_message(f"Closed case #{case_id}.", ephemeral=True)


TREE.add_command(staff_group)
TREE.add_command(loa_group)


# =========================
# Background checks
# =========================



async def post_weekly_report_embeds(embeds: list[discord.Embed]) -> None:
    channel_id = CONFIG.get("weekly_report_channel_id", 0)
    if not channel_id or not embeds:
        return
    for embed in embeds:
        await send_embed_to_channel(channel_id, embed, mention_management=True, mention_regular_staff=True)


def build_weekly_report_embeds(wk: str, title: str) -> list[discord.Embed]:
    rows = fetch_all_week_rows(wk)
    if not rows:
        return []

    minimum_seconds = int(float(CONFIG["minimum_weekly_hours"]) * 3600)
    embeds: list[discord.Embed] = []
    chunk: list[str] = []
    page = 1

    def flush_chunk(lines: list[str], page_num: int) -> None:
        if not lines:
            return
        embed = discord.Embed(title=f"{title} • Page {page_num}", colour=discord.Colour.blurple())
        embed.description = "\n".join(lines)
        embed.set_footer(text=f"Week key: {wk} • Minimum: {human_hours(minimum_seconds)}")
        embeds.append(embed)

    for row in rows:
        discord_id = int(row["discord_id"])
        total_punishments = int(row["discord_warns"]) + int(row["discord_kicks"]) + int(row["discord_bans"]) + int(row["discord_mutes"]) + int(row["mc_warns"]) + int(row["mc_kicks"]) + int(row["mc_bans"]) + int(row["mc_mutes"])
        strikes = active_strike_count(discord_id)
        status = status_label_for(discord_id, int(row["minecraft_seconds"]), bool(row["strike_eligible"]), wk)
        line = (
            f"<@{discord_id}> • MC {human_hours(int(row['minecraft_seconds']))} • "
            f"Punishments {total_punishments} • Strikes {strikes} • **{status_emoji(status)} {status}**"
        )
        if len("\n".join(chunk + [line])) > 3500:
            flush_chunk(chunk, page)
            page += 1
            chunk = []
        chunk.append(line)

    flush_chunk(chunk, page)
    return embeds


async def evaluate_previous_week() -> None:
    prev_key = previous_week_key()
    minimum_seconds = int(float(CONFIG["minimum_weekly_hours"]) * 3600)
    rows = fetch_all_week_rows(prev_key)
    if not rows:
        return

    for row in rows:
        if row["weekly_checked"]:
            continue
        member = bot.get_user(int(row["discord_id"])) or await bot.fetch_user(int(row["discord_id"]))
        mc_seconds = int(row["minecraft_seconds"])
        strike_eligible = bool(row["strike_eligible"])
        excused = has_loa_overlap_week(int(row["discord_id"]), prev_key)
        if strike_eligible and mc_seconds < minimum_seconds and not excused:
            await apply_strike_and_notify(
                member,
                1,
                f"Weekly playtime requirement not met. Required: {CONFIG['minimum_weekly_hours']}h, Logged: {mc_seconds / 3600:.2f}h.",
                kind="weekly_playtime",
                issued_by=bot.user.id if bot.user else None,
                send_dm=True,
            )
        mark_week_checked(int(row["discord_id"]), prev_key)

    embeds = build_weekly_report_embeds(prev_key, "Weekly Staff Review")
    await post_weekly_report_embeds(embeds)


async def evaluate_inactivity() -> None:
    threshold_days = int(CONFIG["inactivity_days_for_strike"])
    threshold = utcnow() - timedelta(days=threshold_days)

    for row in get_registered_staff():
        if not row["strike_eligible"]:
            continue
        if get_active_loa_for_member(int(row["discord_id"])):
            continue

        last_login = str_to_dt(row["last_minecraft_login_at"])
        anchor = str_to_dt(row["inactivity_strike_anchor"])

        if last_login and last_login >= threshold:
            continue

        if anchor:
            continue

        member = bot.get_user(int(row["discord_id"])) or await bot.fetch_user(int(row["discord_id"]))
        await apply_strike_and_notify(
            member,
            1,
            f"No Minecraft login detected for {threshold_days} days in a row.",
            kind="inactivity",
            issued_by=bot.user.id if bot.user else None,
            send_dm=True,
        )
        set_inactivity_anchor(int(row["discord_id"]), utcnow())


@tasks.loop(minutes=30)

async def background_loops() -> None:
    expire_decayed_strikes()
    await process_pending_dm_events()
    await evaluate_inactivity()
    await maybe_send_daily_management_checks()


@background_loops.before_loop
async def before_background() -> None:
    await bot.wait_until_ready()


@tasks.loop(minutes=1)
async def weekly_reset_loop() -> None:
    now = utcnow()
    next_reset = next_reset_after(now)
    # fire only inside the first minute after reset
    if 0 <= (now - (next_reset - timedelta(days=7))).total_seconds() < 60:
        await evaluate_previous_week()


@weekly_reset_loop.before_loop
async def before_weekly_reset() -> None:
    await bot.wait_until_ready()


# =========================
# Minecraft bridge HTTP API
# =========================


async def verify_bridge(request: web.Request) -> Optional[web.Response]:
    if not BRIDGE_TOKEN:
        return web.json_response({"error": "Bridge token not configured on bot."}, status=500)
    token = request.headers.get("X-Bridge-Token", "")
    if token != BRIDGE_TOKEN:
        return web.json_response({"error": "Unauthorized"}, status=401)
    return None


async def bridge_login(request: web.Request) -> web.Response:
    auth = await verify_bridge(request)
    if auth:
        return auth
    data = await request.json()
    discord_id = data.get("discord_id")
    minecraft_name = data.get("minecraft_name")

    if not discord_id and minecraft_name:
        row = resolve_staff_by_mc_name(minecraft_name)
        if row:
            discord_id = int(row["discord_id"])

    if not discord_id:
        return web.json_response({"error": "discord_id or a mapped minecraft_name is required."}, status=400)

    staff_row = get_staff_member(int(discord_id))
    if not staff_row:
        return web.json_response({"error": "Staff member is not registered."}, status=404)
    if not is_staff_active_row(staff_row):
        return web.json_response({"error": "Staff member is offboarded/inactive."}, status=404)

    open_session(int(discord_id), "minecraft")
    return web.json_response({"ok": True})


async def bridge_logout(request: web.Request) -> web.Response:
    auth = await verify_bridge(request)
    if auth:
        return auth
    data = await request.json()
    discord_id = data.get("discord_id")
    minecraft_name = data.get("minecraft_name")

    if not discord_id and minecraft_name:
        row = resolve_staff_by_mc_name(minecraft_name)
        if row:
            discord_id = int(row["discord_id"])

    if not discord_id:
        return web.json_response({"error": "discord_id or a mapped minecraft_name is required."}, status=400)

    staff_row = get_staff_member(int(discord_id))
    if not staff_row:
        return web.json_response({"error": "Staff member is not registered."}, status=404)
    if not is_staff_active_row(staff_row):
        return web.json_response({"error": "Staff member is offboarded/inactive."}, status=404)

    seconds = close_session(int(discord_id), "minecraft")
    return web.json_response({"ok": True, "seconds_added": seconds})


async def bridge_punishment(request: web.Request) -> web.Response:
    auth = await verify_bridge(request)
    if auth:
        return auth
    data = await request.json()

    staff_discord_id = data.get("staff_discord_id")
    minecraft_name = data.get("minecraft_name")
    if not staff_discord_id and minecraft_name:
        row = resolve_staff_by_mc_name(minecraft_name)
        if row:
            staff_discord_id = int(row["discord_id"])

    action_type = str(data.get("action_type", "")).lower().strip()
    reason = data.get("reason")
    target_name = data.get("target_name")

    if action_type not in {"warn", "kick", "ban", "mute"}:
        return web.json_response({"error": "Invalid action_type."}, status=400)
    if not staff_discord_id:
        return web.json_response({"error": "staff_discord_id or mapped minecraft_name is required."}, status=400)
    staff_row = get_staff_member(int(staff_discord_id))
    if not staff_row:
        return web.json_response({"error": "Staff member is not registered."}, status=404)
    if not is_staff_active_row(staff_row):
        return web.json_response({"error": "Staff member is offboarded/inactive."}, status=404)

    increment_punishment(int(staff_discord_id), "minecraft", action_type, None, target_name, reason)
    return web.json_response({"ok": True})


async def start_http_server() -> web.AppRunner:
    app = web.Application()
    app.router.add_post("/minecraft/login", bridge_login)
    app.router.add_post("/minecraft/logout", bridge_logout)
    app.router.add_post("/minecraft/punishment", bridge_punishment)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=HTTP_HOST, port=HTTP_PORT)
    await site.start()
    log.info("Minecraft bridge listening on %s:%s", HTTP_HOST, HTTP_PORT)
    return runner


async def main() -> None:
    if not TOKEN:
        raise RuntimeError("DISCORD_BOT_TOKEN is not set.")
    init_db()
    runner = await start_http_server()
    try:
        await bot.start(TOKEN)
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    if not TOKEN:
        raise RuntimeError("DISCORD_BOT_TOKEN is missing.")
    if not BRIDGE_TOKEN:
        raise RuntimeError("MINECRAFT_BRIDGE_TOKEN is missing.")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
