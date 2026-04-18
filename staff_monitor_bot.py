from __future__ import annotations

import asyncio
import json
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
    "management_ping_target_ids": [
        1016147595128754207,
        1016146618170495077,
        101614586811482973,
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


def load_config() -> dict[str, Any]:
    if not CONFIG_PATH.exists():
        CONFIG_PATH.write_text(json.dumps(DEFAULT_CONFIG, indent=2), encoding="utf-8")
        return DEFAULT_CONFIG.copy()
    data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    merged = DEFAULT_CONFIG.copy()
    merged.update(data)
    return merged


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
    FOREIGN KEY(discord_id) REFERENCES staff_members(discord_id) ON DELETE CASCADE
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
"""


def init_db() -> None:
    with get_db() as conn:
        conn.executescript(SCHEMA)


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


# =========================
# Data access
# =========================


def upsert_staff_member(discord_id: int, minecraft_name: str | None, strike_eligible: bool, notes: str | None = None) -> None:
    now = dt_to_str(utcnow())
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO staff_members (discord_id, minecraft_name, strike_eligible, notes, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(discord_id) DO UPDATE SET
                minecraft_name = excluded.minecraft_name,
                strike_eligible = excluded.strike_eligible,
                notes = COALESCE(excluded.notes, staff_members.notes),
                updated_at = excluded.updated_at
            """,
            (discord_id, minecraft_name, 1 if strike_eligible else 0, notes, now, now),
        )


def get_staff_member(discord_id: int) -> Optional[sqlite3.Row]:
    with get_db() as conn:
        return conn.execute("SELECT * FROM staff_members WHERE discord_id = ?", (discord_id,)).fetchone()


def resolve_staff_by_mc_name(minecraft_name: str) -> Optional[sqlite3.Row]:
    with get_db() as conn:
        return conn.execute(
            "SELECT * FROM staff_members WHERE lower(minecraft_name) = lower(?)",
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
    with get_db() as conn:
        row = conn.execute(
            "SELECT COALESCE(SUM(amount), 0) AS total FROM strikes WHERE discord_id = ? AND active = 1",
            (discord_id,),
        ).fetchone()
        return int(row["total"] if row else 0)


def add_strike(discord_id: int, amount: int, reason: str, kind: str, issued_by: int | None = None) -> int:
    now = dt_to_str(utcnow())
    with get_db() as conn:
        conn.execute(
            "INSERT INTO strikes (discord_id, amount, reason, kind, issued_by, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (discord_id, amount, reason, kind, issued_by, now),
        )
    return active_strike_count(discord_id)


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



def set_staff_notes(discord_id: int, notes: str | None) -> None:
    now = dt_to_str(utcnow())
    with get_db() as conn:
        conn.execute(
            "UPDATE staff_members SET notes = ?, updated_at = ? WHERE discord_id = ?",
            (notes, now, discord_id),
        )


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
            WHERE ws.week_key = ?
            ORDER BY ws.discord_id
            """,
            (wk,),
        ).fetchall()


def expire_decayed_strikes() -> int:
    now = utcnow()
    changed = 0
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, kind, reason, created_at FROM strikes WHERE active = 1"
        ).fetchall()
        for row in rows:
            created = str_to_dt(row["created_at"])
            if not created:
                continue
            days = strike_decay_days_for(str(row["kind"]), str(row["reason"]))
            if days <= 0:
                continue
            if created + timedelta(days=days) <= now:
                conn.execute("UPDATE strikes SET active = 0 WHERE id = ?", (row["id"],))
                changed += 1
    return changed


def status_label_for(discord_id: int, mc_seconds: int, strike_eligible: bool, wk: str) -> str:
    if has_loa_overlap_week(discord_id, wk):
        return "Excused"
    strikes = active_strike_count(discord_id)
    minimum_seconds = int(float(CONFIG["minimum_weekly_hours"]) * 3600)
    if strikes >= 3:
        return "Needs Review"
    if strikes >= 1:
        return "Watchlist"
    if strike_eligible and mc_seconds < minimum_seconds:
        return "Needs Review"
    if mc_seconds >= minimum_seconds:
        return "Good Standing"
    return "Tracked"


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


def build_ping_text_from_ids(ids: list[int] | list[str], channel: discord.abc.GuildChannel | None) -> str | None:
    guild = channel.guild if isinstance(channel, discord.abc.GuildChannel) else None
    parts: list[str] = []
    for raw_id in ids:
        try:
            target_id = int(raw_id)
        except Exception:
            continue
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


async def dm_strike_notice(member: discord.abc.User, total_strikes: int, reason: str) -> None:
    msg = (
        f"You received a staff strike.\n"
        f"Reason: {reason}\n"
        f"Active strikes: {total_strikes}\n\n"
        f"{CONFIG['warning_text']}"
    )
    try:
        await member.send(msg)
    except discord.Forbidden:
        log.warning("Could not DM strike notice to %s", member.id)


async def apply_strike_and_notify(member: discord.Member | discord.User, amount: int, reason: str, kind: str, issued_by: int | None = None, send_dm: bool = True) -> int:
    total = add_strike(member.id, amount, reason, kind, issued_by)
    if send_dm:
        await dm_strike_notice(member, total, reason)

    embed = discord.Embed(title="Strike Issued", colour=discord.Colour.red())
    embed.add_field(name="Staff", value=f"{member} ({member.id})", inline=False)
    embed.add_field(name="Amount", value=str(amount))
    embed.add_field(name="Reason", value=reason, inline=False)
    embed.add_field(name="Type", value=kind)
    embed.add_field(name="Active Strikes", value=str(total))
    await send_log(embed, view=StaffStatsButtonView(member.id, allow_remove_strike=True))
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


@bot.event
async def on_ready() -> None:
    log.info("Logged in as %s (%s)", bot.user, bot.user.id if bot.user else "?")

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


@staff_group.command(name="register", description="Register or update a staff member in the tracker")
@management_check
@app_commands.describe(member="Discord staff member", minecraft_name="Minecraft username", strike_eligible="Auto strike eligible (helper/jr mod/mod)")
async def register_staff(interaction: discord.Interaction, member: discord.Member, minecraft_name: str, strike_eligible: bool) -> None:
    upsert_staff_member(member.id, minecraft_name, strike_eligible)
    ensure_week_row(member.id)
    await interaction.response.send_message(
        f"Registered {member.mention} with Minecraft name `{minecraft_name}`. Strike eligible: `{strike_eligible}`.",
        ephemeral=True,
    )


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
    loa_row = get_active_loa_for_member(member.id)
    embed = discord.Embed(title=f"Weekly stats for {member}", colour=discord.Colour.blurple())
    embed.add_field(name="Minecraft Playtime", value=human_hours(stats["minecraft_seconds"]), inline=True)
    embed.add_field(name="Discord Activity", value=human_hours(stats["discord_seconds"]), inline=True)
    embed.add_field(name="Active Strikes", value=str(strikes), inline=True)
    embed.add_field(name="Status", value=status, inline=True)
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
    def __init__(self, discord_id: int, *, allow_remove_strike: bool = False):
        super().__init__(timeout=None)
        self.discord_id = discord_id
        self.remove_one.disabled = not allow_remove_strike

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
        embed = discord.Embed(title="Strike Removed", colour=discord.Colour.green())
        embed.add_field(name="Staff", value=display, inline=False)
        embed.add_field(name="Removed", value="1")
        embed.add_field(name="Active Strikes", value=str(total))
        embed.add_field(name="Removed By", value=f"<@{interaction.user.id}>", inline=False)
        await send_log(embed, view=StaffStatsButtonView(self.discord_id, allow_remove_strike=False))
        await interaction.response.send_message(f"Removed 1 strike from {display}. Active strikes: {total}.", ephemeral=True)

@staff_group.command(name="stats", description="Show this week's staff stats")
@management_check
@app_commands.describe(member="Staff member to inspect")
async def staff_stats(interaction: discord.Interaction, member: discord.Member) -> None:
    embed = build_staff_stats_embed(member)
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
    send_dm: bool = True,
) -> None:
    amount = int(amount_override if amount_override > 0 else SEVERITY_AMOUNTS[severity.value])
    reason = format_manual_strike_reason(severity.value, reason_code.value, details)
    total = await apply_strike_and_notify(member, amount, reason, kind="manual", issued_by=interaction.user.id, send_dm=send_dm)
    await interaction.response.send_message(
        f"Issued {amount} strike(s) to {member.mention}. Reason: {reason}. Active strikes: {total}.",
        ephemeral=True,
    )




@staff_group.command(name="note_set", description="Set or replace private management notes for a staff member")
@management_check
@app_commands.describe(member="Staff member", notes="Private note text")
async def note_set(interaction: discord.Interaction, member: discord.Member, notes: str) -> None:
    if not get_staff_member(member.id):
        await interaction.response.send_message("That user is not registered in the tracker.", ephemeral=True)
        return
    set_staff_notes(member.id, notes.strip())
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
    set_staff_notes(member.id, None)
    await interaction.response.send_message(f"Cleared notes for {member.mention}.", ephemeral=True)


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


TREE.add_command(staff_group)
TREE.add_command(mod_group)
TREE.add_command(admin_group)
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
            f"Punishments {total_punishments} • Strikes {strikes} • **{status}**"
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
    await evaluate_inactivity()


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

    if not get_staff_member(int(discord_id)):
        return web.json_response({"error": "Staff member is not registered."}, status=404)

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
    if not get_staff_member(int(staff_discord_id)):
        return web.json_response({"error": "Staff member is not registered."}, status=404)

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
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
