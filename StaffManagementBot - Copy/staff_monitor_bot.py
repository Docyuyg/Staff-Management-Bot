from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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
    "management_role_ids": [],
    "auto_strike_role_ids": [],  # helper / jr mod / mod
    "upper_staff_role_ids": [],  # tracked but exempt from automatic strikes
    "minimum_weekly_hours": 3.0,
    "inactivity_days_for_strike": 3,
    "warning_text": "If you receive further strikes, you may risk getting fired.",
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


async def send_log(embed: discord.Embed) -> None:
    channel_id = CONFIG.get("staff_log_channel_id", 0)
    if not channel_id:
        return
    channel = bot.get_channel(channel_id)
    if isinstance(channel, discord.abc.Messageable):
        await channel.send(embed=embed)


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
    await send_log(embed)
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
        await send_log(embed)
    except Exception:
        log.exception("Error processing audit log entry")


# =========================
# Slash commands
# =========================


staff_group = app_commands.Group(name="staff", description="Staff management commands")
mod_group = app_commands.Group(name="mod", description="Moderation commands tracked by the bot")
admin_group = app_commands.Group(name="admin", description="Administrative tracking helpers")


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


@staff_group.command(name="stats", description="Show this week's staff stats")
@management_check
@app_commands.describe(member="Staff member to inspect")
async def staff_stats(interaction: discord.Interaction, member: discord.Member) -> None:
    staff = get_staff_member(member.id)
    if not staff:
        await interaction.response.send_message("That user is not registered in the tracker.", ephemeral=True)
        return
    stats = fetch_weekly_stats(member.id)
    strikes = active_strike_count(member.id)
    embed = discord.Embed(title=f"Weekly stats for {member}", colour=discord.Colour.blurple())
    embed.add_field(name="Minecraft Playtime", value=human_hours(stats["minecraft_seconds"]), inline=True)
    embed.add_field(name="Discord Activity", value=human_hours(stats["discord_seconds"]), inline=True)
    embed.add_field(name="Active Strikes", value=str(strikes), inline=True)
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
    await interaction.response.send_message(embed=embed, ephemeral=True)


@staff_group.command(name="strike_add", description="Manually issue a strike and DM the staff member")
@management_check
@app_commands.describe(member="Staff member", amount="Strike amount", reason="Reason for the strike", send_dm="Whether to DM the user")
async def strike_add(interaction: discord.Interaction, member: discord.Member, amount: app_commands.Range[int, 1, 10], reason: str, send_dm: bool = True) -> None:
    total = await apply_strike_and_notify(member, amount, reason, kind="manual", issued_by=interaction.user.id, send_dm=send_dm)
    await interaction.response.send_message(
        f"Issued {amount} strike(s) to {member.mention}. Active strikes: {total}.",
        ephemeral=True,
    )


@staff_group.command(name="strike_remove", description="Remove active strikes from a staff member")
@management_check
@app_commands.describe(member="Staff member", amount="How many active strikes to remove")
async def strike_remove(interaction: discord.Interaction, member: discord.Member, amount: app_commands.Range[int, 1, 10]) -> None:
    total = remove_strikes(member.id, amount)
    embed = discord.Embed(title="Strikes Removed", colour=discord.Colour.green())
    embed.add_field(name="Staff", value=f"{member} ({member.id})", inline=False)
    embed.add_field(name="Removed", value=str(amount))
    embed.add_field(name="Active Strikes", value=str(total))
    await send_log(embed)
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


TREE.add_command(staff_group)
TREE.add_command(mod_group)
TREE.add_command(admin_group)


# =========================
# Background checks
# =========================


async def send_weekly_report(lines: list[str]) -> None:
    channel_id = CONFIG.get("weekly_report_channel_id", 0)
    if not channel_id:
        return
    channel = bot.get_channel(channel_id)
    if isinstance(channel, discord.abc.Messageable):
        message = "\n".join(lines)
        await channel.send(message[:1900])


async def evaluate_previous_week() -> None:
    prev_key = previous_week_key()
    minimum_seconds = int(float(CONFIG["minimum_weekly_hours"]) * 3600)
    rows = fetch_all_previous_week_candidates(prev_key)
    if not rows:
        return

    report_lines = [f"**Weekly staff reset finished for `{prev_key}`**"]
    for row in rows:
        if row["weekly_checked"]:
            continue
        member = bot.get_user(int(row["discord_id"])) or await bot.fetch_user(int(row["discord_id"]))
        mc_seconds = int(row["minecraft_seconds"])
        strike_eligible = bool(row["strike_eligible"])
        got_auto_strike = False
        if strike_eligible and mc_seconds < minimum_seconds:
            total = await apply_strike_and_notify(
                member,
                1,
                f"Weekly playtime requirement not met. Required: {CONFIG['minimum_weekly_hours']}h, Logged: {mc_seconds / 3600:.2f}h.",
                kind="weekly_playtime",
                issued_by=bot.user.id if bot.user else None,
                send_dm=True,
            )
            got_auto_strike = True
        mark_week_checked(int(row["discord_id"]), prev_key)
        report_lines.append(
            f"- <@{row['discord_id']}> | MC {human_hours(mc_seconds)} | Discord {human_hours(int(row['discord_seconds']))} | "
            f"DC warns/kicks/bans/mutes {row['discord_warns']}/{row['discord_kicks']}/{row['discord_bans']}/{row['discord_mutes']} | "
            f"MC warns/kicks/bans/mutes {row['mc_warns']}/{row['mc_kicks']}/{row['mc_bans']}/{row['mc_mutes']} | "
            f"Auto strike: {'YES' if got_auto_strike else 'NO'}"
        )
    await send_weekly_report(report_lines)


async def evaluate_inactivity() -> None:
    wk = week_key_for()
    threshold_days = int(CONFIG["inactivity_days_for_strike"])
    threshold = utcnow() - timedelta(days=threshold_days)

    for row in get_registered_staff():
        if not row["strike_eligible"]:
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
