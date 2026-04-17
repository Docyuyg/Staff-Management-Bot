# Staff Monitor Bot

This bot is built for a Discord staff team that also moderates a Minecraft server.

## What it does
- Tracks weekly staff playtime
- Resets the tracked week every Sunday at 6:00 PM America/Chicago
- Automatically gives a strike if a strike-eligible staff member logs under 3 hours for the week
- Automatically gives a strike if a strike-eligible staff member has no Minecraft login for 3 days in a row
- DMs staff automatically when they receive a strike, including the total active strikes and a warning message
- Lets upper management manually issue strikes and still have the bot DM the staff member
- Tracks warns, kicks, bans, and mutes for Discord and Minecraft
- Posts a weekly report to a configured channel

## Important limits
- Discord has no native "warn" action, so Discord warns are only truly trackable if staff issue warns through this bot's `/mod warn` command.
- Discord kicks, bans, and timeouts can be tracked either through this bot's moderation commands or from audit-log events if the bot has the right permissions.
- Minecraft playtime and Minecraft punishments are automatic only if your Minecraft server sends data into the bot through the included HTTP bridge.

## Install
1. Create a bot in the Discord developer portal.
2. Give it these permissions in your server:
   - View Audit Log
   - Moderate Members
   - Kick Members
   - Ban Members
   - Send Messages
   - Read Messages/View Channels
   - Embed Links
3. Enable the Server Members Intent in the Discord developer portal.
4. Install Python 3.11+.
5. Install packages:
   ```bash
   pip install -r requirements.txt
   ```
6. Copy `config.example.json` to `config.json` and fill in your IDs.
7. Set environment variables:
   ```bash
   export DISCORD_BOT_TOKEN="your_bot_token"
   export MINECRAFT_BRIDGE_TOKEN="super_secret_token"
   ```
8. Run:
   ```bash
   python staff_monitor_bot.py
   ```

## Register staff
Use:
- `/staff register member:@Name minecraft_name:TheirIGN strike_eligible:true`

Set `strike_eligible` to `true` for helper, jr mod, and mod.
Set it to `false` for upper staff.

## Commands
### Staff
- `/staff register`
- `/staff stats`
- `/staff strike_add`
- `/staff strike_remove`

### Discord moderation tracking
- `/mod warn`
- `/mod kick`
- `/mod mute`
- `/mod ban`

### Manual Minecraft helpers
- `/admin log_mc_punishment`
- `/admin mc_login`
- `/admin mc_logout`
- `/admin add_playtime`

## HTTP bridge endpoints
Add your own server-side sender so your Minecraft server can call these:

### Login
`POST /minecraft/login`
```json
{
  "discord_id": 123456789012345678,
  "minecraft_name": "StaffIGN"
}
```

### Logout
`POST /minecraft/logout`
```json
{
  "discord_id": 123456789012345678,
  "minecraft_name": "StaffIGN"
}
```

### Punishment
`POST /minecraft/punishment`
```json
{
  "staff_discord_id": 123456789012345678,
  "minecraft_name": "StaffIGN",
  "action_type": "ban",
  "target_name": "RuleBreaker123",
  "reason": "Cheating"
}
```

Include this request header on every bridge request:
```text
X-Bridge-Token: your_secret_token
```

## Recommended Minecraft setup
If you already use LiteBans, the cleanest setup is either:
- a tiny bridge plugin that sends login/logout/punishment events to this bot, or
- a second small service that listens to your punishment source and forwards them here.

## Notes
- Weekly stats are stored per week key instead of being wiped, so you keep history.
- Weekly reset evaluation happens when the new Sunday 6 PM week begins.
- Inactivity strikes only fire once per streak until the staff member logs in again.
