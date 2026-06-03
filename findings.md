# Findings

## Source Bot Shape

- Existing source is on devNuc at `/home/dreaife/dev/infra/group-backup-bot`.
- Main entrypoint: `telebot/group_backup_bot.py`.
- Core files:
  - `telebot/group_backup/core.py`
  - `telebot/group_backup/handlers.py`
  - `telebot/group_backup/mapper.py`
  - `telebot/group_backup/summarizer.py`
- Current implementation forwards source messages to backup Telegram groups, tracks mapping in JSON, and periodically exports `.bak` NDJSON files.

## New Product Boundary

- New core should not forward to backup Telegram groups.
- New core should not generate summaries on server.
- New core should focus on structured archival and sync.

## Migration Notes

- Do not copy `.env`, `data/`, `logs/`, `venv/`, `.session`, or real `group_backup_config.yml`.
- Avoid server-specific defaults like `/data`, `/logs`, `/opt/data`, and `.hermes-docker`.
- DevNuc deployment can pass explicit config/data/log locations through systemd.

