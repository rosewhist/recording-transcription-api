"""程序内执行 Alembic 迁移。"""
from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config

_ROOT = Path(__file__).resolve().parents[3]


def run_migrations(revision: str = "head") -> None:
    """将数据库升级到指定 revision（默认 head）。"""
    cfg = Config(str(_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(_ROOT / "alembic"))
    command.upgrade(cfg, revision)
