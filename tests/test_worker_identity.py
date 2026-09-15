from __future__ import annotations

from pathlib import Path

from api.config.settings import Settings
from api.worker.identity import resolve_worker_id


def _settings(tmp_path: Path, **overrides) -> Settings:
    params = {"WORKER_ID": None, "WORKER_STATE_DIR": str(tmp_path)}
    params.update(overrides)
    return Settings(**params)


def test_explicit_worker_id_wins_and_is_not_persisted(tmp_path: Path):
    settings = _settings(tmp_path, WORKER_ID="pinned-worker")

    assert resolve_worker_id(settings) == "pinned-worker"
    assert not (tmp_path / "worker_id").exists()


def test_default_id_is_persisted_and_stable_across_restarts(tmp_path: Path):
    first = resolve_worker_id(_settings(tmp_path))

    assert first.startswith("worker-")
    assert len(first) <= 64
    assert (tmp_path / "worker_id").read_text(encoding="utf-8") == first

    # 模拟进程重启：同一状态目录重新解析 → 标识不变
    assert resolve_worker_id(_settings(tmp_path)) == first


def test_persisted_id_survives_hostname_change(tmp_path: Path, monkeypatch):
    (tmp_path / "worker_id").write_text("worker-oldhost-deadbeef", encoding="utf-8")
    monkeypatch.setattr("api.worker.identity._hostname", lambda: "brand-new-host")

    assert resolve_worker_id(_settings(tmp_path)) == "worker-oldhost-deadbeef"


def test_generated_id_is_length_capped(tmp_path: Path, monkeypatch):
    monkeypatch.setattr("api.worker.identity._hostname", lambda: "h" * 200)

    worker_id = resolve_worker_id(_settings(tmp_path))

    assert len(worker_id) == 64


def test_falls_back_when_state_dir_unwritable(tmp_path: Path):
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory", encoding="utf-8")

    worker_id = resolve_worker_id(_settings(tmp_path, WORKER_STATE_DIR=str(blocker)))

    assert worker_id.startswith("worker-")
    assert len(worker_id) <= 64
