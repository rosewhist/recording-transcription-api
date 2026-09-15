"""``RecordingRepository`` 的真实读写，以及上传幂等所依赖的同哈希竞态。

``RecordingService.upload`` 靠两条保证才敢删自己的文件：唯一约束会让竞态中的一方
失败，且落败方能回读到已有记录。这两条以前完全没有测试。
"""
from __future__ import annotations

import asyncio
from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy.exc import IntegrityError

from _pg import make_recording, make_task, utcnow

from api.repository.dao.recording import Recording, RecordingDao
from api.repository.dao.task import TaskDao
from api.repository.dao.task_types import TaskStatus
from api.repository.recording import RecordingRepository


@pytest.fixture
def repository(session_factory, monkeypatch) -> RecordingRepository:
    """把仓储的模块级会话工厂指向测试库。"""
    monkeypatch.setattr("api.repository.recording.AsyncSessionFactory", session_factory)
    return RecordingRepository()


async def test_create_recording_and_task_persists_both(repository, session_factory):
    recording, task, created = await repository.create_recording_and_task(
        file_name="demo.wav", file_path="/tmp/demo.wav", file_size=12, file_hash="a" * 64
    )

    assert created is True
    assert task is not None
    assert task.status == TaskStatus.PENDING.value

    async with session_factory() as s:
        stored = await s.get(Recording, recording.id)
        assert stored is not None
        assert stored.file_name == "demo.wav"
        assert stored.file_size == 12
        assert (await TaskDao(s).get_by_recording(recording.id)) is not None


async def test_same_hash_second_upload_reuses_existing(repository, session_factory):
    """顺序重复上传：直接复用已有记录与任务，不新增行。"""
    first = await repository.create_recording_and_task(
        file_name="first.wav", file_path="/tmp/first.wav", file_size=1, file_hash="b" * 64
    )
    second = await repository.create_recording_and_task(
        file_name="second.wav", file_path="/tmp/second.wav", file_size=1, file_hash="b" * 64
    )

    assert first[2] is True
    assert second[2] is False
    assert second[0].id == first[0].id
    assert second[1].id == first[1].id

    async with session_factory() as s:
        assert await RecordingDao(s).count_all() == 1


async def test_concurrent_same_hash_creates_exactly_one(repository, session_factory):
    """并发同哈希：无论走查重还是唯一约束回退，恰好一方 created=True。"""
    results = await asyncio.gather(
        *(
            repository.create_recording_and_task(
                file_name=f"{i}.wav",
                file_path=f"/tmp/{i}.wav",
                file_size=1,
                file_hash="c" * 64,
            )
            for i in range(4)
        )
    )

    assert sum(1 for _, _, created in results if created) == 1
    async with session_factory() as s:
        assert await RecordingDao(s).count_all() == 1


async def test_duplicate_hash_hits_unique_constraint(session_factory):
    """幂等的地基：数据库层确实拦得住重复 file_hash。"""
    async with session_factory() as a, session_factory() as b:
        await make_recording(a, file_hash="d" * 64)
        await a.commit()

        with pytest.raises(IntegrityError):
            await make_recording(b, file_hash="d" * 64)


async def test_integrity_error_falls_back_to_existing_row(
    repository, session_factory, monkeypatch
):
    """竞态落败时必须回读已有记录并返回 created=False，而不是把异常抛给调用方。

    用「首次查重故意返回 None」精确重现两个请求同时通过查重窗口的那一瞬间。
    """
    existing_hash = "e" * 64
    async with session_factory() as s:
        existing = await make_recording(s, file_name="first.wav", file_hash=existing_hash)
        await make_task(s, existing)
        await s.commit()
        existing_id = existing.id

    real_get_by_hash = RecordingDao.get_by_hash
    calls = {"n": 0}

    async def flaky_get_by_hash(self, file_hash):
        calls["n"] += 1
        if calls["n"] == 1:
            return None  # 模拟「查重时还没提交」的窗口
        return await real_get_by_hash(self, file_hash)

    monkeypatch.setattr(RecordingDao, "get_by_hash", flaky_get_by_hash)

    recording, task, created = await repository.create_recording_and_task(
        file_name="second.wav",
        file_path="/tmp/second.wav",
        file_size=2,
        file_hash=existing_hash,
    )

    assert calls["n"] >= 2  # 走了回读分支
    assert created is False
    assert recording.id == existing_id
    assert task is not None
    assert task.recording_id == existing_id


async def test_list_page_orders_desc_with_tasks_and_total(repository, session_factory):
    async with session_factory() as s:
        now = utcnow()
        ids = []
        for i in range(3):
            rec = await make_recording(
                s, file_name=f"{i}.wav", created_at=now - timedelta(minutes=5 - i)
            )
            await make_task(s, rec)
            ids.append(rec.id)
        await s.commit()

    items, total = await repository.list_page(page=1, page_size=2)

    assert total == 3
    assert [r.id for r, _ in items] == [ids[2], ids[1]]  # 最新在前
    assert all(task is not None for _, task in items)

    second_page, _ = await repository.list_page(page=2, page_size=2)
    assert [r.id for r, _ in second_page] == [ids[0]]

    empty_page, _ = await repository.list_page(page=9, page_size=2)
    assert empty_page == []


async def test_delete_removes_task_then_recording(repository, session_factory):
    async with session_factory() as s:
        rec = await make_recording(s)
        await make_task(s, rec)
        await s.commit()
        rec_id, file_path = rec.id, rec.file_path

    assert await repository.delete(rec_id) == file_path
    assert await repository.delete(rec_id) is None  # 幂等：再删一次不再报错

    async with session_factory() as s:
        assert await s.get(Recording, rec_id) is None
        assert await TaskDao(s).get_by_recording(rec_id) is None


async def test_get_by_id_and_hash_for_unknown_values(repository):
    assert await repository.get_by_id(uuid4()) == (None, None)
    assert await repository.get_by_hash("f" * 64) == (None, None)
