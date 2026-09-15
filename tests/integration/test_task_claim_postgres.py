"""``claim_pending`` 的真实 SQL 语义。

这些用例取代了原先「编译成 SQL 字符串再断言包含 SKIP LOCKED / OR」的做法：断言的是
真实行状态与真实并发行为，SQL 写错（列、条件、区间单位）就会失败。
"""
from __future__ import annotations

from datetime import timedelta

import pytest

from _pg import fetch_task, make_recording, make_task, utcnow

from api.repository.dao.task import TaskDao
from api.repository.dao.task_types import TaskStatus


async def test_claim_picks_only_due_pending(session_factory):
    """只抢占 ``pending`` 且 ``next_retry_at`` 已到的任务。"""
    async with session_factory() as s:
        due = await make_task(s, await make_recording(s), status=TaskStatus.PENDING.value)
        future = await make_task(
            s,
            await make_recording(s),
            status=TaskStatus.PENDING.value,
            next_retry_at=utcnow() + timedelta(hours=1),
        )
        inflight = await make_task(
            s,
            await make_recording(s),
            status=TaskStatus.TRANSCRIBING.value,
            locked_by="worker-other",
            lease_seconds=3600,
        )
        await s.commit()
        due_id, future_id, inflight_id = due.id, future.id, inflight.id

    async with session_factory() as s:
        claimed = await TaskDao(s).claim_pending(
            limit=10, worker_id="worker-1", lease_seconds=300
        )
        await s.commit()

    assert claimed == [due_id]

    async with session_factory() as s:
        claimed_row = await fetch_task(s, due_id)
        assert claimed_row.status == TaskStatus.TRANSCRIBING.value
        assert claimed_row.locked_by == "worker-1"
        # 租约必须落在未来，否则任务会被立刻判定过期并回收
        assert claimed_row.lease_expires_at > utcnow() + timedelta(seconds=200)

        assert (await fetch_task(s, future_id)).status == TaskStatus.PENDING.value
        untouched = await fetch_task(s, inflight_id)
        assert untouched.locked_by == "worker-other"
        assert untouched.status == TaskStatus.TRANSCRIBING.value


async def test_claim_respects_limit_and_oldest_first(session_factory):
    """按 ``created_at`` 升序（FIFO）抢占，且不超过 limit。"""
    async with session_factory() as s:
        now = utcnow()
        ids = []
        for i in range(5):
            task = await make_task(
                s,
                await make_recording(s),
                created_at=now - timedelta(seconds=(5 - i) * 10),
            )
            ids.append(task.id)
        await s.commit()

    async with session_factory() as s:
        claimed = await TaskDao(s).claim_pending(
            limit=2, worker_id="worker-1", lease_seconds=300
        )
        await s.commit()

    assert claimed == ids[:2]


async def test_claim_zero_limit_is_noop(session_factory):
    async with session_factory() as s:
        task = await make_task(s, await make_recording(s))
        await s.commit()
        task_id = task.id

    async with session_factory() as s:
        assert (
            await TaskDao(s).claim_pending(limit=0, worker_id="worker-1", lease_seconds=300)
            == []
        )
        await s.commit()

    async with session_factory() as s:
        assert (await fetch_task(s, task_id)).status == TaskStatus.PENDING.value


async def test_concurrent_claim_never_double_claims(session_factory):
    """两个 worker 并发抢占：``FOR UPDATE SKIP LOCKED`` 保证互不重叠。

    这是整个 worker 正确性的基石——以前只断言 SQL 文本里出现 "SKIP LOCKED"。
    """
    async with session_factory() as s:
        for _ in range(4):
            await make_task(s, await make_recording(s))
        await s.commit()

    async with session_factory() as a, session_factory() as b:
        # A 先抢占 2 条并持有行锁（事务未提交）
        claimed_a = await TaskDao(a).claim_pending(
            limit=2, worker_id="worker-a", lease_seconds=300
        )
        # B 紧随其后：A 锁住的行必须被跳过，而不是阻塞等待
        claimed_b = await TaskDao(b).claim_pending(
            limit=2, worker_id="worker-b", lease_seconds=300
        )
        await b.commit()
        await a.commit()

    assert len(claimed_a) == 2
    assert len(claimed_b) == 2
    assert set(claimed_a).isdisjoint(claimed_b)
    assert len(set(claimed_a) | set(claimed_b)) == 4


async def test_concurrent_claim_skips_locked_rows_when_quota_is_full(session_factory):
    """A 一次拿走全部时，B 拿不到任何任务（也不会阻塞）。"""
    async with session_factory() as s:
        for _ in range(3):
            await make_task(s, await make_recording(s))
        await s.commit()

    async with session_factory() as a, session_factory() as b:
        claimed_a = await TaskDao(a).claim_pending(
            limit=10, worker_id="worker-a", lease_seconds=300
        )
        claimed_b = await TaskDao(b).claim_pending(
            limit=10, worker_id="worker-b", lease_seconds=300
        )
        await b.commit()
        await a.commit()

    assert len(claimed_a) == 3
    assert claimed_b == []


async def test_reclaimed_task_becomes_claimable_again(session_factory):
    """租约过期回收后任务重新可被抢占（端到端的「卡住 → 恢复」闭环）。"""
    async with session_factory() as s:
        task = await make_task(
            s,
            await make_recording(s),
            status=TaskStatus.TRANSCRIBING.value,
            locked_by="worker-dead",
            lease_seconds=-60,  # 已过期
        )
        await s.commit()
        task_id = task.id

    async with session_factory() as s:
        n = await TaskDao(s).reclaim_in_flight(
            worker_id="worker-new", include_own_locks=False, error_msg="expired"
        )
        await s.commit()
    assert n == 1

    async with session_factory() as s:
        claimed = await TaskDao(s).claim_pending(
            limit=1, worker_id="worker-new", lease_seconds=300
        )
        await s.commit()
    assert claimed == [task_id]

    async with session_factory() as s:
        row = await fetch_task(s, task_id)
        assert row.locked_by == "worker-new"
        assert row.status == TaskStatus.TRANSCRIBING.value
