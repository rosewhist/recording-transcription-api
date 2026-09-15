"""状态流转 / 租约 / 重试退避的真实 SQL 语义（接真实 Postgres）。

重点覆盖此前只被字符串匹配「验证」过的部分：fencing 守卫（``WHERE locked_by = ...``）、
``mark_failure`` 的指数退避数值、以及 ``reclaim_in_flight`` 的 owner 过滤条件。
"""
from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import text

from _pg import fetch_task, make_recording, make_task, utcnow

from api.repository.dao.task import TaskDao
from api.repository.dao.task_types import TaskStatus


async def _one_inflight(session_factory, *, worker_id: str = "worker-1", lease_seconds: float = 300):
    async with session_factory() as s:
        task = await make_task(
            s,
            await make_recording(s),
            status=TaskStatus.TRANSCRIBING.value,
            locked_by=worker_id,
            lease_seconds=lease_seconds,
        )
        await s.commit()
        return task.id


async def test_state_machine_happy_path_persists_each_step(session_factory):
    """claim → transcribing → summarizing → done 的每一步都真正落库。"""
    async with session_factory() as s:
        task = await make_task(s, await make_recording(s))
        await s.commit()
        task_id = task.id

    async with session_factory() as s:
        assert await TaskDao(s).claim_pending(
            limit=1, worker_id="worker-1", lease_seconds=300
        ) == [task_id]
        await s.commit()

    async with session_factory() as s:
        assert await TaskDao(s).mark_summarizing(
            task_id, worker_id="worker-1", transcript="转写文本"
        )
        await s.commit()

    async with session_factory() as s:
        row = await fetch_task(s, task_id)
        assert row.status == TaskStatus.SUMMARIZING.value
        assert row.transcript == "转写文本"
        assert row.locked_by == "worker-1"  # 仍在持有租约

    summary = {"summary": "一句话", "key_points": ["a"], "todos": []}
    async with session_factory() as s:
        assert await TaskDao(s).mark_done(
            task_id, worker_id="worker-1", summary=summary
        )
        await s.commit()

    async with session_factory() as s:
        row = await fetch_task(s, task_id)
        assert row.status == TaskStatus.DONE.value
        assert row.summary == summary
        assert row.locked_by is None
        assert row.lease_expires_at is None
        assert row.error_msg is None


async def test_renew_lease_requires_ownership_and_extends_expiry(session_factory):
    task_id = await _one_inflight(session_factory, lease_seconds=10)

    async with session_factory() as s:
        before = (await fetch_task(s, task_id)).lease_expires_at

    async with session_factory() as s:
        # 非持有者无法续约
        assert not await TaskDao(s).renew_lease(
            task_id, worker_id="worker-other", lease_seconds=600
        )
        await s.commit()
    async with session_factory() as s:
        assert (await fetch_task(s, task_id)).lease_expires_at == before

    async with session_factory() as s:
        assert await TaskDao(s).renew_lease(
            task_id, worker_id="worker-1", lease_seconds=600
        )
        await s.commit()
    async with session_factory() as s:
        after = (await fetch_task(s, task_id)).lease_expires_at
    assert after > before
    assert after > utcnow() + timedelta(seconds=400)


async def test_mark_summarizing_rejected_for_non_owner(session_factory):
    """fencing：锁被他人持有（或被回收）时不得写入 transcript / 状态。"""
    task_id = await _one_inflight(session_factory)

    async with session_factory() as s:
        assert not await TaskDao(s).mark_summarizing(
            task_id, worker_id="worker-other", transcript="不该写入"
        )
        await s.commit()

    async with session_factory() as s:
        row = await fetch_task(s, task_id)
        assert row.status == TaskStatus.TRANSCRIBING.value
        assert row.transcript is None
        assert row.locked_by == "worker-1"


async def test_mark_done_rejected_for_non_owner(session_factory):
    task_id = await _one_inflight(session_factory)

    async with session_factory() as s:
        assert not await TaskDao(s).mark_done(
            task_id, worker_id="worker-other", summary={"summary": "不该写入"}
        )
        await s.commit()

    async with session_factory() as s:
        row = await fetch_task(s, task_id)
        assert row.status == TaskStatus.TRANSCRIBING.value
        assert row.summary is None
        assert row.locked_by == "worker-1"


async def test_mark_failure_requires_ownership(session_factory):
    task_id = await _one_inflight(session_factory)

    async with session_factory() as s:
        assert (
            await TaskDao(s).mark_failure(
                task_id, worker_id="worker-other", error_msg="x", max_retries=3
            )
            is None
        )
        await s.commit()

    async with session_factory() as s:
        row = await fetch_task(s, task_id)
        assert row.retry_count == 0
        assert row.error_msg is None
        assert row.status == TaskStatus.TRANSCRIBING.value


async def _relock_after_backoff(session_factory, task_id):
    """把 ``next_retry_at`` 拨到过去并重新抢占，模拟「退避到期 → 派发器再次拾取」。

    生产流程是 fail → pending → claim → fail，``mark_failure`` 每次都会清空
    ``locked_by``，所以连续两次失败之间必须重新抢占。这里不真的 sleep，靠回拨
    时间来完成同一件事。
    """
    async with session_factory() as s:
        await s.execute(
            text(
                "UPDATE tasks SET next_retry_at = timezone('utc', now()) - interval '1 second'"
                " WHERE id = :id"
            ),
            {"id": task_id},
        )
        await s.commit()

    async with session_factory() as s:
        claimed = await TaskDao(s).claim_pending(
            limit=1, worker_id="worker-1", lease_seconds=300
        )
        await s.commit()
    assert claimed == [task_id]


async def test_mark_failure_backoff_is_exponential_then_exhausts(session_factory):
    """退避为 ``now + 2^retry_count`` 秒；达到 max_retries 后再失败即 failed。

    以前只断言 SQL 里出现 "power"/"CASE"，退避写错（次数、底数、单位）不会被发现。
    """
    task_id = await _one_inflight(session_factory, lease_seconds=300)

    observed: list[float] = []
    for expected_count in (1, 2):
        async with session_factory() as s:
            before = utcnow()
            row = await TaskDao(s).mark_failure(
                task_id, worker_id="worker-1", error_msg="boom", max_retries=2
            )
            await s.commit()

        assert row == (expected_count, TaskStatus.PENDING.value)

        async with session_factory() as s:
            task = await fetch_task(s, task_id)
            observed.append((task.next_retry_at - before).total_seconds())
            assert task.retry_count == expected_count
            assert task.error_msg == "boom"
            assert task.locked_by is None
            assert task.lease_expires_at is None

        await _relock_after_backoff(session_factory, task_id)

    assert 1.4 < observed[0] < 2.6, f"第一次退避应约为 2s，实际 {observed[0]}"
    assert 3.4 < observed[1] < 4.6, f"第二次退避应约为 4s，实际 {observed[1]}"

    async with session_factory() as s:
        pending_expiry = (await fetch_task(s, task_id)).next_retry_at

    async with session_factory() as s:
        row = await TaskDao(s).mark_failure(
            task_id, worker_id="worker-1", error_msg="boom again", max_retries=2
        )
        await s.commit()
    assert row == (3, TaskStatus.FAILED.value)

    async with session_factory() as s:
        task = await fetch_task(s, task_id)
        assert task.status == TaskStatus.FAILED.value
        # 终态不再安排重试时间
        assert task.next_retry_at == pending_expiry
        assert task.error_msg == "boom again"


async def test_reclaim_in_flight_include_own_locks_only_touches_own_or_expired(
    session_factory,
):
    """``include_own_locks=True``：回收「本 worker 的 in-flight」+「租约已过期」。

    这正是重启恢复与周期回收的差别所在，旧测试只用 ``assert " OR " in compiled`` 代替。
    """
    async with session_factory() as s:
        own_live = await make_task(
            s, await make_recording(s),
            status=TaskStatus.TRANSCRIBING.value, locked_by="worker-1", lease_seconds=3600,
        )
        own_summarizing = await make_task(
            s, await make_recording(s),
            status=TaskStatus.SUMMARIZING.value, locked_by="worker-1", lease_seconds=3600,
        )
        other_expired = await make_task(
            s, await make_recording(s),
            status=TaskStatus.TRANSCRIBING.value, locked_by="worker-2", lease_seconds=-60,
        )
        other_live = await make_task(
            s, await make_recording(s),
            status=TaskStatus.TRANSCRIBING.value, locked_by="worker-2", lease_seconds=3600,
        )
        await s.commit()
        reclaimed_ids = [own_live.id, own_summarizing.id, other_expired.id]
        untouched_id = other_live.id

    async with session_factory() as s:
        n = await TaskDao(s).reclaim_in_flight(
            worker_id="worker-1", include_own_locks=True, error_msg="restart"
        )
        await s.commit()
    assert n == 3

    async with session_factory() as s:
        for tid in reclaimed_ids:
            row = await fetch_task(s, tid)
            assert row.status == TaskStatus.PENDING.value
            assert row.locked_by is None
            assert row.lease_expires_at is None
            assert row.error_msg == "restart"

        survivor = await fetch_task(s, untouched_id)
        assert survivor.status == TaskStatus.TRANSCRIBING.value
        assert survivor.locked_by == "worker-2"


async def test_reclaim_in_flight_expired_only_leaves_own_live_lock_alone(session_factory):
    """``include_own_locks=False``（周期回收）：只动过期租约，不动自己在跑的任务。"""
    async with session_factory() as s:
        own_live = await make_task(
            s, await make_recording(s),
            status=TaskStatus.TRANSCRIBING.value, locked_by="worker-1", lease_seconds=3600,
        )
        other_expired = await make_task(
            s, await make_recording(s),
            status=TaskStatus.SUMMARIZING.value, locked_by="worker-2", lease_seconds=-60,
        )
        await s.commit()
        live_id, expired_id = own_live.id, other_expired.id

    async with session_factory() as s:
        n = await TaskDao(s).reclaim_in_flight(
            worker_id="worker-1", include_own_locks=False, error_msg="expired"
        )
        await s.commit()
    assert n == 1

    async with session_factory() as s:
        assert (await fetch_task(s, live_id)).status == TaskStatus.TRANSCRIBING.value
        assert (await fetch_task(s, expired_id)).status == TaskStatus.PENDING.value


async def test_reclaim_in_flight_ignores_terminal_statuses(session_factory):
    """终态任务即使带过期租约也不能被拉回 pending。"""
    async with session_factory() as s:
        done = await make_task(
            s, await make_recording(s),
            status=TaskStatus.DONE.value, locked_by="worker-contact", lease_seconds=-60,
        )
        failed = await make_task(
            s, await make_recording(s),
            status=TaskStatus.FAILED.value, locked_by="worker-1", lease_seconds=-60,
        )
        await s.commit()
        done_id, failed_id = done.id, failed.id

    async with session_factory() as s:
        n = await TaskDao(s).reclaim_in_flight(
            worker_id="worker-1", include_own_locks=True, error_msg="restart"
        )
        await s.commit()
    assert n == 0

    async with session_factory() as s:
        assert (await fetch_task(s, done_id)).status == TaskStatus.DONE.value
        assert (await fetch_task(s, failed_id)).status == TaskStatus.FAILED.value


async def test_requeue_failed_resets_every_result_field(session_factory):
    async with session_factory() as s:
        task = await make_task(
            s, await make_recording(s),
            status=TaskStatus.FAILED.value,
            retry_count=4,
            locked_by=None,
        )
        task.error_msg = "boom"
        task.transcript = "半截转写"
        task.summary = {"summary": "半截摘要"}
        await s.commit()
        task_id = task.id

    async with session_factory() as s:
        requeued = await TaskDao(s).requeue_failed(task_id)
        await s.commit()

    assert requeued is not None
    async with session_factory() as s:
        row = await fetch_task(s, task_id)
        assert row.status == TaskStatus.PENDING.value
        assert row.retry_count == 0
        assert row.error_msg is None
        assert row.transcript is None
        assert row.summary is None
        assert row.locked_by is None
        assert row.lease_expires_at is None
        assert row.next_retry_at <= utcnow()


@pytest.mark.parametrize(
    "status",
    [
        TaskStatus.PENDING.value,
        TaskStatus.TRANSCRIBING.value,
        TaskStatus.SUMMARIZING.value,
        TaskStatus.DONE.value,
    ],
)
async def test_requeue_failed_returns_none_for_non_failed(session_factory, status: str):
    async with session_factory() as s:
        task = await make_task(
            s,
            await make_recording(s),
            status=status,
            locked_by="worker-1" if status != TaskStatus.PENDING.value else None,
            lease_seconds=300 if status != TaskStatus.PENDING.value else None,
        )
        await s.commit()
        task_id = task.id

    async with session_factory() as s:
        assert await TaskDao(s).requeue_failed(task_id) is None
        await s.commit()

    async with session_factory() as s:
        assert (await fetch_task(s, task_id)).status == status


async def test_reset_aborted_only_touches_own_in_flight(session_factory):
    async with session_factory() as s:
        own = await make_task(
            s, await make_recording(s),
            status=TaskStatus.SUMMARIZING.value, locked_by="worker-1", lease_seconds=300,
        )
        other = await make_task(
            s, await make_recording(s),
            status=TaskStatus.SUMMARIZING.value, locked_by="worker-2", lease_seconds=300,
        )
        done = await make_task(
            s, await make_recording(s), status=TaskStatus.DONE.value,
        )
        await s.commit()
        own_id, other_id, done_id = own.id, other.id, done.id

    async with session_factory() as s:
        n = await TaskDao(s).reset_aborted(
            [own_id, other_id, done_id], worker_id="worker-1", error_msg="优雅关闭"
        )
        await s.commit()
    assert n == 1

    async with session_factory() as s:
        row = await fetch_task(s, own_id)
        assert row.status == TaskStatus.PENDING.value
        assert row.error_msg == "优雅关闭"
        assert row.locked_by is None
        assert (await fetch_task(s, other_id)).locked_by == "worker-2"
        assert (await fetch_task(s, done_id)).status == TaskStatus.DONE.value
