import asyncio

import pytest
from sqlalchemy import text

from app.crud import create_message
from app.db import SessionLocal
from worker import worker as w

from .conftest import dead_letter_count, fetch_message


async def new_message(text_content="hello", conversation_id="c1", user_id="u1"):
    async with SessionLocal() as session:
        msg = await create_message(
            session, user_id=user_id, conversation_id=conversation_id, text_content=text_content
        )
        return msg.id


async def expire_lease(message_id):
    async with SessionLocal() as session, session.begin():
        await session.execute(
            text("UPDATE messages SET processing_deadline_at = now() - interval '1 second' WHERE id = :id"),
            {"id": message_id},
        )


@pytest.fixture
def instant(monkeypatch):
    monkeypatch.setattr(w, "MIN_DELAY_SECONDS", 0)
    monkeypatch.setattr(w, "MAX_DELAY_SECONDS", 0)
    monkeypatch.setattr(w, "MAX_ATTEMPTS", 3)


@pytest.mark.parametrize("attempt,expected", [(0, 2), (1, 2), (2, 4), (3, 8), (10, 120)])
def test_backoff_grows_exponentially_and_is_capped(attempt, expected):
    assert w.backoff_seconds(attempt) == expected


async def test_claim_moves_message_to_processing():
    mid = await new_message()

    jobs = await w.claim_jobs()

    assert [j.id for j in jobs] == [mid]
    assert jobs[0].attempt_count == 1
    row = await fetch_message(mid)
    assert row["state"] == "PROCESSING"
    assert row["processing_started_at"] is not None
    assert row["processing_deadline_at"] > row["processing_started_at"]


async def test_claim_skips_messages_not_yet_available():
    mid = await new_message()
    async with SessionLocal() as session, session.begin():
        await session.execute(
            text("UPDATE messages SET available_at = now() + interval '1 hour' WHERE id = :id"), {"id": mid}
        )

    assert await w.claim_jobs() == []


async def test_claim_respects_batch_size(monkeypatch):
    monkeypatch.setattr(w, "CLAIM_BATCH", 2)
    for i in range(5):
        await new_message(f"m{i}")

    assert len(await w.claim_jobs()) == 2


async def test_concurrent_claims_never_hand_out_the_same_message(monkeypatch):
    monkeypatch.setattr(w, "CLAIM_BATCH", 3)
    for i in range(6):
        await new_message(f"m{i}", conversation_id=f"c{i}")

    batches = await asyncio.gather(w.claim_jobs(), w.claim_jobs(), w.claim_jobs())

    ids = [j.id for batch in batches for j in batch]
    assert len(ids) == len(set(ids)) == 6


async def test_completed_job_stores_reply(instant):
    mid = await new_message("What is tax deduction?")
    [job] = await w.claim_jobs()

    await w.mark_completed(job, "reply")

    row = await fetch_message(mid)
    assert row["state"] == "COMPLETED"
    assert row["assistant_text"] == "reply"
    assert row["completed_at"] is not None
    assert row["last_error"] is None


async def test_failure_with_attempts_left_is_requeued_with_backoff(instant):
    mid = await new_message()
    [job] = await w.claim_jobs()

    await w.mark_failed(job, "boom")

    row = await fetch_message(mid)
    assert row["state"] == "RECEIVED"
    assert row["last_error"] == "boom"
    assert row["deferred"] is True
    assert await dead_letter_count() == 0


async def test_failure_on_last_attempt_is_dead_lettered(instant, monkeypatch):
    monkeypatch.setattr(w, "MAX_ATTEMPTS", 1)
    mid = await new_message()
    [job] = await w.claim_jobs()

    await w.mark_failed(job, "boom")

    row = await fetch_message(mid)
    assert row["state"] == "FAILED"
    assert await dead_letter_count(mid) == 1


async def test_retry_path_ends_in_failed_after_max_attempts(instant, monkeypatch):
    monkeypatch.setattr(w, "BACKOFF_BASE_SECONDS", 0)
    mid = await new_message()

    for _ in range(3):
        [job] = await w.claim_jobs()
        await w.mark_failed(job, f"attempt {job.attempt_count}")

    row = await fetch_message(mid)
    assert row["state"] == "FAILED"
    assert row["attempt_count"] == 3
    assert row["last_error"] == "attempt 3"
    assert await w.claim_jobs() == []


async def test_expired_lease_is_requeued_when_attempts_remain(instant):
    mid = await new_message()
    [job] = await w.claim_jobs()
    await w.mark_failed(job, "first attempt failed")
    async with SessionLocal() as session, session.begin():
        await session.execute(text("UPDATE messages SET available_at = now() WHERE id = :id"), {"id": mid})
    await w.claim_jobs()
    await expire_lease(mid)

    await w.reap_expired_jobs()

    row = await fetch_message(mid)
    assert row["state"] == "RECEIVED"
    assert row["attempt_count"] == 2
    assert row["last_error"] == "processing lease expired"
    assert await dead_letter_count() == 0


async def test_expired_lease_on_last_attempt_stays_timeout(instant, monkeypatch):
    monkeypatch.setattr(w, "MAX_ATTEMPTS", 1)
    mid = await new_message()
    await w.claim_jobs()
    await expire_lease(mid)

    await w.reap_expired_jobs()

    assert (await fetch_message(mid))["state"] == "TIMEOUT"
    assert await dead_letter_count(mid) == 1


async def test_stale_worker_cannot_overwrite_a_reclaimed_attempt(instant, monkeypatch):
    monkeypatch.setattr(w, "BACKOFF_BASE_SECONDS", 0)
    mid = await new_message()
    [stale] = await w.claim_jobs()
    await expire_lease(mid)
    await w.reap_expired_jobs()
    [current] = await w.claim_jobs()

    await w.mark_completed(stale, "late reply")
    await w.mark_failed(stale, "late failure")

    row = await fetch_message(mid)
    assert (row["state"], row["attempt_count"], row["assistant_text"]) == ("PROCESSING", 2, None)

    await w.mark_completed(current, "fresh reply")
    assert (await fetch_message(mid))["assistant_text"] == "fresh reply"


async def test_stale_final_failure_is_not_dead_lettered_twice(instant, monkeypatch):
    monkeypatch.setattr(w, "MAX_ATTEMPTS", 1)
    mid = await new_message()
    [job] = await w.claim_jobs()
    await expire_lease(mid)
    await w.reap_expired_jobs()

    await w.mark_failed(job, "late failure")

    row = await fetch_message(mid)
    assert row["state"] == "TIMEOUT"
    assert await dead_letter_count(mid) == 1


async def test_process_job_completes_on_success(instant, monkeypatch):
    monkeypatch.setattr(w, "FAIL_RATE", 0.0)
    mid = await new_message("ping")
    [job] = await w.claim_jobs()

    await w.process_job(job)

    row = await fetch_message(mid)
    assert row["state"] == "COMPLETED"
    assert "ping" in row["assistant_text"]


async def test_process_job_records_simulated_failure(instant, monkeypatch):
    monkeypatch.setattr(w, "FAIL_RATE", 1.0)
    mid = await new_message()
    [job] = await w.claim_jobs()

    await w.process_job(job)

    row = await fetch_message(mid)
    assert row["state"] == "RECEIVED"
    assert row["last_error"].startswith("simulated failure")


async def test_claim_limit_overrides_batch_size():
    for i in range(3):
        await new_message(f"m{i}")

    assert len(await w.claim_jobs(1)) == 1
    assert await w.claim_jobs(0) == []


async def wait_for(predicate, timeout=10.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not await predicate():
        assert loop.time() < deadline, "condition not reached in time"
        await asyncio.sleep(0.05)


async def count_states():
    async with SessionLocal() as session:
        res = await session.execute(text("SELECT state, count(*) FROM messages GROUP BY state"))
        return dict(res.all())


async def test_run_never_holds_more_jobs_than_it_can_process(monkeypatch):
    monkeypatch.setattr(w, "MIN_DELAY_SECONDS", 1)
    monkeypatch.setattr(w, "MAX_DELAY_SECONDS", 1)
    monkeypatch.setattr(w, "FAIL_RATE", 0.0)
    monkeypatch.setattr(w, "CONCURRENCY", 2)
    monkeypatch.setattr(w, "CLAIM_BATCH", 10)
    monkeypatch.setattr(w, "POLL_INTERVAL_SECONDS", 0.05)
    for i in range(5):
        await new_message(f"m{i}", conversation_id=f"c{i}")
    stop = asyncio.Event()
    runner = asyncio.create_task(w.run(stop))

    async def two_in_flight():
        return (await count_states()).get("PROCESSING") == 2

    await wait_for(two_in_flight)
    assert (await count_states()).get("RECEIVED") == 3

    async def all_done():
        return (await count_states()) == {"COMPLETED": 5}

    await wait_for(all_done)
    stop.set()
    await runner


async def test_run_keeps_retrying_until_dead_lettered(monkeypatch, instant):
    monkeypatch.setattr(w, "FAIL_RATE", 1.0)
    monkeypatch.setattr(w, "BACKOFF_BASE_SECONDS", 0)
    monkeypatch.setattr(w, "POLL_INTERVAL_SECONDS", 0.05)
    mid = await new_message()
    stop = asyncio.Event()
    runner = asyncio.create_task(w.run(stop))

    async def failed():
        return (await count_states()) == {"FAILED": 1}

    await wait_for(failed)
    stop.set()
    await runner
    assert (await fetch_message(mid))["attempt_count"] == 3
    assert await dead_letter_count(mid) == 1
