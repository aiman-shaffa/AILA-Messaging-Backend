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
    await w.claim_jobs()
    await expire_lease(mid)

    await w.reap_expired_jobs()

    row = await fetch_message(mid)
    assert row["state"] == "RECEIVED"
    assert row["last_error"] == "processing lease expired"


async def test_expired_lease_on_last_attempt_stays_timeout(instant, monkeypatch):
    monkeypatch.setattr(w, "MAX_ATTEMPTS", 1)
    mid = await new_message()
    await w.claim_jobs()
    await expire_lease(mid)

    await w.reap_expired_jobs()

    assert (await fetch_message(mid))["state"] == "TIMEOUT"


async def test_process_job_completes_on_success(instant, monkeypatch):
    monkeypatch.setattr(w, "FAIL_RATE", 0.0)
    mid = await new_message("ping")
    [job] = await w.claim_jobs()

    await w.process_job(job, asyncio.Semaphore(1))

    row = await fetch_message(mid)
    assert row["state"] == "COMPLETED"
    assert "ping" in row["assistant_text"]


async def test_process_job_records_simulated_failure(instant, monkeypatch):
    monkeypatch.setattr(w, "FAIL_RATE", 1.0)
    mid = await new_message()
    [job] = await w.claim_jobs()

    await w.process_job(job, asyncio.Semaphore(1))

    row = await fetch_message(mid)
    assert row["state"] == "RECEIVED"
    assert row["last_error"].startswith("simulated failure")
