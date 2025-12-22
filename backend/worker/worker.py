import asyncio
import logging
import os
import random
import uuid
from dataclasses import dataclass
from typing import List

from sqlalchemy import text

from app.db import SessionLocal, init_db

# Simulation settings (match assignment defaults)
MIN_DELAY_SECONDS = int(os.getenv("MIN_DELAY_SECONDS", "5"))
MAX_DELAY_SECONDS = int(os.getenv("MAX_DELAY_SECONDS", str(4 * 60)))
FAIL_RATE = float(os.getenv("FAIL_RATE", "0.2"))

# Reliability settings
POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "1.0"))
CLAIM_BATCH = int(os.getenv("CLAIM_BATCH", "10"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "8"))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "3"))
PROCESSING_TIMEOUT_SECONDS = int(os.getenv("PROCESSING_TIMEOUT_SECONDS", str(5 * 60)))  # lease per attempt
BACKOFF_BASE_SECONDS = int(os.getenv("BACKOFF_BASE_SECONDS", "2"))
BACKOFF_MAX_SECONDS = int(os.getenv("BACKOFF_MAX_SECONDS", "120"))

log = logging.getLogger("worker")


@dataclass
class Job:
    id: uuid.UUID
    user_text: str
    attempt_count: int


def backoff_seconds(attempt: int) -> int:
    # attempt is already incremented when claimed; attempt=1 => 2s, attempt=2 => 4s ...
    secs = BACKOFF_BASE_SECONDS ** max(1, attempt)
    return int(min(secs, BACKOFF_MAX_SECONDS))


async def reap_expired_jobs():
    # A worker that crashes or stalls mid-processing leaves its row in PROCESSING.
    # Expired leases become TIMEOUT; those with attempts left are requeued, the rest
    # are dead-lettered and stay TIMEOUT.
    async with SessionLocal() as session:
        async with session.begin():
            await session.execute(
                text(
                    """
                    WITH expired AS (
                        UPDATE messages
                        SET state = 'TIMEOUT',
                            last_error = 'processing lease expired',
                            updated_at = now()
                        WHERE state = 'PROCESSING'
                          AND processing_deadline_at IS NOT NULL
                          AND processing_deadline_at < now()
                        RETURNING id, user_id, conversation_id, seq, attempt_count
                    )
                    INSERT INTO dead_letters (id, message_id, user_id, conversation_id, seq, reason, created_at)
                    SELECT gen_random_uuid(), id, user_id, conversation_id, seq, 'processing lease expired', now()
                    FROM expired
                    WHERE attempt_count >= :max_attempts
                    """
                ),
                {"max_attempts": MAX_ATTEMPTS},
            )

            await session.execute(
                text(
                    """
                    UPDATE messages
                    SET state = 'RECEIVED',
                        available_at = now() + make_interval(secs => :delay),
                        updated_at = now()
                    WHERE state = 'TIMEOUT'
                      AND attempt_count < :max_attempts
                    """
                ),
                {"delay": BACKOFF_BASE_SECONDS, "max_attempts": MAX_ATTEMPTS},
            )


async def claim_jobs(limit: int | None = None) -> List[Job]:
    async with SessionLocal() as session:
        async with session.begin():
            # Claim RECEIVED jobs that are available, in send-order within a conversation (seq),
            # but we can process across conversations/users concurrently.
            res = await session.execute(
                text(
                    """
                    WITH cte AS (
                        SELECT id
                        FROM messages
                        WHERE state = 'RECEIVED'
                          AND available_at <= now()
                        ORDER BY created_at ASC
                        LIMIT :batch
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE messages
                    SET state = 'PROCESSING',
                        attempt_count = attempt_count + 1,
                        processing_started_at = now(),
                        processing_deadline_at = now() + make_interval(secs => :lease_secs),
                        updated_at = now()
                    WHERE id IN (SELECT id FROM cte)
                    RETURNING id, user_text, attempt_count
                    """
                ),
                {"batch": CLAIM_BATCH if limit is None else limit, "lease_secs": PROCESSING_TIMEOUT_SECONDS},
            )
            rows = res.mappings().all()
            return [Job(id=row["id"], user_text=row["user_text"], attempt_count=row["attempt_count"]) for row in rows]


# Every transition out of PROCESSING is guarded by the attempt number, so a worker
# whose lease expired (and whose job was reclaimed by someone else) cannot overwrite
# the newer attempt's outcome.
async def mark_completed(job: Job, assistant_text: str):
    async with SessionLocal() as session:
        async with session.begin():
            await session.execute(
                text(
                    """
                    UPDATE messages
                    SET state = 'COMPLETED',
                        assistant_text = :assistant_text,
                        completed_at = now(),
                        updated_at = now(),
                        last_error = NULL
                    WHERE id = :id
                      AND state = 'PROCESSING'
                      AND attempt_count = :attempt
                    """
                ),
                {"id": job.id, "attempt": job.attempt_count, "assistant_text": assistant_text},
            )


async def mark_failed(job: Job, err: str):
    params = {"id": job.id, "attempt": job.attempt_count, "err": err}
    async with SessionLocal() as session:
        async with session.begin():
            if job.attempt_count < MAX_ATTEMPTS:
                await session.execute(
                    text(
                        """
                        UPDATE messages
                        SET state = 'RECEIVED',
                            available_at = now() + make_interval(secs => :delay),
                            last_error = :err,
                            updated_at = now()
                        WHERE id = :id
                          AND state = 'PROCESSING'
                          AND attempt_count = :attempt
                        """
                    ),
                    {**params, "delay": backoff_seconds(job.attempt_count)},
                )
                return

            await session.execute(
                text(
                    """
                    WITH failed AS (
                        UPDATE messages
                        SET state = 'FAILED',
                            last_error = :err,
                            updated_at = now()
                        WHERE id = :id
                          AND state = 'PROCESSING'
                          AND attempt_count = :attempt
                        RETURNING id, user_id, conversation_id, seq
                    )
                    INSERT INTO dead_letters (id, message_id, user_id, conversation_id, seq, reason, created_at)
                    SELECT gen_random_uuid(), id, user_id, conversation_id, seq, :err, now()
                    FROM failed
                    """
                ),
                params,
            )


async def process_job(job: Job):
    delay = random.randint(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
    await asyncio.sleep(delay)
    if random.random() < FAIL_RATE:
        await mark_failed(job, f"simulated failure after {delay}s (attempt {job.attempt_count})")
    else:
        # A placeholder "AI" response. In real integration, call your LLM here.
        await mark_completed(job, f"[AI] Response (attempt {job.attempt_count}): {job.user_text}")


def _log_crash(task: asyncio.Task) -> None:
    if not task.cancelled() and task.exception() is not None:
        # The row stays PROCESSING; the reaper requeues it once its lease expires.
        log.error("job crashed", exc_info=task.exception())


async def run(stop: asyncio.Event | None = None):
    """Keep up to CONCURRENCY jobs in flight.

    Jobs are only claimed for free slots, so a claimed job's lease never ticks
    while it waits behind others, and one slow job does not hold back the rest
    of its batch.
    """
    in_flight: set[asyncio.Task] = set()
    while stop is None or not stop.is_set():
        await reap_expired_jobs()
        free = CONCURRENCY - len(in_flight)
        jobs = await claim_jobs(min(CLAIM_BATCH, free)) if free > 0 else []
        for job in jobs:
            task = asyncio.create_task(process_job(job))
            in_flight.add(task)
            task.add_done_callback(in_flight.discard)
            task.add_done_callback(_log_crash)
        if not jobs:
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
    await asyncio.gather(*in_flight, return_exceptions=True)


async def main():
    logging.basicConfig(level=logging.INFO)
    await init_db()
    await run()


if __name__ == "__main__":
    asyncio.run(main())
