import os

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/aila_test")

import pytest  # noqa: E402
from httpx import ASGITransport, AsyncClient  # noqa: E402
from sqlalchemy import text  # noqa: E402

from app.db import Base, SessionLocal, engine  # noqa: E402
from app.main import app  # noqa: E402


@pytest.fixture(autouse=True)
async def clean_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.execute(text("TRUNCATE dead_letters, messages, conversations"))
    yield
    # Each test runs on its own event loop; pooled asyncpg connections must not outlive it.
    await engine.dispose()


@pytest.fixture
async def client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest.fixture
async def db():
    async with SessionLocal() as session:
        yield session


async def fetch_message(message_id):
    async with SessionLocal() as session:
        row = await session.execute(
            text(
                """
                SELECT *, available_at > now() AS deferred
                FROM messages WHERE id = :id
                """
            ),
            {"id": message_id},
        )
        return row.mappings().one()


async def dead_letter_count(message_id=None):
    async with SessionLocal() as session:
        if message_id is None:
            res = await session.execute(text("SELECT count(*) FROM dead_letters"))
        else:
            res = await session.execute(
                text("SELECT count(*) FROM dead_letters WHERE message_id = :id"), {"id": message_id}
            )
        return res.scalar_one()
