# import psycopg2
# import os
#
# DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://dq_user:dq_pass@localhost:5432/dq_db")
#
#
# def get_db_connection():
#     return psycopg2.connect(DATABASE_URL)

import os
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://dq_user:dq_pass@localhost:5432/dq_db"
)

# Параметры пула. Значения подобраны для нагрузки ~50 RPS.
# max_size обычно = (количество воркеров uvicorn) * 2 + 1.
POOL_MIN_SIZE = int(os.getenv("DB_POOL_MIN", "2"))
POOL_MAX_SIZE = int(os.getenv("DB_POOL_MAX", "10"))
POOL_TIMEOUT = float(os.getenv("DB_POOL_TIMEOUT", "30.0"))

# Глобальный пул. Создаётся в lifespan, закрывается при shutdown.
_pool: AsyncConnectionPool | None = None


async def init_pool() -> AsyncConnectionPool:
    """Инициализация пула при старте приложения."""
    global _pool
    if _pool is not None:
        return _pool

    _pool = AsyncConnectionPool(
        conninfo=DATABASE_URL,
        min_size=POOL_MIN_SIZE,
        max_size=POOL_MAX_SIZE,
        timeout=POOL_TIMEOUT,
        open=False,  # открытие контролируем явно
        kwargs={"autocommit": False},
    )
    await _pool.open(wait=True, timeout=10)
    logger.info(
        f"DB pool opened: min={POOL_MIN_SIZE}, max={POOL_MAX_SIZE}"
    )
    return _pool


async def close_pool() -> None:
    """Корректное закрытие пула при shutdown."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("DB pool closed")


def get_pool() -> AsyncConnectionPool:
    """Доступ к пулу из любого места кода."""
    if _pool is None:
        raise RuntimeError("DB pool is not initialized. Call init_pool() first.")
    return _pool


@asynccontextmanager
async def get_connection() -> AsyncIterator[AsyncConnection]:
    """
    Контекстный менеджер для получения соединения из пула.
    Автоматически возвращает соединение в пул и корректно обрабатывает ошибки.

    Использование:
        async with get_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
    """
    pool = get_pool()
    async with pool.connection() as conn:
        try:
            yield conn
        except Exception:
            await conn.rollback()
            raise
        else:
            await conn.commit()


async def db_dependency() -> AsyncIterator[AsyncConnection]:
    """FastAPI dependency: yield'ит соединение и возвращает его в пул."""
    async with get_connection() as conn:
        yield conn
