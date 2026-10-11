from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from redis_client import redis_mgr


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # Initialize connection pool inside the active event loop
    redis_mgr.init_client("redis://localhost:6379")
    app.state.redis_client = redis_mgr.client

    try:
        yield
    finally:
        # Clean shutdown
        await redis_mgr.close()
