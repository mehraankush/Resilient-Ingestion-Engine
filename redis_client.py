import redis.asyncio as aioredis

class RedisManager:
    def __init__(self):
        self._client: aioredis.Redis | None = None

    def init_client(self, url: str = "redis://localhost:6379"):
        """Call once on app startup."""
        self._client = aioredis.from_url(
            url,
            encoding="utf-8",
            decode_responses=True,
            max_connections=20
        )

    async def close(self):
        """Call on app shutdown."""
        if self._client:
            await self._client.aclose()
            self._client = None

    @property
    def client(self) -> aioredis.Redis:
        """Central property to get the active client."""
        if self._client is None:
            raise RuntimeError("Redis client is not initialized. Call init_client() first.")
        return self._client

# singleton
redis_mgr = RedisManager()
