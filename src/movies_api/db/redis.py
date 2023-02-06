from aioredis import Redis

redis: Redis


async def get_redis() -> Redis:
    return redis  # noqa: F821
