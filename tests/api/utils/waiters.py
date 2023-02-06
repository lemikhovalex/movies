import asyncio
import logging

from aioredis import Redis
from elasticsearch import AsyncElasticsearch
from settings import TestSettings

logger = logging.getLogger(__name__)
SETTINGS = TestSettings()


async def wait_for_es():
    url = f"http://{SETTINGS.es_host}:{SETTINGS.es_port}"
    es = AsyncElasticsearch(url)
    timeout = 0
    while not (await es.ping()) and timeout < SETTINGS.service_wait_timeout:
        await asyncio.sleep(SETTINGS.service_wait_interval)
        timeout += SETTINGS.service_wait_interval
    await es.close()


async def wait_for_redis():
    redis = Redis(host=SETTINGS.redis_host, port=SETTINGS.redis_port)
    timeout = 0
    while not (await redis.ping()) and timeout < SETTINGS.service_wait_timeout:
        await asyncio.sleep(SETTINGS.service_wait_interval)
        timeout += SETTINGS.service_wait_interval
    await redis.close()
