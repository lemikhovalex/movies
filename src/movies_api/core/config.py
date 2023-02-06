import os
from logging import config as logging_config

from core.logger import LOGGING

logging_config.dictConfig(LOGGING)

PROJECT_NAME = os.getenv("PROJECT_NAME", "movies")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

ELASTIC_HOST = os.getenv("ELASTIC_HOST", "elasticsearch")
ELASTIC_PORT = int(os.getenv("ELASTIC_PORT", 9200))

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

UVICORN_RELOAD = bool(os.getenv("UVICORN_RELOAD", False))

IS_DEBUG = bool(os.getenv("DEBUG", 0))

MAX_ES_SEARCH_FROM_SIZE = 5 if IS_DEBUG else 10_000
REDIS_CACHE_EXPIRE = 60

PORT = int(os.getenv("PORT", 8001))
