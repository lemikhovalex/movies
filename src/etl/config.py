from typing import Optional

from pydantic import BaseSettings


class Config(BaseSettings):
    es_host: str
    es_port: int

    db_host: str
    db_port: int
    db_user: str
    db_name: str
    db_password: str

    spark_master_host: str
    spark_master_port: int

    af_port: int
    redis_port: int = 6379
    redis_host: str = "redis"
    logger_path: Optional[str] = None


CONFIG = Config()
