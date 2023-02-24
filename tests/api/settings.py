from pydantic import BaseSettings, Field


class TestSettings(BaseSettings):
    es_host: str = Field("elasticsearch", env="ELASTIC_HOST")
    es_port: int = Field(9200, env="ELASTIC_PORT")
    redis_host: str = Field("redis", env="REDIS_HOST")
    redis_port: int = Field(6379, env="REDIS_PORT")
    api_host: str = Field("movies_api", env="API_HOST")
    api_port: int = Field(8001, env="MOVIES_API_PORT")
    service_wait_timeout: int = Field(30, env="SERVICE_WAIT_TIMEOUT")  # seconds
    service_wait_interval: int = Field(1, env="SERVICE_WAIT_INTERVAL")  # seconds
