import logging
import time
from typing import Type

import click
import psycopg2
import redis
from elasticsearch import Elasticsearch
from psycopg2.extras import DictCursor

from etl.config import CONFIG
from etl.pg_to_es import extracters
from etl.pg_to_es.extracters import IPEMExtracter, TargetExtracer
from etl.pg_to_es.loaders import Loader
from etl.pg_to_es.pipelines import MoviesETL
from etl.pg_to_es.transformers import PgToESTransformer
from etl.state import GenericFileStorage, RedisQueue, State

pg_dsl = {
    "dbname": CONFIG.db_name,
    "user": CONFIG.db_user,
    "password": CONFIG.db_password,
    "host": CONFIG.db_host,
    "port": CONFIG.db_port,
}

LOGGER_NAME = "cli.log"
logger = logging.getLogger(LOGGER_NAME)
logger.addHandler(logging.FileHandler(LOGGER_NAME))


@click.group()
def messages():
    pass


def get_es() -> Elasticsearch:
    url = f"http://{CONFIG.es_host}:{CONFIG.es_port}"
    es = Elasticsearch(url)
    return es


@click.command()
@click.option("--count-p", type=int)
def hello(count_p: int):
    logger.info("HELLO!")
    for _ in range(count_p):
        print("hello")
        time.sleep(15)


@click.command()
@click.option("--extracter", type=str)
@click.option("--batch-size", type=int)
@click.option("--queue-name", type=str)
def fill_base_q(extracter: str, queue_name: str, batch_size: int):
    logger.info(f"Start task filling {queue_name!r} queue")
    extr_class: Type[IPEMExtracter] = getattr(extracters, extracter)
    logger.info(f"Retrieve lextracer {extracter!r}")
    with psycopg2.connect(**pg_dsl, cursor_factory=DictCursor) as pg_conn:
        extracer = extr_class(
            pg_connection=pg_conn,
            state=State(GenericFileStorage()),
            batch_size=batch_size,
        )
        baes_prod = extracer.produce_base()
        with redis.Redis(
            port=CONFIG.redis_port,
            host=CONFIG.redis_host,
            decode_responses=True,
        ) as redis_conn:
            queue = RedisQueue(q_name=queue_name, conn=redis_conn)
            logger.info("prepare iterative loading to queue")
            for base_batch in baes_prod:
                logger.info(f"prepare to load batch of {len(base_batch)}")
                queue.update(base_batch)
                logger.info(f"loaded batch of {len(base_batch)}")


@click.command()
@click.option("--target-queue", type=str)
@click.option("--extracter", type=str)
@click.option("--source-queue", type=str)
@click.option("--batch-size", type=int)
def fill_q_from_q(
    target_queue: str,
    extracter: str,
    source_queue: str,
    batch_size: int,
):
    with redis.Redis(
        port=CONFIG.redis_port,
        host=CONFIG.redis_host,
        decode_responses=True,
    ) as redis_conn:
        source_queue_ = RedisQueue(q_name=source_queue, conn=redis_conn)
        target_queue_ = RedisQueue(q_name=target_queue, conn=redis_conn)

        base_prod = source_queue_.get_iterator(batch_size)
        extr_class: Type[IPEMExtracter] = getattr(extracters, extracter)
        with psycopg2.connect(**pg_dsl, cursor_factory=DictCursor) as pg_conn:
            extracer_ = extr_class(
                pg_connection=pg_conn,
                state=State(GenericFileStorage()),
                batch_size=batch_size,
            )
            for base_batch in base_prod:
                target_ids = extracer_.get_target_ids(base_batch)
                target_queue_.update(target_ids)


@click.command()
@click.option("--queue", type=str)
@click.option("--batch-size", type=int)
def fill_es_from_q(queue: str, batch_size: int):
    with redis.Redis(
        port=CONFIG.redis_port,
        host=CONFIG.redis_host,
        decode_responses=True,
    ) as redis_conn:
        source_queue_ = RedisQueue(q_name=queue, conn=redis_conn)
        with psycopg2.connect(**pg_dsl, cursor_factory=DictCursor) as pg_conn:
            q_extracter = TargetExtracer(
                pg_connection=pg_conn, u_storage=source_queue_, batch_size=batch_size
            )
            transformer = PgToESTransformer()
            loader = Loader(index="movies", es_factory=get_es, debug=True)
            etl = MoviesETL(
                extracter=q_extracter,
                transformer=transformer,
                loader=loader,
            )
            etl.run()


messages.add_command(hello)
messages.add_command(fill_base_q)
messages.add_command(fill_q_from_q)
messages.add_command(fill_es_from_q)

if __name__ == "__main__":
    messages()
