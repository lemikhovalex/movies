from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk


async def es_load(
    es_client: AsyncElasticsearch, index: str, data: list[dict]
) -> list[dict]:
    await async_bulk(
        es_client,
        actions=[
            {"_index": index, "_id": item["id"], "_source": item} for item in data
        ],
    )
    await es_client.indices.refresh(index=index)


def filter_uuid(data):
    return set([i["uuid"] for i in data])


def filter_int(x):
    if isinstance(x, int):
        return x
