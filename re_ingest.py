import logging
import os

from elasticsearch import Elasticsearch

from server.controllers.document_controller import re_ingest_documents_in_workspace
from server.storage import nosql_db as nosqldb

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def clear_es():
    if os.getenv("ES_SECRET"):
        es = Elasticsearch(
            os.getenv("ES_URL"),
            http_auth=("elastic", os.getenv("ES_SECRET")),
            timeout=3600,
        )
    else:
        es = Elasticsearch(
            os.getenv("ES_URL"),
            timeout=3600,
        )

    es.cluster.put_settings(
        {
            "transient": {
                "cluster.routing.allocation.total_shards_per_node": 1_000_000_000,
            },
        },
    )

    try:
        es.indices.delete("*")
        es.indices.put_settings(
            body={"index": {"highlight.max_analyzed_offset": 10_000_000}},
            index="*",
        )
    except Exception:
        pass


def re_ingest_all_documents():
    workspace_list = nosqldb.get_all_workspaces()
    user = nosqldb.get_user_by_email("default@nlmatics.com")
    token_info = {
        "user_obj": user.to_dict(),
    }
    for ws in workspace_list:
        if ws.id:
            try:
                re_ingest_documents_in_workspace(
                    "default@nlmatics.com",
                    token_info,
                    ws.id,
                )
            except Exception:
                pass


clear_es()
re_ingest_all_documents()
