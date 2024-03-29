import os

from pymongo import MongoClient

from server.controllers.extraction_controller import apply_template
from server.storage import nosql_db as nosqldb


def get_field_bundle_ids_from_ws(db, workspace_id):
    field_bundles = db["field_bundle"].find({"workspace_id": workspace_id})
    field_bundle_id_list = []
    for field_bundle in field_bundles:
        if "id" in field_bundle:
            field_bundle_id_list.append(field_bundle["id"])
    return field_bundle_id_list


def re_run_extraction():
    workspace_list = nosqldb.get_all_workspaces()
    mongo_str = os.environ["MONGO_HOST"]
    db_name = os.environ["MONGO_DATABASE"]
    db = MongoClient(mongo_str)[db_name]
    for ws in workspace_list:
        field_bundle_id_list = get_field_bundle_ids_from_ws(db=db, workspace_id=ws.id)

        for field_bundle_id in field_bundle_id_list:
            try:
                apply_template(
                    workspace_idx=ws.id,
                    field_bundle_idx=field_bundle_id,
                    override_topic="ALL",
                )
            except Exception:
                pass


re_run_extraction()
