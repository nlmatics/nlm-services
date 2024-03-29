import connexion
from flask import jsonify
from flask import make_response

from server.storage import nosql_db


def get_tasks(
    user,
    token_info,
):
    body = connexion.request.get_json()
    user_json = token_info["user_obj"]
    query = {"user_id": user_json["id"]}
    # if "user_id" in body:
    #     query["user_id"] = body["user_id"]

    if "doc_id" in body:
        query["body.doc_id"] = body["doc_id"]

    if "workspace_idx" in body:
        query["body.workspace_idx"] = body["workspace_idx"]

    if "task_name" in body:
        query["task_name"] = body["task_name"]

    if "status" in body:
        query["status"] = body["status"]

    offset = body["offset"] if "offset" in body else 0
    task_per_page = body["task_per_page"] if "task_per_page" in body else 10000

    tasks = nosql_db.get_task(query, offset=offset, task_per_page=task_per_page)
    return make_response(jsonify(tasks))
