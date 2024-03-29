import logging

from server import err_response
from server.storage import nosql_db
from server.utils import str_utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def add_recent_opened_document(
    user,
    token_info,
    doc_id,
):
    # get attributes
    workspace_id = nosql_db.get_document_info_by_id(doc_id).workspace_id
    user_json = token_info.get("user_obj", None)
    user_permission, _ws = nosql_db.get_user_permission(
        workspace_id,
        email=user,
        user_json=user_json,
    )
    if user_permission not in ["admin", "owner", "editor", "viewer"]:
        err_str = "Not authorized to access this workspace"
        log_str = f"user {user} not authorized to add recent documents to workspace {workspace_id}"
        logger.info(log_str)
        return err_response(err_str, 403)

    user_id = nosql_db.get_user_by_email(user).id

    # make the history record object
    timestamp = str_utils.timestamp_as_str()
    user_history = {
        "user_id": user_id,
        "workspace_id": workspace_id,
        "doc_id": doc_id,
        "timestamp": timestamp,
        "action": "opened_document",
        "details": {},
    }
    nosql_db.create_file_history(user_history)


def get_workspace_dashboard_data(user, token_info, workspace_id, ndoc=10):
    user_json = token_info.get("user_obj", None)
    user_permission, _ws = nosql_db.get_user_permission(
        workspace_id,
        email=user,
        user_json=user_json,
    )
    if user_permission not in ["admin", "owner", "editor", "viewer"]:
        err_str = "Not authorized to access this workspace"
        log_str = (
            f"user {user} not authorized to retrieve dashboard data for {workspace_id}"
        )
        logger.info(log_str)
        return err_response(err_str, 403)

    def remove_duplicate(docs, ndoc):
        doc_ids = set()
        res = []
        for doc in docs:
            if doc.doc_id not in doc_ids:
                doc_ids.add(doc.doc_id)
                res.append(doc)
            if len(res) == ndoc:
                break
        return res

    user_id = user_json["id"]
    recently_uploaded = remove_duplicate(
        nosql_db.get_file_history_in_workspace(
            user_id,
            workspace_id,
            "uploaded_document",
        ),
        ndoc,
    )
    recently_accessed = remove_duplicate(
        nosql_db.get_file_history_in_workspace(
            user_id,
            workspace_id,
            "opened_document",
        ),
        ndoc,
    )
    return {
        "recently_accessed": recently_accessed,
        "recently_uploaded": recently_uploaded,
    }
