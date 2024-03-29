import logging

from server import err_response
from server.storage import nosql_db

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def check_document_access(email_id, doc_id):
    doc = nosql_db.get_document_info_by_id(doc_id)
    if doc:
        return check_workspace_access(email_id, doc.workspace_id)
    else:
        logger.error(f"doc with id {doc_id} not found")
        return False


def check_workspace_access(email_id, workspace_id):
    ws = nosql_db.get_workspace_by_id(workspace_id)
    if not ws:
        logger.error(f"workspace with id {workspace_id} not found")
        return err_response("invalid workspace")
    email_domain = email_id.split("@")[1]
    can_access = (
        ws.shared_with == "*"
        or "*" in ws.shared_with
        or email_id in ws.shared_with
        or email_domain in ws.shared_with
        or nosql_db.is_user_email_matches_id(email_id, ws.user_id)
    )
    if not can_access:
        err_str = "Not authorized to access workspace"
        log_str = f"{email_id} not authorized to access workspace {ws}"
        logger.info(log_str)
        return err_response(err_str, 403)
