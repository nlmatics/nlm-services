import logging

import connexion
from flask import jsonify
from flask import make_response

from server import err_response
from server.models.bbox import BBox  # noqa: E501
from server.models.id_with_message import IdWithMessage  # noqa: E501
from server.storage import nosql_db
from server.utils import bbox_utils


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def update_bbox(
    user,
    token_info,
    doc_id,
):  # noqa: E501
    """Add New User defined Field (no bundle)

     # noqa: E501

    :param user:
    :param token_info:
    :param doc_id:

    :rtype: IdWithMessage
    """
    document = nosql_db.get_document_info_by_id(doc_id)
    user_permission, _ws = nosql_db.get_user_permission(
        document.workspace_id,
        email=user,
        user_json=token_info.get("user_obj", None) if token_info else None,
    )
    if user_permission not in ["admin", "owner", "editor"]:
        err_str = "Not authorized"
        log_str = (
            f"user {user} not authorized to modify bounding box info for {doc_id} "
            f"from workspace {document.workspace_id}",
        )
        logger.info(log_str)
        return err_response(err_str, 403)

    if not connexion.request.is_json:
        _, rc, msg = "fail", 422, "invalid request"
        return err_response(msg, rc)

    bbox = BBox.from_dict(connexion.request.get_json())  # noqa: E501
    bbox.split = "new"
    if not bbox_utils.ensure_bbox(bbox["bbox"]):
        return make_response(jsonify({"status": "fail", "reason": "invalid bbox"}), 400)

    bbox.file_idx = doc_id
    try:
        newly_created_bbox_id = nosql_db.save_bbox(bbox)
    except Exception as e:
        logger.error(
            f"Error when saving bbox {e}",
            exc_info=True,
        )
        status, rc, msg = "fail", 500, "unknown status saving bbox"
        return make_response(jsonify({"status": status, "reason": msg}), rc)

    if newly_created_bbox_id:  # TODO: test case needed
        newly_created_bbox_id = str(newly_created_bbox_id)
        return make_response(
            jsonify(
                IdWithMessage(
                    newly_created_bbox_id,
                    f"new bbox with id {newly_created_bbox_id} created",
                ),
            ),
            200,
        )
    else:
        logger.error(
            f"unknown error saving bbox, got 'null' as id {newly_created_bbox_id}",
        )
        status, rc, msg = "fail", 500, "unknown status saving bbox"
        return make_response(jsonify({"status": status, "reason": msg}), rc)


def get_bbox_by_doc_id(
    user,
    token_info,
    doc_id,
    audited_only=False,
):  # noqa: E501
    document = nosql_db.get_document_info_by_id(doc_id)
    user_permission, _ws = nosql_db.get_user_permission(
        document.workspace_id,
        email=user,
        user_json=token_info.get("user_obj", None) if token_info else None,
    )
    if user_permission not in ["admin", "owner", "editor"]:
        err_str = "Not authorized"
        log_str = (
            f"user {user} not authorized to retrieve bounding box info for {doc_id} "
            f"from workspace {document.workspace_id}",
        )
        logger.info(log_str)
        return err_response(err_str, 403)

    bboxes = nosql_db.get_bbox_by_doc_id(doc_id, audited_only)
    return make_response(jsonify(bboxes), 200)


def delete_bbox(
    user,
    token_info,
    doc_id,
    page_id,
):  # noqa: E501
    document = nosql_db.get_document_info_by_id(doc_id)
    user_permission, _ws = nosql_db.get_user_permission(
        document.workspace_id,
        email=user,
        user_json=token_info.get("user_obj", None) if token_info else None,
    )
    if user_permission not in ["admin", "owner", "editor"]:
        err_str = "Not authorized to delete bounding box"
        log_str = (
            f"user {user} not authorized to delete bounding box info for {doc_id} "
            f"from workspace {document.workspace_id}",
        )
        logger.info(log_str)
        return err_response(err_str, 403)

    count = nosql_db.remove_bbox(doc_id, page_id)
    return make_response(jsonify(count), 200)
