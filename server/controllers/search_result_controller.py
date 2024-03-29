import logging
import traceback

import connexion
from flask import jsonify
from flask import make_response

from server import err_response
from server.models import SavedSearchResult  # noqa: E501
from server.storage import nosql_db
from server.utils import str_utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def delete_saved_search_result(user, doc_id, unique_id):
    doc = nosql_db.get_document_info_by_id(doc_id)
    if doc:
        user_permission, _ws = nosql_db.get_user_permission(
            doc.workspace_id,
            email=user,
        )
        if user_permission not in ["admin", "owner", "editor"]:
            err_str = "Not authorized to delete saved searches"
            log_str = f"{user} not authorized to delete saved searches in {doc_id}"
            logger.info(log_str)
            return err_response(err_str, 403)
    else:
        msg = f"document {doc_id} does not exist"
        logger.error(msg)
        return make_response(jsonify({"status": "fail", "reason": str(msg)}), 500)
    try:
        nosql_db.delete_saved_search_result(doc_id, unique_id)
        status, rc, msg = "success", 200, "search removed"
        return make_response(jsonify({"status": status, "reason": msg}), rc)

    except Exception as e:
        logger.error(
            f"Error retrieving saved searches for document with id {doc_id}, err: {str(e)}",
        )
        return make_response(jsonify({"status": "fail", "reason": str(e)}), 500)


def get_saved_searches_by_action(user, doc_id, action):
    doc = nosql_db.get_document_info_by_id(doc_id)
    if doc:
        user_permission, _ws = nosql_db.get_user_permission(
            doc.workspace_id,
            email=user,
        )
        if user_permission not in ["admin", "owner", "editor", "viewer"]:
            err_str = "Not authorized to view saved searches"
            log_str = f"{user} not authorized to view saved searches in {doc_id}"
            logger.info(log_str)
            return err_response(err_str, 403)
    else:
        msg = f"document {doc_id} does not exist"
        logger.error(msg)
        return make_response(jsonify({"status": "fail", "reason": str(msg)}), 500)
    try:
        return make_response(
            jsonify(nosql_db.get_saved_searches_by_action(doc_id, action)),
            200,
        )
    except Exception as e:
        logger.error(
            f"Error retrieving saved searches for document with id {doc_id} and action {action}, err: {str(e)}",
        )
        return make_response(jsonify({"status": "fail", "reason": str(e)}), 500)


def save_search_result(user, body=None):  # noqa: E501
    """Stores Search Result in DB

     # noqa: E501

    :param body:
    :type body: dict | bytes

    :rtype: Object
    """
    if connexion.request.is_json:
        user_obj = nosql_db.get_user_by_email(user)
        status, rc, msg = "success", 200, "search saved"
        if user_obj:
            try:
                saved_search_result = SavedSearchResult.from_dict(
                    connexion.request.get_json(),
                )  # noqa: E501
                user_permission, _ws = nosql_db.get_user_permission(
                    saved_search_result.workspace_id,
                    email=user,
                    user_json=user_obj.to_dict(),
                )
                if user_permission not in ["admin", "owner", "editor"]:
                    err_str = "Not authorized to save searches"
                    log_str = f"{user} not authorized to save searches in {saved_search_result.workspace_id}"
                    logger.info(log_str)
                    return err_response(err_str, 403)
                saved_search_result.user_id = user_obj.id
                saved_search_result.user_name = (
                    user_obj.first_name + " " + user_obj.last_name
                )
                saved_search_result.unique_id = (
                    saved_search_result.search_result["uniq_id"]
                    if saved_search_result.search_result
                    and "uniq_id" in saved_search_result.search_result
                    else ""
                )
                saved_search_result.created_on = str_utils.timestamp_as_str()

                nosql_db.save_search_result(saved_search_result)
                return make_response(
                    jsonify(saved_search_result),
                    200,
                )
            except Exception as e:
                logger.error(
                    f"error saving field, err: {traceback.format_exc()}",
                )
                status, rc, msg = "fail", 500, str(e)
            finally:
                pass
            return make_response(jsonify({"status": status, "reason": msg}), rc)
