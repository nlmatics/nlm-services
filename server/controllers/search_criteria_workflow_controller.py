import logging
import traceback

import connexion
from flask import jsonify
from flask import make_response

from server import unauthorized_response
from server.models import IdWithMessage
from server.models import SearchCriteriaWorkflow
from server.storage import nosql_db

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def create_search_criteria_workflow(
    user,
    token_info,
    body=None,
):  # noqa: E501
    """Creates an entry in the search criteria workflow.

     # noqa: E501

    :param user:
    :param token_info:
    :param body:
    :type body: dict | bytes

    :rtype: IdWithMessage
    """
    if connexion.request.is_json:
        search_criteria_workflow = SearchCriteriaWorkflow.from_dict(
            connexion.request.get_json(),
        )  # noqa: E501
        user_obj = token_info.get("user_obj", None)
        if not user_obj:
            return unauthorized_response()
        search_criteria_workflow.user_id = user_obj["id"]

        ret_id = nosql_db.add_to_search_criteria_workflow(search_criteria_workflow)
        return make_response(
            jsonify(
                IdWithMessage(
                    ret_id,
                    "Successfully saved search criteria workflow",
                ),
            ),
            200,
        )
    else:
        status, rc, msg = "fail", 422, "invalid json"
    return make_response(jsonify({"status": status, "reason": msg}), rc)


def get_search_criteria_workflows_for_user(
    user,
    token_info,
    workspace_id: str = None,
):
    """
    Return all the Search Criteria Workflows for a user and a given workspace (when provided).
    """
    user_obj = token_info.get("user_obj", None)
    if not user_obj:
        return unauthorized_response()

    user_id = user_obj["id"]
    sc_workflows = nosql_db.get_search_criteria_workflows(
        user_id=user_id,
        workspace_id=workspace_id,
    )

    try:
        return make_response(jsonify(sc_workflows), 200)
    except Exception as e:
        logger.error(
            f"error retrieving search criteria workflows, err: {traceback.format_exc()}",
        )
        return make_response(jsonify({"status": "fail", "reason": str(e)}), 500)
