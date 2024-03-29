import logging
import traceback

import connexion
from flask import jsonify
from flask import make_response

from server import unauthorized_response
from server.models import IdWithMessage
from server.models import Prompt
from server.storage import nosql_db

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def add_to_prompt_library(
    user,
    token_info,
    body=None,
):  # noqa: E501
    """Creates an entry in the prompt library.

     # noqa: E501

    :param user:
    :param token_info:
    :param body:
    :type body: dict | bytes

    :rtype: IdWithMessage
    """
    if connexion.request.is_json:
        prompt = Prompt.from_dict(
            connexion.request.get_json(),
        )  # noqa: E501
        user_obj = token_info.get("user_obj", None)
        if not user_obj:
            return unauthorized_response()
        prompt.user_id = user_obj["id"]

        ret_id = nosql_db.add_to_prompt_library(prompt)
        return make_response(
            jsonify(
                IdWithMessage(
                    ret_id,
                    "Successfully added to the prompt library",
                ),
            ),
            200,
        )
    else:
        status, rc, msg = "fail", 422, "invalid json"
    return make_response(jsonify({"status": status, "reason": msg}), rc)


def get_prompts_for_workspace(
    user,
    token_info,
    workspace_id,
    query_scope: str = None,
    prompt_type: str = None,
    doc_id: str = None,
):
    """
    Return all the prompts for a given workspace.
    """
    user_obj = token_info.get("user_obj", None)
    if not user_obj:
        return unauthorized_response()
    # set the initial values.
    prompt_type = prompt_type or "public"
    doc_id = doc_id or ""
    user_id = None
    if prompt_type == "private":
        user_id = user_obj["id"]

    prompts = nosql_db.get_prompts(
        workspace_id,
        doc_id,
        prompt_type,
        query_scope=query_scope,
        user_id=user_id,
    )

    try:
        return make_response(jsonify(prompts), 200)
    except Exception as e:
        logger.error(f"error retrieving prompts, err: {traceback.format_exc()}")
        return make_response(jsonify({"status": "fail", "reason": str(e)}), 500)
