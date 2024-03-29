import logging

from flask import jsonify
from flask import make_response

from server.models.prefered_workspace import PreferedWorkspace  # noqa: E501
from server.storage import nosql_db

# import server.config as cfg

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_prefered_workspace_by_user_id(user_id):
    pref_workspace = nosql_db.get_prefered_workspace(user_id, return_preferred_ws=True)
    if pref_workspace:
        logger.info(f"Opened preferred workspace {pref_workspace} for {user_id}")
    else:
        pref_workspace = nosql_db.get_default_workspace_for_user_id(user_id)
        workspace_id = pref_workspace.id
        preferred_workspace = PreferedWorkspace(
            default_workspace_id=workspace_id,
            user_id=user_id,
        )
        nosql_db.create_prefered_workspace(preferred_workspace)
        logger.info(
            f"Created prefered workspace for {user_id} and set it to {workspace_id}",
        )
    return make_response(jsonify(pref_workspace), 200)
