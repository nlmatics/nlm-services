from flask import jsonify
from flask import make_response

from server import auth
from server.utils import metric_utils


def update_global_params(token_info):  # noqa: E501
    """Updates the global parameters maintained in Cache

     # noqa: E501


    :rtype: None
    """
    user_json = token_info["user_obj"]
    if user_json["is_admin"]:
        # Update the global params
        auth.update_global_params()
        metric_utils.update_global_params()

        return make_response(
            "Successfully updated global parameters",
            200,
        )
    else:
        return make_response(
            jsonify(
                {
                    "status": "fail",
                    "reason": "Only Admins can change global parameters",
                },
            ),
            403,
        )
