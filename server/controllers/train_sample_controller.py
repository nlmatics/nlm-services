import logging
import traceback

import connexion
from flask import jsonify
from flask import make_response

from server import err_response
from server.models.id_with_message import IdWithMessage  # noqa: E501
from server.models.train_sample import TrainSample  # noqa: E501
from server.storage import nosql_db
from server.utils import str_utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def add_sample_for_model_training(
    user,
    token_info,
    body=None,
):  # noqa: E501
    """Creates a new training sample.

     # noqa: E501

    :param body:
    :type body: dict | bytes

    :rtype: IdWithMessage
    """
    try:
        user_json = token_info.get("user_obj", None)
        if not user_json.get("is_admin", False):
            err_str = "Not authorized to add sample for training"
            log_str = f"user {user} not authorized to add sample for training"
            logging.info(log_str)
            return err_response(err_str, 403)

        if connexion.request.is_json:
            sample = TrainSample.from_dict(connexion.request.get_json())
            # Add default params
            if sample.created_on is None:
                sample.created_on = str_utils.timestamp_as_str()
            sample.id = str_utils.generate_training_sample_id(
                sample.doc_id,
                sample.workspace_id,
            )
            sample.train_state = "READY"
            logger.debug(f"Creating training sample: {sample}")
            ret_id = nosql_db.create_training_sample(sample)
            if ret_id:
                return make_response(
                    jsonify(IdWithMessage(ret_id, "training sample created")),
                    200,
                )
            else:
                status, rc, msg = "fail", 500, "Internal Server Error"
        else:
            status, rc, msg = "fail", 422, "invalid json"
    except Exception:
        logger.error(f"error creating training sample, err: {traceback.format_exc()}")
        status, rc, msg = "fail", 422, "unable to create training sample"
    return make_response(jsonify({"status": status, "reason": msg}), rc)
