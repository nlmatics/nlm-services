import logging
import traceback

import connexion
from flask import jsonify
from flask import make_response

from server import err_response
from server.models.id_with_message import IdWithMessage  # noqa: E501
from server.models.template import Template  # noqa: E501
from server.storage import nosql_db as nosqldb
from server.utils import str_utils

# import server.config as cfg

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def create_new_template_for_field_with_given_id(
    user, body=None, nosql_db=nosqldb,
):  # noqa: E501
    """Create a new template for a given field id

     # noqa: E501

    :param body:
    :type body: dict | bytes

    :rtype: IdWithMessage
    """
    try:
        if connexion.request.is_json:
            template = Template.from_dict(connexion.request.get_json())  # noqa: E501
            if template.field_id is None:
                raise Exception("field_id must be specified, cannot create template")
            if template.id is None:
                template.id = str_utils.generate_template_id(
                    template.field_id, template.text,
                )
            id = nosql_db.create_template(template)
            if id:
                return make_response(
                    jsonify(IdWithMessage(id, "template created")), 200,
                )
            else:
                status, rc, msg = "fail", 500, "unable to create template"
        else:
            status, rc, msg = "fail", 422, "invalid json request"
    except Exception:
        logger.error(f"error creating template, err: {traceback.format_exc()}")
        status, rc, msg = "fail", 422, "field is not specified"
    return make_response(jsonify({"status": status, "reason": msg}), rc)


def delete_template_by_id(user, template_id, field_id, nosql_db=nosqldb):  # noqa: E501
    """Delete the template with given id

     # noqa: E501

    :param template_id:
    :type template_id: str
    :param field_id:
    :type field_id: str

    :rtype: IdWithMessage
    """

    try:
        if nosql_db.is_template_exists_in_field(template_id, field_id):
            nosql_db.delete_template(template_id, field_id)
            return make_response(IdWithMessage(template_id, "template deleted"))
        else:
            logger.error(
                f"template id {template_id} does not belong to field {field_id}",
            )
            return err_response("template not found", 400)
    except Exception as e:
        logger.error(
            f"error deleting template id {template_id}, err: {traceback.format_exc()}",
        )
        return err_response(str(e))


def update_template_by_id(
    user, template_id, field_id, body=None, nosql_db=nosqldb,
):  # noqa: E501
    """update template with given id

     # noqa: E501

    :param template_id:
    :type template_id: str
    :param field_id:
    :type field_id: str
    :param body:
    :type body: dict | bytes

    :rtype: IdWithMessage
    """
    if connexion.request.is_json:
        template = Template.from_dict(connexion.request.get_json())  # noqa: E501
        if nosql_db.is_template_exists_in_field(template_id, field_id):
            updated_template_id = nosql_db.update_template(template_id, template)
            if updated_template_id:
                logger.info(f"template id {template_id} updated")
                return make_response(
                    jsonify(IdWithMessage(template_id, "template updated")), 200,
                )
            else:
                logger.error(
                    f"unknown status updating template id {template_id} (id returned is null)",
                )
                return err_response(
                    "unknown status updating template id (id returned is null)",
                )
        else:
            logger.error(
                f"template id {template_id} does not  belong to field {field_id}",
            )
            return err_response("template not found", 400)
    else:
        logger.error("cannot update template, invalid json request")
        return err_response("invalid json request", 400)
