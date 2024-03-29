import json
import logging
import os
import re
import tempfile

import magic
from bson import ObjectId
from flask import jsonify
from flask import make_response
from flask import send_from_directory
from nlm_utils.storage import file_storage
from openpyxl import load_workbook
from werkzeug.utils import secure_filename

import server.config as cfg
from server import err_response
from server.controllers.extraction_controller import apply_template
from server.models.id_with_message import IdWithMessage  # noqa: E501
from server.models.template_to_file import TemplateToFile
from server.storage import nosql_db as nosqldb
from server.utils import file_utils
from server.utils import str_utils


logger = logging.getLogger(__name__)
logger.setLevel(cfg.log_level())


def generate_document(
    user,
    token_info,
    field_bundle_id,
    document_id,
    extraction_template_id,
    nosql_db=nosqldb,
    file_storage=file_storage,
):

    document = nosql_db.get_document_info_by_id(document_id)
    user_permission, _ws = nosql_db.get_user_permission(
        document.workspace_id,
        email=user,
        user_json=token_info.get("user_obj", None) if token_info else None,
    )
    if user_permission not in ["admin", "owner", "editor", "viewer"]:
        err_str = "Not authorized to access this workspace"
        log_str = f"user {user} not authorized to generate document in workspace {document.workspace_id} "
        logger.info(log_str)
        return err_response(err_str, 403)

    extracted_topics = {}

    data = apply_template(file_idx=document_id, field_bundle_idx=field_bundle_id)

    for topic in data:
        extracted_topics[topic["topic"]] = topic["topic_facts"][0]["formatted_answer"]
    # print(extracted_topics)
    doc_template = nosql_db.get_template_by_id(extraction_template_id)
    dest_file_location_handler, dest_file_location = tempfile.mkstemp(suffix=".xlsx")
    os.close(dest_file_location_handler)

    if doc_template:
        doc_loc_to_serve = doc_template["doc_location"]
        doc_mimetype = doc_template["mime_type"]
        doc_name = doc_template["name"]
        file_storage.download_from_location(doc_loc_to_serve, dest_file_location)
    else:
        raise RuntimeError("Template file does not exist")

    file_storage.download_from_location(doc_loc_to_serve, dest_file_location)
    wb = load_workbook(filename=dest_file_location)
    ws = wb.active
    for row in ws.rows:
        for cell in row:
            text = str(cell.value)
            if re.search(r".*\{\{.*\}\}", text):
                j, k = text.find("{{"), text.find("}}")
                stripped = text.split("}}")[0].split("{{")[1]
                if stripped in extracted_topics and extracted_topics[stripped]:
                    text = text[:j] + extracted_topics[stripped] + text[k + 2 :]
                    cell.value = text
                else:
                    cell.value = ""
    wb.save(dest_file_location)

    return send_from_directory(
        os.path.dirname(dest_file_location),
        os.path.basename(dest_file_location),
        mimetype=doc_mimetype,
        as_attachment=True,
        download_name=doc_name,
    )


def upload_template(
    user,
    token_info,
    workspace_id,
    file=None,
    nosql_db=nosqldb,
    file_storage=file_storage,
):
    user_permission, _ws = nosql_db.get_user_permission(
        workspace_id,
        email=user,
        user_json=token_info.get("user_obj", None) if token_info else None,
    )
    if user_permission not in ["admin", "owner", "editor"]:
        err_str = "Not authorized to access this workspace"
        log_str = (
            f"user {user} not authorized to upload template in workspace {workspace_id}"
        )
        logger.info(log_str)
        return err_response(err_str, 403)

    # make an entry into db
    filename = secure_filename(file.filename)
    tempfile_handler, tmp_file = tempfile.mkstemp(filename)
    os.close(tempfile_handler)
    file.save(tmp_file)
    props = _extract_file_properties(tmp_file)
    doc_id = str_utils.generate_unique_document_id(
        file.filename,
        props["checksum"],
        props["fileSize"],
    )
    doc_loc = _upload_file_to_store(
        file_storage,
        tmp_file,
        workspace_id,
        doc_id,
        props["mimeType"],
    )

    doc_details = {
        **props,
        "id": doc_id,
        "workspaceId": workspace_id,
        "docLocation": doc_loc,
        "name": file.filename,
    }
    doc = TemplateToFile.from_dict(doc_details)
    logger.info(f"uploading template: {filename}")

    if tmp_file and os.path.exists(tmp_file):
        os.unlink(tmp_file)

    if nosql_db.create_excel_template(doc):
        logger.info("document successfully uploaded")
        return make_response(jsonify(IdWithMessage(doc_id, "upload successful")))
    else:
        logger.error("upload status unknown")
        status, rc, msg = "fail", 500, "unknown upload status"
        return make_response(jsonify({"status": status, "reason": msg}), rc)


def get_workspace_templates(
    user,
    token_info,
    workspace_id,
    nosql_db=nosqldb,
):
    user_permission, _ws = nosql_db.get_user_permission(
        workspace_id,
        email=user,
        user_json=token_info.get("user_obj", None) if token_info else None,
    )
    if user_permission not in ["admin", "owner", "editor", "viewer"]:
        err_str = "Not authorized to access this workspace"
        log_str = f"user {user} not authorized to retrieve templates in workspace {workspace_id}"
        logger.info(log_str)
        return err_response(err_str, 403)

    resp = JSONEncoder().encode(nosqldb.get_template_in_workspace(workspace_id))
    return make_response(resp, 200)


def _extract_file_properties(filepath):
    mime_type = magic.from_file(filepath, mime=True)
    file_size = os.path.getsize(filepath)
    checksum = file_utils.get_file_sha256(filepath)
    creation_date = str_utils.timestamp_as_str()
    return {
        "fileSize": file_size,
        "mimeType": mime_type,
        "checksum": checksum,
        "createdOn": creation_date,
        "isDeleted": False,
    }


def _upload_file_to_store(
    store,
    tmp_file,
    workspace_id,
    doc_id,
    mime_type,
):
    dest_blob_name = f"templates/{workspace_id}/{doc_id}"
    return store.upload_blob(tmp_file, dest_blob_name, mime_type)


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        return json.JSONEncoder.default(self, o)
