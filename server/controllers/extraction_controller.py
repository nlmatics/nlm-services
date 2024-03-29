import datetime
import json
import logging
import os
import tempfile
import traceback
from timeit import default_timer
from typing import List

import connexion
import pandas as pd
import requests
from flask import jsonify
from flask import make_response
from flask import send_from_directory
from nlm_utils.rabbitmq import producer
from nlm_utils.utils import ensure_bool

from server import err_response
from server import unauthorized_response
from server.models import GridSelector
from server.models.search_criteria import SearchCriteria  # noqa: E501
from server.storage import nosql_db
from server.utils import extraction_utils
from server.utils import str_utils
from server.utils.metric_utils import update_metric_data

# import server.config as cfg

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
UPDATE_USAGE_METRICS = ensure_bool(os.getenv("UPDATE_USAGE_METRICS", False))


def apply_template(**kwargs):

    start = default_timer()
    # make sure post_processors and aggregate_post_processors are list
    for key in ["post_processors", "aggregate_post_processors"]:
        if key in kwargs and not isinstance(kwargs[key], list):
            kwargs[key] = [kwargs[key]]
    req_data = json.dumps(kwargs)
    url = f'{os.getenv("DE_LITE_URL")}/apply_template'

    resp = requests.post(
        url,
        headers={"content-type": "application/json"},
        data=req_data,
    )
    if resp.ok:
        data = resp.json()
        # data = dict(resp.json())
        logger.info(f"de-lite services finished in {default_timer() - start:.4f}s")
        return data
    else:
        raise RuntimeError(f"Exception: {resp}")


if "DE_LITE_URL" not in os.environ:
    logger.info("DE_LITE_URL is unset, import local discovery_engine_lite")
    try:
        from server.extraction_engine import init_de_lite

        discovery_engine = init_de_lite()

        def apply_template_local(**kwargs):
            facts = discovery_engine.apply_template(**kwargs)
            return facts

        apply_template = apply_template_local  # noqa :F811

    except Exception as e:
        logger.error(
            "Cannot import discovery_engine_lite locally, please install it first, or use restful services",
        )
        if not os.getenv("NO_DE_LITE"):
            raise e


def extract_field_bundle_from_document(
    doc_id: str,
    field_bundle_id: str,
    overwrite_cache: str = None,
    field_ids: list = None,
    user=None,
    token_info=None,
):  # noqa: E501
    try:
        bundle_info = nosql_db.get_field_bundle_info(field_bundle_id)
        user_permission, _ws = nosql_db.get_user_permission(
            bundle_info.workspace_id,
            email=user,
            user_json=token_info.get("user_obj", None) if token_info else None,
        )
        if user_permission not in ["admin", "owner", "editor", "viewer"]:
            err_str = "Not authorized to extract field bundle"
            log_str = (
                f"user {user} not authorized to extract field bundle documents in "
                f"workspace {bundle_info.workspace_id}"
            )
            logger.info(log_str)
            return err_response(err_str, 403)

        walltime = default_timer()
        field_idx2field = {}
        if not field_ids:
            fields = nosql_db.get_fields_in_bundle(field_bundle_id)
            for field in fields:
                field_idx2field[field.id] = field
            # Reset the order
            fields = [field_idx2field[i] for i in bundle_info.field_ids]
        else:
            fields = nosql_db.get_field_by_ids(field_ids)

        condition = {
            "file_idx": doc_id,
            "field_bundle_idx": field_bundle_id,
        }
        if field_ids:
            condition["field_idx"] = {"$in": field_ids}
        else:
            condition["field_idx"] = {"$in": list(field_idx2field.keys())}

        facts_count = nosql_db.read_extracted_field(
            condition,
            count_only=True,
        )
        # re-run extraction if overwrite_cache is set, or no facts for current fields
        if (
            overwrite_cache or (fields and facts_count == 0)
        ) and user_permission != "viewer":
            logger.info("generating topic facts")
            apply_template(
                file_idx=doc_id,
                field_bundle_idx=field_bundle_id,
                override_topic="ALL",
            )

        document = nosql_db.get_document_info_by_id(doc_id)
        output = {}
        for field in fields:
            output[field.id] = {
                "answers": 0,
                # "criteria_uid": "d1289f32",
                "dataType": field.data_type,
                "options": field.options,
                "isEnteredField": field.is_entered_field,
                "criterias": field.search_criteria.criterias,
                "file_idx": document.id,
                "file_name": document.name,
                "post_processors": field.search_criteria.post_processors,
                "topic": field.name,
                "topicId": field.id,
                "topic_facts": [],
            }

        for fact in nosql_db.read_extracted_field(
            condition,
        ):
            if "topic_facts" not in fact:
                fact["topic_facts"] = []
            # insert top_fact if is override
            top_fact_type = fact["top_fact"].get("type", None)
            if top_fact_type == "override":
                # find the override answer
                matched = -1
                for idx, extract_fact in enumerate(fact.get("topic_facts", [])):
                    if extract_fact["match_idx"] == fact["top_fact"]["match_idx"]:
                        matched = idx
                # pop it from top_fact
                if matched >= 0:
                    fact["topic_facts"].pop(matched)
            if top_fact_type:
                fact["topic_facts"].insert(0, fact["top_fact"])

            output[fact["field_idx"]].update(
                {
                    "answers": len(fact["topic_facts"]),
                    "topic_facts": fact["topic_facts"],
                },
            )
        output = list(output.values())
        walltime = (default_timer() - walltime) * 1000
        logger.info(f"loading workspace field_bundle takes {walltime:.2f}ms")
        return make_response(jsonify(output), 200)
    except Exception as e:
        logger.error(
            f"error extracting values for field bundle {field_bundle_id} from document {doc_id}, error: {traceback.format_exc()}",
        )
        return make_response(jsonify({"status": "fail", "reason": str(e)}))


def extract_relation_field(
    field=None,
    user=None,
    token_info=None,
):
    user_json = token_info.get("user_obj", None) if token_info else None
    user_permission, _ws = nosql_db.get_user_permission(
        field.workspace_id,
        email=user,
        user_json=user_json,
    )
    if user_permission not in ["admin", "owner", "editor", "viewer"]:
        err_str = "Not authorized to extract field bundle"
        log_str = (
            f"user {user} not authorized to extract field bundle documents in "
            f"workspace {field.workspace_id}"
        )
        logger.info(log_str)
        return err_response(err_str, 403)

    # create field values with empty topic facts for, then spawn tasks to populate with results
    extracted_answer_in_db = [
        {
            "file_idx": "all_files",
            "workspace_idx": field.workspace_id,
            "field_idx": field.id,
            "field_bundle_idx": None,
            "topic_facts": [],
            "batch_idx": "0",
        },
    ]
    nosql_db.create_extracted_field(extracted_answer_in_db)

    logger.info(
        f"created field value, spawning tasks to populate topic_facts (results) for field: {field.id}",
    )
    task_body = {
        "workspace_idx": field.workspace_id,
        "field_bundle_idx": None,
        "override_topic": field.id,
        "doc_per_page": field.search_criteria.doc_per_page,
        "offset": 0,
        "group_by_file": field.search_criteria.group_by_file,
        "search_type": field.search_criteria.search_type,
        "file_idx": None,
        "batch_idx": "0",
    }

    output = apply_template(**task_body)
    if "pagination" in output:
        total = output["pagination"][0]["workspace"]["total"]
    else:
        total = field.search_criteria.doc_per_page

    n_local_updates = 0
    n_pages = round(total / field.search_criteria.doc_per_page)
    logger.info(f"scheduling tasks for {total} items and {n_pages} pages")

    field.status = {
        "total": total,  # first page we got directly
        "done": field.search_criteria.doc_per_page if n_pages > 1 else total,
        "progress": "queued" if n_pages > 1 else "done",
    }
    nosql_db.update_field_by_id(field.id, field)
    prev_batch_idx = task_body["batch_idx"]
    for page_idx in range(1, n_pages):
        offset = page_idx * field.search_criteria.doc_per_page
        task_body["offset"] = offset
        task_body["batch_idx"] = str(offset // 1000)  # In groups of 1000
        if task_body["batch_idx"] != "0" and prev_batch_idx != task_body["batch_idx"]:
            extracted_answer_in_db = [
                {
                    "file_idx": "all_files",
                    "workspace_idx": field.workspace_id,
                    "field_idx": field.id,
                    "field_bundle_idx": None,
                    "topic_facts": [],
                    "batch_idx": task_body["batch_idx"],
                },
            ]
            nosql_db.create_extracted_field(extracted_answer_in_db)

        prev_batch_idx = task_body["batch_idx"]
        task = nosql_db.insert_task(
            user_json["id"],
            "extraction",
            task_body,
        )
        logger.info(
            f"scheduling task for page {page_idx} with offset {offset} and batch_idx: {task_body['batch_idx']}",
        )
        res = producer.send(task)
        # fallback to direct call if producer return False
        if not res:
            nosql_db.update_field_extraction_status(
                field_idx=field.id,
                action="extracting",
            )
            n_local_updates += 1
            apply_template(**task_body)
            nosql_db.update_field_extraction_status(
                field_idx=field.id,
                action="batch_done",
                doc_per_page=task_body["doc_per_page"],
            )


def extract_field_bundle_from_workspace(
    workspace_id: str,
    field_bundle_id: str,
    overwrite_cache: str = None,
    field_ids: list = None,
    user=None,
    token_info=None,
    return_output=True,
):

    try:
        bundle_info = nosql_db.get_field_bundle_info(field_bundle_id)
        user_json = token_info.get("user_obj", None) if token_info else None
        user_permission, _ws = nosql_db.get_user_permission(
            bundle_info.workspace_id,
            email=user,
            user_json=user_json,
        )
        if user_permission not in ["admin", "owner", "editor", "viewer"]:
            err_str = "Not authorized to extract field bundle"
            log_str = (
                f"user {user} not authorized to extract field bundle documents in "
                f"workspace {bundle_info.workspace_id}"
            )
            logger.info(log_str)
            return err_response(err_str, 403)

        walltime = default_timer()
        field_idx2field = {}
        if not field_ids:
            fields = nosql_db.get_fields_in_bundle(field_bundle_id)
            for field in fields:
                field_idx2field[field.id] = field
            # Reset the order
            fields = []
            for i in bundle_info.field_ids:
                if i in field_idx2field:
                    fields.append(field_idx2field[i])
        else:
            fields = nosql_db.get_field_by_ids(field_ids)
            for field in fields:
                field_idx2field[field.id] = field

        condition = {
            "workspace_idx": workspace_id,
            "field_bundle_idx": field_bundle_id,
        }
        if field_ids:
            condition["field_idx"] = {"$in": field_ids}

        facts_count = nosql_db.read_extracted_field(
            condition,
            count_only=True,
        )

        # re-run extraction if overwrite_cache is set, or no facts for current fields
        if (fields and facts_count == 0) or overwrite_cache == "ALL":
            overwrite_cache = field_idx2field

        if isinstance(overwrite_cache, str):
            overwrite_cache = {
                x: field_idx2field[x] for x in overwrite_cache.split(",")
            }

        doc_projection = {
            "_id": 0,
            "id": 1,
            "name": 1,
        }
        docs_in_ws = nosql_db.get_folder_contents(
            workspace_id,
            projection=doc_projection,
            do_sort=False,
        )["documents"]
        if (
            overwrite_cache or (fields and facts_count == 0)
        ) and user_permission != "viewer":
            logger.info("generating topic facts")
            perform_extraction_on_fields(
                user_json,
                workspace_id,
                len(docs_in_ws),
                overwrite_cache,
            )
        # if nosql_db.create_field_bundle_grid(workspace_id=workspace_id,
        #                                      field_bundle_id=field_bundle_id):
        #     grid_output, _, _ = extraction_utils.get_field_bundle_grid_data(workspace_id=workspace_id,
        #                                                                     field_bundle_id=field_bundle_id,
        #                                                                     fields=fields,
        #                                                                     docs_in_ws=docs_in_ws,
        #                                                                     use_id_as_key=True)
        #     nosql_db.insert_field_bundle_grid_rows(workspace_id=workspace_id,
        #                                            field_bundle_id=field_bundle_id,
        #                                            rows=grid_output)
        # grid_output = nosql_db.get_field_bundle_grid_rows(workspace_id=workspace_id,
        #                                                   field_bundle_id=field_bundle_id)
        output = {}
        if return_output:
            grid_output, _, _ = extraction_utils.get_field_bundle_grid_data(
                workspace_id=workspace_id,
                field_bundle_id=field_bundle_id,
                fields=fields,
                docs_in_ws=docs_in_ws,
                use_id_as_key=True,
            )

            output = {
                "aggregate_post_processors": {field.name: {} for field in fields},
                "grid": [
                    # post_processor info
                    [
                        {
                            "post_processors": field.search_criteria.post_processors,
                            "topic": field.name,
                            "topicId": field.id,
                            "isEnteredField": field.is_entered_field,
                            "dataType": field.data_type,
                            "options": field.options,
                            "field": field,
                        }
                        for field in fields
                    ],
                    # output per file
                    grid_output,
                ],
                # "outputs": list(output.values()),
            }

        walltime = (default_timer() - walltime) * 1000
        logger.info(f"loading workspace field_bundle takes {walltime:.2f}ms")
        return make_response(jsonify(output), 200)
    except Exception as e:
        logger.error(
            f"error extracting values for field bundle {field_bundle_id} from workspace {workspace_id}, "
            f"error: {traceback.format_exc()}",
        )
        return make_response(jsonify({"status": "fail", "reason": str(e)}))


def extract_file_meta_dependent_workflow_fields(
    user,
    token_info,
    workspace_id: str,
    field_bundle_id: str,
    field_ids: list,
    doc_meta_param: str,
    check_permission=True,
):
    try:
        user_json = token_info.get("user_obj", None) if token_info else None
        if check_permission:
            bundle_info = nosql_db.get_field_bundle_info(field_bundle_id)

            user_permission, _ws = nosql_db.get_user_permission(
                bundle_info.workspace_id,
                email=user,
                user_json=user_json,
            )
            if user_permission not in ["admin", "owner", "editor", "viewer"]:
                err_str = (
                    "Not authorized to extract file meta dependent workflow fields"
                )
                log_str = (
                    f"user {user} not authorized to extract file meta dependent workflow fields {field_ids} "
                    f"in field_bundle {field_bundle_id} for workspace {bundle_info.workspace_id}"
                )
                logger.info(log_str)
                return err_response(err_str, 403)

        field_idx2field = {}
        fields = nosql_db.get_field_by_ids(field_ids)
        for field in fields:
            field_idx2field[field.id] = field

        doc_projection = {
            "_id": 0,
            "id": 1,
        }
        docs_in_ws = nosql_db.get_folder_contents(
            workspace_id,
            projection=doc_projection,
            do_sort=False,
        )["documents"]
        num_docs_in_ws = len(docs_in_ws)
        for field_idx, field in field_idx2field.items():
            if not field_bundle_id:
                field_bundle_id = field.parent_bundle_id
            field.status = {
                "total": num_docs_in_ws,
                "done": 0,
                "progress": "queued",
            }
            nosql_db.update_field_by_id(field_idx, field)

        name = f"{user_json['first_name']} {user_json['last_name']}"
        created_time = str_utils.timestamp_as_str()
        for field_id in field_idx2field:
            logger.info(
                f"Extracting File Meta {doc_meta_param} for workspace {workspace_id}, field_id {field_id}",
            )
            task_body = {
                "workspace_idx": workspace_id,
                "field_bundle_idx": field_bundle_id,
                "override_topic": field_id,
                "doc_per_page": num_docs_in_ws,
                "offset": 0,
                "is_dependent_field": True,
                "doc_meta_param": doc_meta_param,
                "name": name,
                "created_time": created_time,
            }
            task = nosql_db.insert_task(
                user_json["id"],
                "extraction",
                task_body,
            )
            res = producer.send(task)
            # fallback to direct call if producer return False
            if not res:
                nosql_db.update_field_extraction_status(
                    field_idx=field_id,
                    action="extracting",
                )

                nosql_db.create_workflow_fields_from_doc_meta(
                    workspace_id,
                    field_bundle_id,
                    field_id,
                    doc_meta_param,
                    name,
                    created_time,
                )

                nosql_db.update_field_extraction_status(
                    field_idx=field_id,
                    action="batch_done",
                    doc_per_page=num_docs_in_ws,
                )

        output = {}
        return make_response(jsonify(output), 200)
    except Exception as e:
        logger.error(
            f"Error {str(e)} extracting values for workflow fields {field_ids} from workspace {workspace_id}, "
            f"error: {traceback.format_exc()}",
        )
        return make_response(jsonify({"status": "fail", "reason": str(e)}))


def extract_fields_dependent_workflow_fields(
    user,
    token_info,
    workspace_id: str,
    field_bundle_id: str,
    field_id: str,
    field_options: dict = None,
    check_permission=True,
):

    try:
        user_json = token_info.get("user_obj", None) if token_info else None
        if check_permission:
            bundle_info = nosql_db.get_field_bundle_info(field_bundle_id)
            user_permission, _ws = nosql_db.get_user_permission(
                bundle_info.workspace_id,
                email=user,
                user_json=user_json,
            )
            if user_permission not in ["admin", "owner", "editor", "viewer"]:
                err_str = "Not authorized to extract dependent_fields for"
                log_str = (
                    f"user {user} not authorized to extract dependent_field in "
                    f"workspace {bundle_info.workspace_id}"
                )
                logger.info(log_str)
                return err_response(err_str, 403)

        doc_projection = {
            "_id": 0,
            "id": 1,
        }
        docs_in_ws = nosql_db.get_folder_contents(
            workspace_id,
            projection=doc_projection,
            do_sort=False,
        )["documents"]
        num_docs_in_ws = len(docs_in_ws)

        status_set_data = {
            "status": {
                "total": num_docs_in_ws,
                "done": 0,
                "progress": "queued",
            },
        }
        nosql_db.update_field_attr(status_set_data, field_id)

        name = f"{user_json['first_name']} {user_json['last_name']}"
        created_time = str_utils.timestamp_as_str()
        logger.info(
            f"Extracting dependent field {field_id} with options {field_options} for workspace {workspace_id}",
        )
        task_body = {
            "workspace_idx": workspace_id,
            "field_bundle_idx": field_bundle_id,
            "override_topic": field_id,
            "doc_per_page": num_docs_in_ws,
            "offset": 0,
            "is_dependent_field": True,
            "field_options": field_options,
            "name": name,
            "created_time": created_time,
        }
        task = nosql_db.insert_task(
            user_json["id"],
            "extraction",
            task_body,
        )
        res = producer.send(task)
        # fallback to direct call if producer return False
        if not res:
            nosql_db.update_field_extraction_status(
                field_idx=field_id,
                action="extracting",
            )

            nosql_db.create_fields_dependent_workflow_field_values(
                workspace_id,
                field_bundle_id,
                field_id,
                field_options,
                name,
                created_time,
            )

            nosql_db.update_field_extraction_status(
                field_idx=field_id,
                action="batch_done",
                doc_per_page=num_docs_in_ws,
            )

        output = {}
        return make_response(jsonify(output), 200)
    except Exception as e:
        logger.error(
            f"Error {str(e)} extracting values for dependent workflow "
            f"field {field_id} from workspace {workspace_id}, "
            f"error: {traceback.format_exc()}",
        )
        return make_response(jsonify({"status": "fail", "reason": str(e)}))


def extract_all_fields_all_workspaces(user, token_info):
    user_permission, _ws = nosql_db.get_user_permission(
        "",
        email=user,
        user_json=token_info.get("user_obj", None),
    )
    if user_permission not in ["admin"]:
        err_str = "Not authorized to extract field bundle"
        log_str = f"user {user} not authorized to extract field bundle documents in all workspaces"
        logger.info(log_str)
        return err_response(err_str, 403)

    workspace_list = nosql_db.get_all_workspaces()
    for ws in workspace_list:
        field_bundle_id_list = nosql_db.get_field_bundles_in_workspace(
            workspace_id=ws.id,
        )

        for field_bundle_id in field_bundle_id_list:
            try:
                _ = apply_template(
                    workspace_idx=ws.id,
                    template=field_bundle_id,
                    override_topic="ALL",
                )
            except Exception:
                pass


def _run_adhoc_extraction(
    ref_idx,
    is_workspace_search,
    search_criteria,
    user,
    token_info,
    search_history_ws_id=None,  # Workspace ID to store in search history
    file_filter_struct=None,
    acl_email_id=None,
):
    # Get user id
    user_json = token_info.get("user_obj", None) if token_info else None
    if not user_json:
        err_str = "Unauthorized"
        log_str = f"user {user} not authorized to perform Adhoc Extraction"
        logger.info(log_str)
        return err_response(err_str, 403)

    if acl_email_id:
        user_acl = nosql_db.get_user_acl(
            email_id=acl_email_id,
        )
    else:
        user_acl = nosql_db.get_user_acl(
            user_id=user_json["id"],
        )

    # Workspace or doc id?
    # Get attributes to save in database for update_search_history
    if is_workspace_search:
        workspace_id = ref_idx
        doc_id = None
    else:
        workspace_id = None
        doc_id = ref_idx

    # get user_id
    user_id = user_json["id"]

    # Get variables to save in database.
    timestamp = str_utils.timestamp_as_str()
    uniq_id = str_utils.generate_search_history_id(timestamp)

    # # Save in database
    nosql_db.update_search_history(
        uniq_id=uniq_id,
        user_id=user_id,
        doc_id=doc_id,
        workspace_id=search_history_ws_id,
        timestamp=datetime.datetime.now(),
        search_criteria=search_criteria,
    )

    # Send to discovery engine
    facts = apply_template(
        workspace_idx=workspace_id,
        file_idx=doc_id,
        ad_hoc=True,
        file_filter_struct=file_filter_struct,
        user_acl=user_acl.access_control_list if user_acl else None,
        **search_criteria.to_dict(),
    )
    logger.info(f"returning {len(facts)} matches")
    # Update the metrics
    if UPDATE_USAGE_METRICS:
        num_docs = 1
        # Comment out the usage metrics for workspace search.
        # We will consider workspace search as a single query.
        # # if is_workspace_search:
        # #     num_docs = nosql_db.get_num_docs_in_folder(workspace_id, "root")
        # Update the number of searches done.
        update_metric_data(user_json, [("num_search", num_docs)])
    return make_response(jsonify(facts), 200)


def run_adhoc_extraction_on_workspace_post(
    workspace_id,
    body,
    user=None,
    token_info=None,
    acl_email_id=None,
):
    user_permission, _ws = nosql_db.get_user_permission(
        workspace_id,
        email=user,
        user_json=token_info.get("user_obj", None) if token_info else None,
    )
    if user_permission not in ["admin", "owner", "editor", "viewer"]:
        err_str = "Not authorized to extract field bundle"
        log_str = f"user {user} not authorized to extract field bundle documents in workspace {workspace_id}"
        logger.info(log_str)
        return err_response(err_str, 403)

    search_criteria = SearchCriteria.from_dict(
        connexion.request.get_json(),
    )  # noqa: E501
    logger.info(
        f"running adhoc extraction on workspace {workspace_id} with search_criteria {search_criteria}",
    )

    file_filter_struct = {}
    if search_criteria.field_filter is not None:
        filter_end_row = _ws.settings["search_settings"].get("filter_end_row", 5000)
        start_row = 0
        end_row = filter_end_row
        grid_query = {
            "startRow": start_row,
            "endRow": end_row,
            "filterModel": search_criteria.field_filter.filter_model or {},
        }

        (
            limit,
            skip,
            sort_tuple_list,
            filter_dict,
            group_by_list,
            value_aggregate_list,
            review_status_filter_dict,
        ) = parse_grid_query(grid_query, None, workspace=_ws)

        logger.info(f"limit: {limit}")
        logger.info(f"skip: {skip}")
        logger.info(f"filter_dict: {filter_dict}")

        if filter_dict:
            file_filter_struct = nosql_db.retrieve_grid_data_from_field_values(
                workspace_id,
                search_criteria.field_filter.field_bundle_id,
                file_ids=search_criteria.doc_filters,
                limit=limit,
                skip=skip,
                sort_tuple_list=sort_tuple_list,
                filter_dict=filter_dict,
                group_by_list=group_by_list,
                value_aggregate_list=value_aggregate_list,
                return_only_file_ids=True,
            )
            if not file_filter_struct.get("results", []) or not file_filter_struct.get(
                "totalMatchCount",
                0,
            ):
                err_str = "No files matching the field filter"
                log_str = f"{err_str} {search_criteria.field_filter}"
                logger.info(log_str)
    elif search_criteria.doc_filters:
        file_filter_struct = {
            "totalMatchCount": len(search_criteria.doc_filters),
            "results": search_criteria.doc_filters,
        }

    return _run_adhoc_extraction(
        ref_idx=workspace_id,
        is_workspace_search=True,
        search_criteria=search_criteria,
        user=user,
        token_info=token_info,
        search_history_ws_id=workspace_id,
        file_filter_struct=file_filter_struct,
        acl_email_id=acl_email_id,
    )


def run_search_on_workspace_post(
    workspace_id,
    body,
    user=None,
    token_info=None,
):
    user_permission, _ws = nosql_db.get_user_permission(
        workspace_id,
        email=user,
        user_json=token_info.get("user_obj", None) if token_info else None,
    )
    if user_permission not in ["admin", "owner", "editor", "viewer"]:
        err_str = "Not authorized to extract field bundle"
        log_str = f"user {user} not authorized to extract field bundle documents in workspace {workspace_id}"
        logger.info(log_str)
        return err_response(err_str, 403)

    search_criteria = SearchCriteria.from_dict(
        connexion.request.get_json(),
    )  # noqa: E501
    logger.info(
        f"running search extraction on workspace {workspace_id} with search_criteria {search_criteria}",
    )
    return _run_adhoc_extraction(
        ref_idx=workspace_id,
        is_workspace_search=True,
        search_criteria=search_criteria,
        user=user,
        token_info=token_info,
        search_history_ws_id=workspace_id,
    )


def run_adhoc_extraction_on_document_post(
    doc_id,
    body,
    user=None,
    token_info=None,
    acl_email_id=None,
):
    workspace_id = nosql_db.get_document_info_by_id(doc_id).workspace_id
    user_permission, _ws = nosql_db.get_user_permission(
        workspace_id,
        email=user,
        user_json=token_info.get("user_obj", None) if token_info else None,
    )
    if user_permission not in ["admin", "owner", "editor", "viewer"]:
        err_str = "Not authorized to extract field bundle"
        log_str = f"user {user} not authorized to extract field bundle documents in workspace {workspace_id}"
        logger.info(log_str)
        return err_response(err_str, 403)

    search_criteria = SearchCriteria.from_dict(
        connexion.request.get_json(),
    )  # noqa: E501
    logger.info(
        f"running adhoc extraction on document {doc_id} with search_criteria {search_criteria}",
    )
    return _run_adhoc_extraction(
        ref_idx=doc_id,
        is_workspace_search=False,
        search_criteria=search_criteria,
        user=user,
        token_info=token_info,
        search_history_ws_id=workspace_id,
        acl_email_id=acl_email_id,
    )


def perform_extraction_on_fields(
    user_profile,
    workspace_id,
    num_docs_in_ws,
    field_dict: dict = None,
):
    """
    Perform extraction task with override_topic option.
    :param user_profile: Profile of the user trying to perform the extraction.
    :param workspace_id: Workspace ID
    :param num_docs_in_ws: Total number of documents in the workspace.
    :param field_dict: Dictionary with format field_id : field
    :return: NONE
    """

    if not num_docs_in_ws:
        logger.info(
            f"No documents in workspace {workspace_id} to perform re-extraction",
        )
        return

    field_dict = field_dict or {}
    field_bundle_id = 0
    # update field status
    for field_idx, field in field_dict.items():
        if not field_bundle_id:
            field_bundle_id = field.parent_bundle_id
        field.status = {
            "total": num_docs_in_ws,
            "done": 0,
            "progress": "queued",
        }
        nosql_db.update_field_by_id(field_idx, field)

    if field_bundle_id:
        batch_size = 20
        producer_send_failed = False
        for batch in range(
            0,
            num_docs_in_ws,
            batch_size,
        ):
            for field_id in field_dict:
                task_body = {
                    "workspace_idx": workspace_id,
                    "field_bundle_idx": field_bundle_id,
                    "override_topic": field_id,
                    "doc_per_page": batch_size,
                    "offset": batch,
                }
                task = nosql_db.insert_task(
                    user_profile["id"],
                    "extraction",
                    task_body,
                )
                res = producer.send(task)
                # fallback to direct call if producer return False
                if not res:
                    producer_send_failed = True
                    nosql_db.update_field_extraction_status(
                        field_idx=field_id,
                        action="extracting",
                    )

                    apply_template(**task_body)

                    nosql_db.update_field_extraction_status(
                        field_idx=field_id,
                        action="batch_done",
                        doc_per_page=task_body["doc_per_page"],
                    )
        if (
            producer_send_failed
            and field.options
            and field.options.get("child_fields", [])
        ):
            # Update the child field values.
            child_fields = field.options.get("child_fields", [])
            created_time = str_utils.timestamp_as_str()
            name = f"{user_profile['first_name']} {user_profile['last_name']}"
            for child_field_id in child_fields:
                child_field = nosql_db.get_field_by_id(child_field_id)
                if (
                    child_field
                    and child_field.is_entered_field
                    and child_field.is_dependent_field
                    and child_field.options
                ):
                    logger.info(
                        f"Updating dependent field {child_field_id} for workspace {workspace_id}",
                    )
                    nosql_db.create_fields_dependent_workflow_field_values(
                        child_field.workspace_id,
                        child_field.parent_bundle_id,
                        child_field_id,
                        child_field.options,
                        name,
                        created_time,
                    )


def convert_to_mongo_operator(filter_type: str):
    """
    Convert AG Grid filter type to mongo equivalent operator.
    :param filter_type: AG Grid filter type.
    :return: Mongo operator.
    """
    mongo_type = "NotImplemented"
    if filter_type == "equals":
        mongo_type = "$eq"
    elif filter_type == "notEqual":
        mongo_type = "$ne"
    elif filter_type == "lessThan":
        mongo_type = "$lt"
    elif filter_type == "lessThanOrEqual":
        mongo_type = "$lte"
    elif filter_type == "greaterThan":
        mongo_type = "$gt"
    elif filter_type == "greaterThanOrEqual":
        mongo_type = "$gte"
    elif filter_type == "inRange":
        mongo_type = "$inRange"
    elif filter_type == "contains":
        mongo_type = "NotImplemented"
    elif filter_type == "notContains":
        mongo_type = "NotImplemented"
    elif filter_type == "startsWith":
        mongo_type = "NotImplemented"
    elif filter_type == "endsWith":
        mongo_type = "NotImplemented"
    elif filter_type == "blank":
        mongo_type = "NotImplemented"
    elif filter_type == "notBlank":
        mongo_type = "NotImplemented"
    elif filter_type == "empty":
        mongo_type = "NotImplemented"
    return mongo_type


def convert_agg_func_to_mongo_type(agg_func: str):
    """
    Convert the AG Grid Aggregate functions to Mongo DB Type.
    :param agg_func: AG Grid specific Aggregate function
    :return: Mongo DB Aggregation Pipeline operators corresponding to the given AG Grid counterparts.
    """
    mongo_type = "NotImplemented"
    if agg_func in ["sum", "min", "max", "count", "avg", "first", "last"]:
        mongo_type = f"${agg_func}"
    return mongo_type


def parse_simple_filter_query(
    field: str,
    filter_query_struct: dict,
    allowed_operators: List[str],
):
    """
    Parse a simple filter query struct from AG Grid Server model
    :param field: Field we are interested in applying the filter
    :param filter_query_struct: Field Query Struct from AG Grid.
    :param allowed_operators: Allowed operators as part of the filter type
    :return: Parsed Filter Query
    """
    parsed_simple_query = {}
    if filter_query_struct:
        filter_type = filter_query_struct.get("type", "")
        if (
            filter_type
            and filter_type in allowed_operators
            and filter_query_struct.get("filter", None) is not None
        ):
            parsed_mongo_type = convert_to_mongo_operator(filter_type)
            if parsed_mongo_type != "NotImplemented":
                if parsed_mongo_type == "$inRange":
                    filter_to = filter_query_struct["filterTo"]
                    parsed_simple_query[f"{field}.answer_details.raw_value"] = {
                        "$gt": filter_query_struct["filter"],
                        "$lt": filter_to,
                    }
                else:
                    parsed_simple_query[f"{field}.answer_details.raw_value"] = {
                        parsed_mongo_type: filter_query_struct["filter"],
                    }
        elif filter_type and filter_type in allowed_operators:
            parsed_mongo_type = convert_to_mongo_operator(filter_type)
            if parsed_mongo_type == "NotImplemented":
                if filter_type in ["blank", "empty"]:
                    parsed_simple_query[f"{field}.answer_details.raw_value"] = {
                        "$in": ["", None],
                    }
                elif filter_type == "notBlank":
                    parsed_simple_query[f"{field}.answer_details.raw_value"] = {
                        "$nin": ["", None],
                    }

    return parsed_simple_query


def parse_num_grid_filter(field: str, filter_model: dict):
    """
    Parse Number type filter query
    :param field: Field we are interested in applying the filter
    :param filter_model: Field Model from AG Grid
    :return: Parsed Number type filter query in MongoDB format.
    """
    allowed_operators = [
        "equals",
        "notEqual",
        "lessThan",
        "lessThanOrEqual",
        "greaterThan",
        "greaterThanOrEqual",
        "inRange",
        "blank",
        "notBlank",
        "empty",
    ]
    parsed_filter_query = {}
    operator = filter_model.get("operator", "")
    if operator:
        cond1 = filter_model.get("condition1", {})
        cond2 = filter_model.get("condition2", {})
        if cond1 and cond2:
            cond1_query = parse_simple_filter_query(field, cond1, allowed_operators)
            cond2_query = parse_simple_filter_query(field, cond2, allowed_operators)
            if cond1_query and cond2_query:
                parsed_filter_query[f"${operator.lower()}"] = [
                    cond1_query,
                    cond2_query,
                ]
    else:
        parsed_filter_query = parse_simple_filter_query(
            field,
            filter_model,
            allowed_operators,
        )
    return parsed_filter_query


def parse_date_grid_filter(field: str, filter_model: dict):
    """
    Parse Date filter type query
    :param field: Field we are interested in applying the filter
    :param filter_model: Field Model from AG Grid
    :return: Parsed Date type filter query in MongoDB format.
    """
    # allowed_operators = [
    #     "equals",
    #     "notEqual",
    #     "lessThan",
    #     "greaterThan",
    #     "inRange",
    #     "blank",
    #     "notBlank",
    #     "empty",
    # ]
    parsed_filter_query = {}
    return parsed_filter_query


def parse_string_grid_filter(field: str, filter_model: dict):
    """
    Parse string type filter query
    :param field: Field we are interested in applying the filter
    :param filter_model: Field Model from AG Grid
    :return: Parsed String type filter query in MongoDB format.
    """
    allowed_operators = [
        "equals",
        "notEqual",
        "contains",
        "notContains",
        "startsWith",
        "endsWith",
        "blank",
        "notBlank",
        "empty",
    ]
    parsed_filter_query = {}
    operator = filter_model.get("operator", "")
    if operator:
        pass
    else:
        parsed_filter_query = parse_simple_filter_query(
            field,
            filter_model,
            allowed_operators,
        )
    return parsed_filter_query


def parse_set_grid_filter(field: str, filter_model: dict):
    """
    Parse set type filter query
    :param field: Field we are interested in applying the filter
    :param filter_model: Field Model from AG Grid
    :return: Parsed set type filter query in MongoDB format.
    """

    parsed_filter_query = {}
    values = filter_model.get("values", [])
    if values:
        parsed_filter_query[f"{field}.answer_details.raw_value"] = {
            "$in": values,
        }
    return parsed_filter_query


def parse_grid_query(grid_query, field_bundle_id, workspace=None):
    """
    Parse the grid query from AG-GRID
    :param grid_query: Grid Query part of the input JSON from FE.
    :param field_bundle_id: field bundle Id associated with the AG Grid query.
    :param workspace: workspace associated with the AG Grid query.
    :return:
    tuple of limit, skip, sort_dict, filter_dict, group_by_list, value_aggregate_list, review_status_filter_dict
    """
    limit = 25  # Default value for limit
    skip = 0  # Default value for skip
    sort_tuple_list = []
    filter_dict = {}
    group_by_list = []
    value_aggregate_list = []
    review_status_filter_dict = {}
    if grid_query:
        # Skip / Offset calculation
        skip = grid_query.get("startRow", None)
        # Limit calculation
        if skip is not None and grid_query.get("endRow", None) is not None:
            limit = grid_query.get("endRow", None) - skip
        # GroupBy List. We do have to maintain the order.
        row_grp_cols = grid_query.get("rowGroupCols", [])
        for grp in row_grp_cols:
            grp_field = grp.get("field", "")
            if grp_field:
                grp_type = grp.get("type", "")
                grp_num_bins = grp.get("numBins", 0)
                group_by_list.append((grp_field, grp_type, grp_num_bins))
        # Expand the row groups
        expand_cols = grid_query.get("groupKeys", [])
        for idx, expand_val in enumerate(expand_cols):
            (field, _, _) = group_by_list[idx]
            filter_dict[f"{field}.answer_details.raw_value"] = {
                convert_to_mongo_operator("equals"): nosql_db.escape_mongo_data(
                    expand_val,
                ),
            }
        if expand_cols and len(expand_cols) == len(
            group_by_list,
        ):  # We are converting the expansion as a filter_model
            group_by_list = []
        elif group_by_list:
            group_by_list = [group_by_list[len(expand_cols)]]
        # Sort Models --> sort_tuple_list
        sort_model_list = grid_query.get("sortModel", [])
        for sort_item in sort_model_list:
            sort_col = sort_item.get("colId", "")
            sort_order = sort_item.get("sort", "")
            if sort_col and sort_order:
                sort_tuple_list.append(
                    (
                        f"{sort_col}.answer_details.raw_value",
                        1 if sort_order == "asc" else -1,
                    ),
                )
        # Filter dictionary
        filter_model_dict = grid_query.get("filterModel", {})
        for field, filter_model in filter_model_dict.items():
            if workspace and field in workspace.settings["search_settings"].get(
                "discard_field_filters_in_db",
                [],
            ):
                continue
            filter_type = filter_model.get("filterType", "")
            add_to_filter_dict = True
            parsed_filter_dict = {}
            if filter_type == "number":
                parsed_filter_dict = parse_num_grid_filter(field, filter_model)
            elif filter_type == "date":
                parsed_filter_dict = parse_date_grid_filter(field, filter_model)
            elif filter_type == "string":
                parsed_filter_dict = parse_string_grid_filter(field, filter_model)
            elif filter_type == "set":
                parsed_filter_dict = parse_set_grid_filter(field, filter_model)
            else:
                add_to_filter_dict = False
            if add_to_filter_dict and parsed_filter_dict:
                for k, v in parsed_filter_dict.items():
                    # Add to the filter dict only if the key is not already present.
                    # Key might be already present if we are expanding the group on which a filter is also applied.
                    if k not in filter_dict:
                        filter_dict[k] = nosql_db.escape_mongo_data(v)
        # Value Aggregators
        value_cols = grid_query.get("valueCols", [])
        for value_item in value_cols:
            field = value_item.get("field", "")
            agg_func = convert_agg_func_to_mongo_type(value_item.get("aggFunc", ""))
            if field and agg_func != "NotImplemented":
                value_aggregate_list.append(
                    (
                        field,
                        {
                            agg_func
                            if agg_func != "$count"
                            else "$sum": f"${field}.answer_details.raw_value"
                            if agg_func != "$count"
                            else 1,
                        },
                    ),
                )
        review_filter = grid_query.get("reviewStatusFilter", {})
        if review_filter:
            review_status = review_filter.get("reviewStatus", "")
            review_id = review_filter.get("fieldId", "file_level")
            if review_status and review_id:
                if review_id != "file_level":
                    if review_status == "approved":
                        review_status_filter_dict["$or"] = [
                            {f"{review_id}.type": {"$eq": "approve"}},
                            {f"{review_id}.is_override": {"$eq": True}},
                        ]
                    else:
                        review_status_filter_dict["$and"] = [
                            {f"{review_id}.type": {"$ne": "approve"}},
                            {f"{review_id}.is_override": {"$eq": False}},
                        ]
                else:
                    # Get all the non-entered fields
                    projection = {
                        "_id": 0,
                        "id": 1,
                        "is_entered_field": 1,
                    }
                    fields = nosql_db.get_fields_in_bundle(
                        field_bundle_id,
                        projection=projection,
                        return_dict=True,
                    )
                    non_entered_fields = [
                        field["id"] for field in fields if not field["is_entered_field"]
                    ]
                    review_status_filter_dict = {}
                    for field in non_entered_fields:
                        field_dict = {}
                        if review_status == "approved":
                            field_dict["$or"] = [
                                {f"{field}.type": {"$eq": "approve"}},
                                {f"{field}.is_override": {"$eq": True}},
                            ]
                            if "$and" in review_status_filter_dict:
                                review_status_filter_dict["$and"].append(field_dict)
                            else:
                                review_status_filter_dict["$and"] = [field_dict]
                        else:
                            field_dict["$and"] = [
                                {f"{field}.type": {"$ne": "approve"}},
                                {f"{field}.is_override": {"$eq": False}},
                            ]
                            if "$or" in review_status_filter_dict:
                                review_status_filter_dict["$or"].append(field_dict)
                            else:
                                review_status_filter_dict["$or"] = [field_dict]

    return (
        limit,
        skip,
        sort_tuple_list,
        filter_dict,
        group_by_list,
        value_aggregate_list,
        review_status_filter_dict,
    )


def extract_field_bundle_grid_data(
    user,
    token_info,
):
    """

    :param user:
    :param token_info:
    :return:
    """
    if connexion.request.is_json:
        grid_selector = GridSelector.from_dict(connexion.request.get_json())
        user_json = token_info.get("user_obj", None) if token_info else None
        if not user_json:
            return unauthorized_response()
        if not (grid_selector.workspace_id and grid_selector.field_bundle_id):
            err_str = (
                "Workspace Id and Field Bundle Id are mandatory to extract grid data"
            )
            logger.info(err_str)
            return err_response(err_str, 422)
        # Permission Check
        user_permission, _ws = nosql_db.get_user_permission(
            grid_selector.workspace_id,
            email=user,
            user_json=user_json,
        )
        if user_permission not in ["admin", "owner", "editor", "viewer"]:
            err_str = "Not authorized to extract field bundle grid data"
            log_str = (
                f"user {user} not authorized to extract field bundle grid data in "
                f"workspace {grid_selector.workspace_id}"
            )
            logger.info(log_str)
            return err_response(err_str, 403)
        logger.info(
            f"Extracting Grid data for workspace {grid_selector.workspace_id} "
            f"and field bundle {grid_selector.field_bundle_id}",
        )
        logger.info(f"Grid Query: {grid_selector.grid_query}")
        (
            limit,
            skip,
            sort_tuple_list,
            filter_dict,
            group_by_list,
            value_aggregate_list,
            review_status_filter_dict,
        ) = parse_grid_query(grid_selector.grid_query, grid_selector.field_bundle_id)

        # If there are no field id list passed, return only the file ids
        return_only_file_ids = False
        if not grid_selector.field_ids:
            return_only_file_ids = True

        logger.info(f"file_ids: {grid_selector.doc_ids}")
        logger.info(f"field_ids: {grid_selector.field_ids}")
        logger.info(f"limit: {limit}")
        logger.info(f"skip: {skip}")
        logger.info(f"sort_tuple_list: {sort_tuple_list}")
        logger.info(f"filter_dict: {filter_dict}")
        logger.info(f"group_by_list: {group_by_list}")
        logger.info(f"value_aggregate_list: {value_aggregate_list}")
        logger.info(f"review_status_filter_dict: {review_status_filter_dict}")
        logger.info(f"distinct_field: {grid_selector.distinct_field}")

        res = nosql_db.retrieve_grid_data_from_field_values(
            grid_selector.workspace_id,
            grid_selector.field_bundle_id,
            file_ids=grid_selector.doc_ids,
            field_ids=grid_selector.field_ids,
            limit=limit,
            skip=skip,
            sort_tuple_list=sort_tuple_list,
            filter_dict=filter_dict,
            group_by_list=group_by_list,
            value_aggregate_list=value_aggregate_list,
            review_status_filter_dict=review_status_filter_dict,
            distinct_field=grid_selector.distinct_field,
            return_only_file_ids=return_only_file_ids,
            return_top_fact_answer=grid_selector.return_top_fact_answer,
        )
        if return_only_file_ids:
            res["results"] = [{"file_idx": x} for x in res["results"]]
        return make_response(jsonify(res), 200)
    else:
        status, rc, msg = "fail", 422, "invalid json"
        return make_response(jsonify({"status": status, "reason": msg}), rc)


def download_field_bundle_grid_data(
    user,
    token_info,
    workspace_id: str,
    field_bundle_id: str,
):
    try:
        bundle_info = nosql_db.get_field_bundle_info(field_bundle_id)
        if not bundle_info:
            err_str = "Invalid Field Bundle ID"
            log_str = f"user {user} Invalid Field bundleID {field_bundle_id}"
            logger.info(log_str)
            return err_response(err_str, 404)
        if bundle_info.workspace_id != workspace_id:
            err_str = "Workspace ID provided does not match with that of Field Bundle"
            log_str = (
                f"user {user} Workspace ID {workspace_id} "
                f"provided does not match with that of Field Bundle {bundle_info.workspace_id}"
            )
            logger.info(log_str)
            return err_response(err_str, 404)
        user_permission, ws = nosql_db.get_user_permission(
            bundle_info.workspace_id,
            email=user,
            user_json=token_info.get("user_obj", None) if token_info else None,
        )
        if user_permission not in ["admin", "owner", "editor", "viewer"]:
            err_str = "Not authorized to download grid data"
            log_str = (
                f"user {user} not authorized to extract field bundle documents in "
                f"workspace {bundle_info.workspace_id}"
            )
            logger.info(log_str)
            return err_response(err_str, 403)
        # Retrieve the fields
        projection = {
            "_id": 0,
            "id": 1,
            "name": 1,
        }
        fields = nosql_db.get_fields_in_bundle(
            field_bundle_id,
            projection=projection,
        )
        field_idx2field = {}
        for field in fields:
            field_idx2field[field.id] = field
        # Reset the order of fields
        fields = [field_idx2field[i] for i in bundle_info.field_ids]
        # Retrieve the output from Database
        output = nosql_db.download_grid_data_from_field_values(
            workspace_id,
            field_bundle_id,
        )
        # Construct Dataframe
        df = pd.DataFrame(output)
        # Add any extra cols (workflow fields which are not yet edited)
        df_cols = list(df.columns.values)
        cols_extra = list(set(bundle_info.field_ids) - (set(df_cols) - {"file_name"}))
        for col_name in cols_extra:
            df[col_name] = ""
        # Re-arrange the order
        df = df[["file_name"] + bundle_info.field_ids]
        # Replace the field id with Fields names
        rename_dict = {k: v.name for k, v in field_idx2field.items()}
        rename_dict["file_name"] = "File Name"
        df.rename(columns=rename_dict, inplace=True)

        tempfile_handler, tempfile_location = tempfile.mkstemp(suffix=".xlsx")
        os.close(tempfile_handler)
        doc_mimetype = (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        df.to_excel(tempfile_location, sheet_name=f"{bundle_info.bundle_name}")
        attachment_filename = f"{ws.name} - {bundle_info.bundle_name}"
        attachment_filename = attachment_filename.replace(".", "_")
        return send_from_directory(
            os.path.dirname(tempfile_location),
            os.path.basename(tempfile_location),
            mimetype=doc_mimetype,
            as_attachment=True,
            download_name=attachment_filename,
        )
    except Exception as e:
        logger.error(
            f"Error downloading values for field bundle {field_bundle_id} for workspace {workspace_id}, "
            f"error: {traceback.format_exc()}",
        )
        return make_response(jsonify({"status": "fail", "reason": str(e)}))
