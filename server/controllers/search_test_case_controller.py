import logging

import connexion
from flask import jsonify
from flask import make_response

from server import err_response
from server.controllers.extraction_controller import apply_template
from server.models.search_result import SearchResult  # noqa: E501
from server.storage import nosql_db


def flag_search_result(
    user,
    token_info,
    body=None,
):  # noqa: E501
    """Flags search and stores in db

     # noqa: E501

    :param user:
    :param token_info:
    :param body:
    :type body: dict | bytes

    :rtype: Object
    """
    if connexion.request.is_json:
        body = SearchResult.from_dict(connexion.request.get_json())  # noqa: E501
        user_json = token_info.get("user_obj", None)
        user_permission, _ws = nosql_db.get_user_permission(
            body.workspace_id,
            email=user,
            user_json=user_json,
        )
        if user_permission not in ["admin", "owner", "editor"]:
            err_str = "Not authorized to access this workspace"
            log_str = f"user {user} not authorized to flag search result in workspace {body.workspace_id}"
            logging.info(log_str)
            return err_response(err_str, 403)

        image = connexion.request.get_json()["image"]
        nosql_db.flag_search_result(body.to_dict(), image)
    return "do some magic!"


def remove_approved_search_result(
    user,
    token_info,
    body=None,
):  # noqa: E501
    """Stores Search Result in DB

     # noqa: E501

    :param body:
    :param user:
    :param token_info:
    :type body: dict | bytes

    :rtype: Object
    """
    if connexion.request.is_json:
        body = SearchResult.from_dict(connexion.request.get_json())  # noqa: E501
        user_json = token_info.get("user_obj", None)
        user_permission, _ws = nosql_db.get_user_permission(
            body.workspace_id,
            email=user,
            user_json=user_json,
        )
        if user_permission not in ["admin", "owner", "editor"]:
            err_str = "Not authorized to access this workspace"
            log_str = f"user {user} not authorized to remove approved search result for {body.workspace_id}"
            logging.info(log_str)
            return err_response(err_str, 403)

        nosql_db.remove_search_result(body.to_dict())
    return "do some magic!"


def remove_flagged_result(
    user,
    token_info,
    body=None,
):  # noqa: E501
    """Undo flagged search result

     # noqa: E501

    :param user:
    :param token_info:
    :param body:
    :type body: dict | bytes

    :rtype: Object
    """
    if connexion.request.is_json:
        body = SearchResult.from_dict(connexion.request.get_json())  # noqa: E501
        user_json = token_info.get("user_obj", None)
        user_permission, _ws = nosql_db.get_user_permission(
            body.workspace_id,
            email=user,
            user_json=user_json,
        )
        if user_permission not in ["admin", "owner", "editor"]:
            err_str = "Not authorized to access this workspace"
            log_str = f"user {user} not authorized to remove flagged search result for {body.workspace_id}"
            logging.info(log_str)
            return err_response(err_str, 403)

        # print(body.to_dict())
        # print("remove flag")
        nosql_db.remove_flag_search_result(body.to_dict())
    return "do some magic!"


def store_search_result(
    user,
    token_info,
    body=None,
):  # noqa: E501
    """Stores Search Result in DB

     # noqa: E501

    :param user:
    :param token_info:
    :param body:
    :type body: dict | bytes

    :rtype: Object
    """
    if connexion.request.is_json:
        body = SearchResult.from_dict(connexion.request.get_json())  # noqa: E501
        user_json = token_info.get("user_obj", None)
        user_permission, _ws = nosql_db.get_user_permission(
            body.workspace_id,
            email=user,
            user_json=user_json,
        )
        if user_permission not in ["admin", "owner", "editor"]:
            err_str = "Not authorized to access this workspace"
            log_str = f"user {user} not authorized to store approved search result for {body.workspace_id}"
            logging.info(log_str)
            return err_response(err_str, 403)

        nosql_db.store_search_result(body.to_dict())
    return "do some magic!"


def get_doc_search_results(
    user,
    token_info,
    ws_id,
    doc_id,
):  # noqa: E501
    """run ingestor tests for tables for one document or all

     # noqa: E501

    :param document_id:
    :type document_id: str

    :rtype: Object
    """
    user_json = token_info.get("user_obj", None)
    user_permission, _ws = nosql_db.get_user_permission(
        ws_id,
        email=user,
        user_json=user_json,
    )
    if user_permission not in ["admin", "owner", "editor"]:
        err_str = "Not authorized to access this workspace"
        log_str = f"user {user} not authorized to retrieve search result for {ws_id}"
        logging.info(log_str)
        return err_response(err_str, 403)

    retrieved_tests = nosql_db.get_document_search_test_cases(ws_id, doc_id)
    search_tests = []
    for search_test in retrieved_tests:
        search_test_dict = {
            k: search_test[k]
            for k in [
                "_id",
                "user_id",
                "workspace_id",
                "header_text",
                "group_type",
                "doc_id",
                "search_answer",
                "search_criteria",
                "time_stamp",
            ]
        }

        # Since test cases were created before these attributes were added
        if "raw_scores" not in search_test:
            search_test_dict["raw_scores"] = []
        else:
            search_test_dict["raw_scores"] = search_test["raw_scores"]

        if "tags" not in search_test:
            search_test_dict["tags"] = []
        else:
            search_test_dict["tags"] = search_test["tags"]

        search_test_dict["_id"] = str(search_test_dict["_id"])
        search_tests.append(search_test_dict)

    if len(search_tests):
        return make_response(
            jsonify({"status": "found test cases", "data": search_tests}),
            200,
        )
    else:
        logging.info(f"no test-cases found for document id {doc_id}")
        return make_response(
            jsonify({"status": "test cases not found", "data": []}),
            200,
        )


def get_flagged_search_results(
    user,
    token_info,
    ws_id,
    doc_id,
):  # noqa: E501
    """run ingestor tests for tables for one document or all

     # noqa: E501

    :param document_id:
    :type document_id: str

    :rtype: Object
    """
    user_json = token_info.get("user_obj", None)
    user_permission, _ws = nosql_db.get_user_permission(
        ws_id,
        email=user,
        user_json=user_json,
    )
    if user_permission not in ["admin", "owner", "editor"]:
        err_str = "Not authorized to access this workspace"
        log_str = (
            f"user {user} not authorized to retrieve flagged search result for {ws_id}"
        )
        logging.info(log_str)
        return err_response(err_str, 403)

    retrieved_flagged_tests = nosql_db.get_document_flagged_search_test_cases(
        ws_id,
        doc_id,
    )
    search_tests = []
    for search_test in retrieved_flagged_tests:

        search_test_dict = {
            k: search_test[k]
            for k in [
                "_id",
                "user_id",
                "workspace_id",
                "header_text",
                "group_type",
                "doc_id",
                "search_answer",
                "search_criteria",
                "time_stamp",
            ]
        }
        if "raw_scores" not in search_test:
            search_test_dict["raw_scores"] = []
        else:
            search_test_dict["raw_scores"] = search_test["raw_scores"]
        search_test_dict["_id"] = str(search_test_dict["_id"])

        if "raw_scores" not in search_test:
            search_test_dict["raw_scores"] = []
        else:
            search_test_dict["raw_scores"] = search_test["raw_scores"]

        if "tags" not in search_test:
            search_test_dict["tags"] = []
        else:
            search_test_dict["tags"] = search_test["tags"]

        search_tests.append(search_test_dict)

    if len(search_tests):
        return make_response(
            jsonify({"status": "found flagged test cases", "data": search_tests}),
            200,
        )
    else:
        logging.info(f"no test-cases found for document id {doc_id}")
        return make_response(
            jsonify({"status": "flagged test cases not found", "data": []}),
            200,
        )


def run_test(
    user,
    token_info,
    workspace_id="ALL",
    doc_id="",
):
    # workspace_id == ALL: all test cases
    # doc_id == ALL: all test cases in workspace
    user_json = token_info.get("user_obj", None)
    user_permission, _ws = nosql_db.get_user_permission(
        workspace_id,
        email=user,
        user_json=user_json,
    )
    perm_list = ["admin", "owner", "editor"]
    if workspace_id == "ALL":
        perm_list = ["admin"]
    if user_permission not in perm_list:
        err_str = "Not authorized to access this workspace"
        log_str = f"user {user} not authorized to access this workspace{workspace_id}"
        logging.info(log_str)
        return err_response(err_str, 403)

    results = {}
    total_correct = total_incorrect = 0
    tests = nosql_db.get_document_search_test_cases(workspace_id, doc_id)
    for test in tests:
        try:
            query = test["search_criteria"]
            kwargs = {
                "workspace_idx": test["workspace_id"],
                "file_idx": test["doc_id"],
                "ad_hoc": True,
                # 'field_boundle_idx': '',
                # 'override_topic': ['ALL'],
                "template": query["template_text"] if query["template_text"] else "",
                "question": query["template_question"]
                if query["template_question"]
                else [""],
                "header": query["header_text"] if query["header_text"] else "",
                "post_processors": query["post_processors"]
                if query["post_processors"]
                else [""],
                "aggregate_post_processors": query["aggregate_processors"]
                if query["aggregate_processors"]
                else [""],
            }
            output = apply_template(**kwargs)
            topic_facts = output[0]["topic_facts"]
            resp = output[0]["topic_facts"][0]
            ans = test["search_answer"]
            # print(output[0]["topic_facts"][0].keys())
            # dict_keys(['matches', 'phrase', 'match_idx', 'page_idx', 'block_type', 'answer', 'table', 'table_all', 'formatted_answer',
            # 'scaled_score', 'match_score', 'relevancy_score', 'file_score', 'semantic_score', 'is_override', 'uniq_id', 'group_type',
            # 'header_text', 'header_text_terms', 'block_text_terms', 'match_text_terms', 'match_semantic_terms', 'header_semantic_terms'])
            correct = True
            # "group_type", "header_text",
            for field in ["answer", "formatted_answer", "phrase", "page_idx"]:
                if not resp[field] == ans[field]:
                    correct = False
                    break
            for field in ["group_type", "header_text"]:
                if not resp[field] == test[field]:
                    correct = False
                    break
            if correct:
                total_correct += 1
            else:
                total_incorrect += 1

            _id = str(test["_id"])
            extracted_answer = {
                "answer": resp["answer"],
                "formatted_answer": resp["formatted_answer"],
                "group_type": resp["group_type"],
                "phrase": resp["phrase"],
                "page_idx": resp["page_idx"],
                "header_text": resp["header_text"],
            }
            results[_id] = {
                "extracted_result": extracted_answer,
                "correct": correct,
                "topic_facts": topic_facts,
            }
            results[_id].update(test)
            del results[_id]["_id"]
        except Exception as e:
            logging.error(f"{e}")

    return make_response(
        jsonify(
            {
                "total_correct": total_correct,
                "total_incorrect": total_incorrect,
                "data": results,
            },
        ),
        200,
    )
