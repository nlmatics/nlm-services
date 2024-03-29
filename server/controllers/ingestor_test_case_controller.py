import logging
import os
import traceback

import connexion
from bs4 import BeautifulSoup
from flask import jsonify
from flask import make_response
from nlm_ingestor.ingestor import table_parser
from nlm_ingestor.ingestor import visual_ingestor
from nlm_utils.storage import file_storage
from tika import parser

from server import unauthorized_response
from server.models.ingest_table_test_case import IngestTableTestCase  # noqa: E501
from server.models.ingest_test_case import IngestTestCase  # noqa: E501
from server.storage import nosql_db

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def create_ingestor_test_case(user, body):  # noqa: E501
    """Creates ingestor testcase db entry

     # noqa: E501

    :param body:
    :type body: dict | bytes

    :rtype: Object
    """
    status = "Pass"
    msg = "magic"
    rc = 200
    try:
        user_obj = nosql_db.get_user_by_email(user)
        if not user_obj:
            return unauthorized_response()

        if connexion.request.is_json:
            body = IngestTestCase.from_dict(connexion.request.get_json())  # noqa: E501

        nosql_db.create_ingestor_test_case(**body.to_dict())
    except Exception as e:
        logger.error(
            f"error uploading file, stacktrace: {traceback.format_exc()}",
            exc_info=True,
        )
        status, rc, msg = "fail", 500, str(e)

    return make_response(jsonify({"status": status, "reason": msg}), rc)


def get_test_case_by_workspace(user, workspace_id):  # noqa: E501
    """Gets testcase by workspace

     # noqa: E501

    :param workspace_id:
    :type workspace_id: str

    :rtype: Object
    """
    status = "Pass"
    msg = "magic"
    rc = 200
    try:
        print(user)
        user_obj = nosql_db.get_user_by_email(user)
        if not user_obj:
            return unauthorized_response()
    except Exception as e:
        logger.error(
            f"error uploading file, stacktrace: {traceback.format_exc()}",
            exc_info=True,
        )
        status, rc, msg = "fail", 500, str(e)

    return make_response(jsonify({"status": status, "reason": msg}), rc)


def add_correct_table(user, body):
    # print(user)
    logger.info("creating table")
    if connexion.request.is_json:
        body = IngestTableTestCase.from_dict(connexion.request.get_json())  # noqa: E501
    body = body.to_dict()
    # insert correct tables into database
    # print(body.keys())
    # print(body)
    table = parse_table(body["html_text"])
    table["doc_id"] = body["doc_id"]
    table["tag"] = body["tag"]
    table["user_id"] = body["user_id"]
    nosql_db.create_ingestor_table_test(table)
    return make_response(jsonify({"status": 200, "reason": 200}), 200)


def parse_table(html_text):
    data = []
    soup = BeautifulSoup(html_text, "html.parser")
    table = soup.find("table")
    # extract table attributes passed in from visual_ingestor's render_html
    attrs = soup.find("table").attrs
    page_idx = attrs["page_idx"]
    name = attrs["name"]
    top, left = attrs["top"], attrs["left"]
    # parse table by rows
    table_body = table.find("tbody")
    rows = table_body.find_all("tr")
    for row in rows:
        # find th and td cells
        cols = row.find_all("th") if row.find_all("th") else row.find_all("td")
        cols = [x.text.strip() for x in cols]
        data.append([x for x in cols])
    # print(data)
    return {"table": data, "page_idx": page_idx, "name": name, "top": top, "left": left}


def ingestor_debug():
    visual_ingestor.HTML_DEBUG = False
    visual_ingestor.LINE_DEBUG = False
    visual_ingestor.MIXED_FONT_DEBUG = False
    table_parser.TABLE_DEBUG = False
    table_parser.TABLE_COL_DEBUG = False
    visual_ingestor.HF_DEBUG = False
    visual_ingestor.REORDER_DEBUG = False
    visual_ingestor.MERGE_DEBUG = False


def get_html(doc_id):
    # download document to temp then parse it
    doc_loc = nosql_db["document"].find_one({"id": doc_id})["doc_location"]
    dest_location = file_storage.download_document(doc_loc)
    parsed = parser.from_file(dest_location, xmlContent=True)
    soup = BeautifulSoup(str(parsed), "html.parser")
    pages = soup.find_all("div", class_=lambda x: x in ["page"])
    ingestor_debug()
    parsed_doc = visual_ingestor.Doc(pages, [])
    os.remove(dest_location)
    # get the html str to compare with
    html_text = parsed_doc.html_str
    return html_text


def compare_results(test, html_text):
    # search on the entire html for table with provided attributes
    soup = BeautifulSoup(html_text, "html.parser")
    table = soup.find(
        "table",
        {"page": test["page_idx"], "top": test["top"], "left": test["left"]},
    )
    if not table:
        return "missed", test

    table_body = table.find("tbody")
    data = []
    rows = table_body.find_all("tr")
    for row in rows:
        cols = row.find_all("th") if row.find_all("th") else row.find_all("td")
        cols = [x.text.strip() for x in cols]
        data.append([x for x in cols])
    # check if the contents in the table matches
    if data == test["table"]:
        return "correct", test
    else:
        return "incorrect", data


def print_results(
    total_tests,
    total_documents,
    correct_tables,
    missed_tables,
    incorrect_tables,
):
    def print_table(table):
        # format table nicely before printing it
        s = ""
        for i in range(len(table)):
            row = table[i]
            s += f"{i} {row} \n"
        return s

    def print_document_attrs(doc_id):
        doc = nosql_db["document"].find_one({"id": doc_id})
        workspace = nosql_db["workspace"].find_one({"id": doc["workspace_id"]})
        return f"Workspace: {workspace['name']}; Document: {doc['name']}, {doc['id']}"

    # print stuff
    line_break = "======================================================== \n"
    row_break = "***** \n"
    s = ""
    if len(incorrect_tables) > 0:
        s += line_break
        s += "Incorrect Matches: \n"
        for match in incorrect_tables:
            s += row_break
            correct = match[0]
            correct_pos = f"{correct['top']}, {correct['left']}"
            s += f"{print_document_attrs(correct['doc_id'])}; Page: {correct['page_idx']}; location: {correct_pos}; Table Name: {correct['name']}; Tag: {correct['tag']} \n"
            s += "Stored: \n"
            s += print_table(correct["table"])
            s += "Parsed: \n"
            s += print_table(match[1])

    if len(missed_tables) > 0:
        s += line_break
        s += "Missed Matches: \n"
        for match in missed_tables:
            s += row_break
            match_pos = f"{match['top']}, {match['left']} \n"
            s += f"{print_document_attrs(match['doc_id'])}; Page: {match['page_idx']}; location: {match_pos}; Table Name: {match['name']}; Tag: {match['tag']} \n"
            s += print_table(match["table"])

    s += line_break
    s += f"Total documents: {total_documents}, Total tests: {total_tests}, correct: {len(correct_tables)}, incorrect: {len(incorrect_tables)}, missed: {len(missed_tables)} \n"
    s += line_break
    return s


# def run_test_table(doc_id="", output=False):
#     total_tests = 0
#     total_documents = 0
#     correct_tables = []
#     missed_tables = []
#     incorrect_tables = []
#     status = 200

#     if doc_id:
#         # run test case in a document
#         html_text = get_html(doc_id)
#         for test in test_db[doc_id].find():
#             result, data = compare_results(test, html_text)
#             if result == "correct":
#                 correct_tables.append(test)
#             elif result == "incorrect":
#                 incorrect_tables.append([test, data])
#             elif result == "missed":
#                 missed_tables.append(test)

#         total_documents += 1
#     else:
#         # loop through every document in every collection
#         for doc_id in test_db.list_collection_names():
#             total_documents += 1
#             html_text = get_html(doc_id)
#             for test in test_db[doc_id].find():
#                 result, data = compare_results(test, html_text)
#                 if result == "correct":
#                     correct_tables.append(test)
#                 elif result == "incorrect":
#                     incorrect_tables.append([test, data])
#                 elif result == "missed":
#                     missed_tables.append(test)

#                 total_tests += 1
#     response = print_results(
#         total_tests, total_documents, correct_tables, missed_tables, incorrect_tables,
#     )

#     if output:
#         print(response)
#     else:
#         # return make_response(response)
#         msg = "nice"
#         rc = 200
#         return make_response(jsonify({"status": status, "reason": msg}), rc)


def get_page_data(html_text):
    soup = BeautifulSoup(html_text, "html.parser")

    page_data = []
    min_indent = 100000
    for tag in soup.find_all(recursive=False):
        name = tag.name
        style = tag.get("style")
        if style and "margin-left: " in style and "px" in style:
            indent = int(style.split("margin-left: ")[1].split("px")[0]) + 20
        elif not style and (tag.get("block_idx") or tag.get("block_idx") == "0"):
            indent = 20
        else:
            indent = None

        if indent:
            min_indent = min(indent, min_indent)
            page_data.append([name, indent])
            page_idx = soup.find("").attrs["page_idx"]

    for p in page_data:
        p[1] = (p[1] - min_indent) // 20

    page = {
        "page_data": page_data,
        "page_idx": page_idx,
    }

    return page


def flag_page(
    token_info,
    body,
):  # noqa: E501
    """store page, doc_id, user_id, workspace_id

     # noqa: E501

    :param token_info:
    :param body:
    :type body: dict | bytes
      "doc_id": "string",
      "page_idx": "string",
      "user_id": "string",
      "workspace_id": "string"
    :rtype: Object
    """
    user_json = token_info["user_obj"]
    if not user_json.get("is_admin", False):
        return make_response(
            jsonify(
                {
                    "status": "fail",
                    "reason": "Only Admins can perform this action.",
                },
            ),
            403,
        )
    logger.info("approving page")
    body = connexion.request.get_json()  # noqa: E501
    page = get_page_data(body["html_text"])
    page["doc_id"] = body["doc_id"]
    page["tag"] = body["tag"] if "tag" in body else ""
    page["user_id"] = body["user_id"]
    nosql_db.create_ingestor_page_test(page)
    return make_response(jsonify({"status": 200, "reason": 200}), 200)


def flag_table(
    token_info,
    body,
):  # noqa: E501
    """Store flagged table

     # noqa: E501

    :param token_info:
    :param body:
    :type body: dict | bytes

    "doc_id": "string",
    "page_idx": "string",
    "table_html": "string",
    "user_id": "string",
    "workspace_id": "string"

    :rtype: Object
    """
    user_json = token_info["user_obj"]
    if not user_json.get("is_admin", False):
        return make_response(
            jsonify(
                {
                    "status": "fail",
                    "reason": "Only Admins can perform this action.",
                },
            ),
            403,
        )
    if connexion.request.is_json:
        body = connexion.request.get_json()  # noqa: E501
        nosql_db.store_flagged_table(body)
    return "do some magic!"


def undo_flagged_page(
    token_info,
    body,
):
    """Store flagged table

     # noqa: E501

    :param token_info:
    :param body:
    :type body: dict | bytes

    "doc_id": "string",
    "page_idx": "string",
    "table_html": "string",
    "user_id": "string",
    "workspace_id": "string"

    :rtype: Object
    """
    user_json = token_info["user_obj"]
    if not user_json.get("is_admin", False):
        return make_response(
            jsonify(
                {
                    "status": "fail",
                    "reason": "Only Admins can perform this action.",
                },
            ),
            403,
        )
    if connexion.request.is_json:
        body = connexion.request.get_json()  # noqa: E501
        nosql_db.remove_flagged_page(body)
        print(body)
    return "do some magic!"


def undo_flagged_table(
    token_info,
    body,
):
    """Store flagged table

     # noqa: E501

    :param token_info:
    :param body:
    :type body: dict | bytes

    "doc_id": "string",
    "page_idx": "string",
    "table_html": "string",
    "user_id": "string",
    "workspace_id": "string"

    :rtype: Object
    """
    user_json = token_info["user_obj"]
    if not user_json.get("is_admin", False):
        return make_response(
            jsonify(
                {
                    "status": "fail",
                    "reason": "Only Admins can perform this action.",
                },
            ),
            403,
        )
    if connexion.request.is_json:
        body = connexion.request.get_json()  # noqa: E501
        nosql_db.remove_flagged_table(body)
        print(body)
    return "do some magic!"


def undo_ingest_test_case_approval(
    token_info,
    body,
):
    """Store flagged table

     # noqa: E501

    :param token_info:
    :param body:
    :type body: dict | bytes

    "doc_id": "string",
    "page_idx": "string",
    "table_html": "string",
    "user_id": "string",
    "workspace_id": "string"

    :rtype: Object
    """
    user_json = token_info["user_obj"]
    if not user_json.get("is_admin", False):
        return make_response(
            jsonify(
                {
                    "status": "fail",
                    "reason": "Only Admins can perform this action.",
                },
            ),
            403,
        )
    if connexion.request.is_json:
        body = connexion.request.get_json()  # noqa: E501
        body = IngestTestCase.from_dict(connexion.request.get_json())
        nosql_db.remove_ingestor_test_case(body.to_dict())
        # print(body)
    return "do some magic!"


def remove_table_test_case(
    token_info,
    body,
):
    """Store flagged table

     # noqa: E501

    :param token_info:
    :param body:
    :type body: dict | bytes

    "doc_id": "string",
    "page_idx": "string",
    "table_html": "string",
    "user_id": "string",
    "workspace_id": "string"

    :rtype: Object
    """
    user_json = token_info["user_obj"]
    if not user_json.get("is_admin", False):
        return make_response(
            jsonify(
                {
                    "status": "fail",
                    "reason": "Only Admins can perform this action.",
                },
            ),
            403,
        )
    if connexion.request.is_json:
        body = IngestTableTestCase.from_dict(connexion.request.get_json())
        body = body.to_dict()
        table = parse_table(body["html_text"])
        table["doc_id"] = body["doc_id"]
        table["tag"] = body["tag"]
        nosql_db.remove_ingestor_table_test_case(table)
    return "do some magic!"
