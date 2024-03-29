import logging
import traceback

from flask import jsonify
from flask import make_response

from server import unauthorized_response
from server.storage import nosql_db

# import server.config as cfg

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Legacy function
# def update_search_history(user_id, query, id_type, workspace_id, doc_id=None):

#     logger.info("IN UPDATE SEARCH HISTORY!")

#     # Assign correct type
#     if id_type == "question":
#         question = query
#         pattern = None
#     if id_type == "pattern":
#         question = None
#         pattern = query

#     # Make sure pattern/question not null
#     if pattern is None and question is None:
#         return
#     if pattern == "" and question == "":
#         return
#     if pattern is None and question == "":
#         return
#     if pattern == "" and question is None:
#         return

#     # Generate timestamp and unique id
#     timestamp = str_utils.timestamp_as_str()
#     uniq_id = str_utils.generate_search_history_id(timestamp)

#     # Store in database
#     return nosql_db.update_search_history(
#         user_id,
#         uniq_id,
#         timestamp,
#         workspace_id=workspace_id,
#         doc_id=doc_id,
#         question=question,
#         pattern=pattern,
#     )


def get_search_history_by_days(
    user,
    token_info,
    id_type,
    days,
    workspace_id=None,
):
    """
    Return all user history of past n weeks.
    """
    user_obj = token_info.get("user_obj", None)
    if not user_obj:
        return unauthorized_response()

    user_id = user_obj["id"]

    # How to check if this is user_id?

    def _unique(items):
        seen = set()
        for i in range(len(items) - 1, -1, -1):
            it = items[i]
            if it in seen:
                del items[i]
            else:
                seen.add(it)
        return list(seen)

    history = nosql_db.get_search_history_by_days(
        user_id,
        id_type,
        days,
        workspace_id=workspace_id,
    )

    # # Sort history and return by string
    # # Get first question and pattern of list [change this if using all question in list]
    # if id_type == "question":
    #     history = [
    #         # s.question[0]
    #         # TODO: replace above line with this after search_history implemented in the frontend
    #         {"question": s.question[0], "pattern": s.pattern, "header": s.header, "format": s.format}
    #         for s in history
    #         if isinstance(s.question, list) and s.question
    #     ]
    # elif id_type == "pattern":
    #     history = [
    #         s.pattern[0] for s in history if isinstance(s.pattern, list) and s.pattern
    #     ]

    # Return search history without dates
    # history = [x[0] for x in history]

    # Unravel list of lists
    # try:
    #     history = reduce(operator.add, history)
    # except Exception as e:
    #     logger.error(
    #         f"This message may also appear if no search history is in database: {traceback.format_exc()}",
    #         str(e),
    #     )
    #     history = [""]

    # # Get unique entries [currently turned off]
    # history = _unique(history)

    try:
        return make_response(jsonify(history), 200)
    except Exception as e:
        logger.error(f"error retrieving search history, err: {traceback.format_exc()}")
        return make_response(jsonify({"status": "fail", "reason": str(e)}), 500)
