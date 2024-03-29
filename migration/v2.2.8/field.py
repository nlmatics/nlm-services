from server.models.criteria import Criteria
from server.models.field import Field
from server.models.search_criteria import SearchCriteria
from server.storage import nosql_db


def process(old_field):
    if "search_criteria" in old_field:
        return

    # print(old_field["_id"])
    question = ""
    if "question" in old_field:
        question = old_field["question"]

    templates = []
    if "patterns" in old_field:
        if old_field["patterns"]:
            for x in old_field["patterns"]:
                if x:
                    templates.append(x)

    headers = []
    if "section_heading" in old_field:
        if old_field["section_heading"]:
            headers.append(old_field["section_heading"])

    post_processors = []
    if "answer_format" in old_field:
        if old_field["answer_format"]:
            post_processors.append(old_field["answer_format"])

    aggregate_post_processors = []

    group_flag = "auto"
    if "enable_grouping" in old_field:
        if old_field["enable_grouping"]:
            group_flag = "auto"
        else:
            group_flag = "disable"

    table_flag = "auto"
    if "enable_table_search" in old_field:
        if old_field["enable_table_search"]:
            group_flag = "auto"
        else:
            group_flag = "disable"

    page_start = old_field.get("page_start", -1)
    page_end = old_field.get("page_end", -1)

    criteria = Criteria(
        question=question,
        templates=templates,
        headers=headers,
        expected_answer_type="auto",
        group_flag=group_flag,
        table_flag=table_flag,
        page_start=page_start,
        page_end=page_end,
        criteria_rank=-1,
    )

    search_criteria = SearchCriteria(
        criterias=[criteria],
        post_processors=post_processors,
        aggregate_post_processors=aggregate_post_processors,
        doc_per_page=20,
        offset=0,
        match_per_doc=20,
        debug=True,
        topn=3,
    )

    new_field = Field(
        id=old_field.get("id", None),
        name=old_field.get("name", None),
        active=old_field.get("active", None),
        user_id=old_field.get("user_id", None),
        workspace_id=old_field.get("workspace_id", None),
        is_user_defined=old_field.get("is_user_defined", None),
        is_entered_field=old_field.get("is_entered_field", None),
        parent_bundle_id=old_field.get("parent_bundle_id", None),
        search_criteria=search_criteria,
    )
    # return
    nosql_db.update_field_by_id(old_field["id"], new_field)


for idx, old_field in enumerate(nosql_db.db["field"].find()):
    print(idx, old_field["id"])
    process(old_field)
