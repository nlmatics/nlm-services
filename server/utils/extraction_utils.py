from server.storage import nosql_db


def get_field_bundle_grid_data(
    workspace_id,
    field_bundle_id,
    fields=[],
    docs_in_ws=[],
    use_id_as_key=False,
):
    # get fields info if not already specified
    field_idx2field = {}
    if len(fields) == 0:
        bundle_info = nosql_db.get_field_bundle_info(field_bundle_id)
        fields = nosql_db.get_fields_in_bundle(field_bundle_id)
        for field in fields:
            field_idx2field[field.id] = field
        # Reset the order
        fields = [field_idx2field[i] for i in bundle_info.field_ids]
    else:
        for field in fields:
            field_idx2field[field.id] = field

    # get docs if not provided
    if len(docs_in_ws) == 0:
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
    # grid output
    output = {}
    document_id_to_name = {}
    for document in docs_in_ws:
        document_id_to_name[document.id] = document.name
        output[document.id] = {}
        for field in fields:
            output[document.id][field.id] = {
                "answers": 0,
                # "criteria_uid": "d1289f32",
                "criterias": field.search_criteria.criterias,
                "file_idx": document.id,
                "file_name": document.name,
                "post_processors": field.search_criteria.post_processors,
                "topic": field.name,
                "topicId": field.id,
                "top_fact": {},
            }

    for fact in nosql_db.read_extracted_field(
        condition={
            "workspace_idx": workspace_id,
            "field_bundle_idx": field_bundle_id,
            "field_idx": {"$in": list(field_idx2field.keys())},
        },
        projection={
            "_id": 0,
            "file_idx": 1,
            "field_idx": 1,
            "answers": 1,
            "top_fact": 1,
        },
    ):
        # backward compatibility, can be removed after CH-3616 with all field extracted
        if "top_fact" not in fact:
            fact["top_fact"] = {}

        if fact["file_idx"] in output:
            output[fact["file_idx"]][fact["field_idx"]]["top_fact"] = fact["top_fact"]

    for k, v in output.items():
        output[k] = list(v.values())

    grid_output = []
    id_field = "topicId" if use_id_as_key else "topic"
    for file_idx, x in output.items():
        _grid_output = {
            "file_idx": file_idx,
            "file_name": document_id_to_name[file_idx],
        }
        for fact in x:
            # if fact["top_fact"].get("answer", "") and not fact["top_fact"].get("answer_details", {}):
            #     fact["top_fact"]["answer_details"] = {
            #         "formatted_value": fact["top_fact"]["answer"],
            #         "raw_value": fact["top_fact"]["answer"]
            #     }
            _grid_output[fact[id_field]] = fact["top_fact"]
        grid_output.append(_grid_output)
    return grid_output, fields, docs_in_ws
