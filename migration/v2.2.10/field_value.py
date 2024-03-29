from server.storage import nosql_db

nosql_db.db["field_value"].rename("old_field_value")


for idx, old_field in enumerate(nosql_db.db["old_field_value"].find()):
    print(idx)
    if "id" not in old_field:
        print("cant migrate")
        print(old_field)
        continue

    if (
        "field_id" not in old_field
        or "doc_id" not in old_field
        or "selected_row" not in old_field
        or not old_field["field_id"]
        or not old_field["doc_id"]
        or not old_field["selected_row"]
    ):
        print(f"invalid field_value: {old_field}")
        continue

    selected_row = old_field["selected_row"]
    selected_row.update({"type": "override"})
    new_field = {
        "field_idx": old_field["field_id"],
        "file_idx": old_field["doc_id"],
        "top_fact": selected_row,
    }

    if "workspace_id" in old_field:
        new_field["workspace_idx"] = old_field["workspace_id"]
    else:
        doc_info = nosql_db.get_document_info_by_id[old_field["doc_id"]]
        if not doc_info["workspace_id"]:
            print("ERROR: skipping")
            continue
        new_field["workspace_idx"] = doc_info["workspace_id"]

    if "bundle_id" in old_field:
        new_field["field_bundle_idx"] = old_field["bundle_id"]
    else:
        field_info = nosql_db.get_field_by_id(old_field["field_id"])
        new_field["field_bundle_idx"] = field_info["parent_bundle_id"]

    if "field_idx" not in new_field or not new_field["field_idx"]:
        print("error1")
        continue
    if "file_idx" not in new_field or not new_field["file_idx"]:
        print("error2")
        continue
    if "top_fact" not in new_field or not new_field["top_fact"]:
        print("error3")
        continue
    if "workspace_idx" not in new_field or not new_field["workspace_idx"]:
        print("error4")
        continue
    if "field_bundle_idx" not in new_field or not new_field["field_bundle_idx"]:
        print("error5")
        continue

    nosql_db.create_extracted_field([new_field])
