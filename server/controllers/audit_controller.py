import json
import logging
import os
import tempfile
from collections import defaultdict
from zipfile import ZipFile

import pandas as pd
from flask import send_from_directory

from server import err_response
from server.controllers.extraction_controller import apply_template
from server.storage import nosql_db


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def convert_de_lite_output_to_df(output):
    data = defaultdict(list)
    for file_output in output["outputs"]:
        for field_output in file_output:

            for key in [
                "file_idx",
                "file_name",
                "topic",
                "topicId",
            ]:
                data[key].append(field_output[key])

            data["display_answer"].append("")
            data["extracted_answer"].append("")
            data["display_match_idx"].append("")
            data["extracted_match_idx"].append("")
            data["is_override"].append(False)
            data["answer_in_top_10"].append(True)
            # no extracted answer, no override answer
            if len(field_output["topic_facts"]) == 0:
                continue
            display_answer = field_output["topic_facts"][0]

            data["display_answer"][-1] = display_answer["formatted_answer"]
            data["display_match_idx"][-1] = display_answer["match_idx"]

            data["extracted_answer"][-1] = display_answer["formatted_answer"]
            data["is_override"][-1] = display_answer["is_override"]

            if data["is_override"][-1]:
                # has extracted answer
                if len(field_output["topic_facts"]) > 1:
                    extracted_answer = field_output["topic_facts"][1]
                    data["extracted_answer"][-1] = extracted_answer["formatted_answer"]
                    data["extracted_match_idx"][-1] = extracted_answer["match_idx"]

                # override value comes from top-k
                for answer in field_output["topic_facts"][1:]:
                    if display_answer["match_idx"] == answer["match_idx"]:
                        data["answer_in_top_10"][-1] = (
                            display_answer["formatted_answer"]
                            == answer["formatted_answer"]
                        )

    df = pd.DataFrame(data)
    return df


def generate_report(df, group_by, identifier):
    data = defaultdict(list)
    for group, _df in df.groupby(group_by):
        data[group_by].append(group)
        data[identifier].append(_df.iloc[0][identifier])
        data["total"].append(_df.shape[0])
        data["overridden"].append(_df[_df["is_override"]].shape[0])
        data["correct"].append(
            _df[_df["display_answer"] == _df["extracted_answer"]].shape[0],
        )
        data["answer_in_top_10"].append(_df[_df["answer_in_top_10"]].shape[0])

    df = pd.DataFrame(data)

    df["accuracy"] = df.apply(lambda row: f"{row.correct/row.total*100:.2f}%", axis=1)
    df["accuracy in top 10"] = df.apply(
        lambda row: f"{row.answer_in_top_10/row.total*100:.2f}%",
        axis=1,
    )

    return df


def get_audit_report(
    user,
    token_info,
    workspace_id,
    start_date_time="2000-01-01 00:00:00",
    end_date_time="",
    output_format="csv",
):  # noqa: E501

    user_permission, _ws = nosql_db.get_user_permission(
        workspace_id,
        user_json=token_info.get("user_obj", None),
    )
    if user_permission not in ["admin", "owner", "editor", "viewer"]:
        err_str = "Not authorized to retrieve audit report"
        log_str = (
            f"user {user} not authorized to retrieve audit report for {workspace_id}"
        )
        logger.info(log_str)
        return err_response(err_str, 403)

    accuracy_report = {}

    field_bundles = nosql_db.get_field_bundles_in_workspace(workspace_id)

    zip_file_handler, zip_filepath = tempfile.mkstemp(suffix=".zip")
    os.close(zip_file_handler)
    zip_file = ZipFile(zip_filepath, "w")

    # dfs = []
    for field_bundle in field_bundles:
        outputs = apply_template(
            workspace_idx=workspace_id,
            field_bundle_idx=field_bundle.id,
        )
        df = convert_de_lite_output_to_df(outputs)
        df["field_bundle_idx"] = field_bundle.id
        df["field_bundle_name"] = field_bundle.bundle_name
        df["correct"] = df["display_answer"] == df["extracted_answer"]

        accuracy_report[f"{field_bundle.id}-{field_bundle.bundle_name}"] = {
            "accuracy@1": df[df["correct"]].shape[0] / df.shape[0],
            "accuracy@10": df[df["answer_in_top_10"]].shape[0] / df.shape[0],
        }

        # dfs.append(df)

        _df = generate_report(df, group_by="topicId", identifier="topic")
        with zip_file.open(
            f"{field_bundle.id}_{field_bundle.bundle_name}_by_field.csv",
            "w",
        ) as f:
            _df.to_csv(f)

        _df = generate_report(df, group_by="file_idx", identifier="file_name")
        with zip_file.open(
            f"{field_bundle.id}_{field_bundle.bundle_name}_by_document.csv",
            "w",
        ) as f:
            _df.to_csv(f)

        _df = df.set_index(["file_name", "topic"])["correct"].unstack()
        with zip_file.open(
            f"{field_bundle.id}_{field_bundle.bundle_name}_accuracy_details.csv",
            "w",
        ) as f:
            _df.to_csv(f)

    with zip_file.open("accuracy_report.json", "w") as f:
        data = json.dumps(accuracy_report, indent=2)
        f.write(bytes(data, encoding="utf-8"))

    zip_file.close()

    try:
        return send_from_directory(
            os.path.dirname(zip_filepath),
            os.path.basename(zip_filepath),
            mimetype="zip",
            as_attachment=True,
            download_name=f"report_{workspace_id}.zip",
        )
    finally:
        os.unlink(zip_filepath)
