import logging
import os

from flask import jsonify
from flask import make_response
from nlm_utils.model_client import ClassificationClient
from nlm_utils.model_client import YoloClient
from nlm_utils.rabbitmq import producer
from nlm_utils.utils import query_preprocessing as qp_utils

from server import err_response
from server.models import id_with_message
from server.storage import nosql_db


# import server.config as cfg

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


qnli_client = ClassificationClient(
    model="roberta",
    task="qnli",
    url=os.getenv("MODEL_SERVER_URL"),
)

boolq_client = ClassificationClient(
    model="roberta",
    task="boolq",
    url=os.getenv(
        "BOOLQ_MODEL_SERVER_URL",
        os.getenv("MODEL_SERVER_URL"),
    ),
)

phrase_qa_client = ClassificationClient(
    model="roberta",
    task="roberta-phraseqa",
    url=os.getenv("MODEL_SERVER_URL"),
)

qa_client = ClassificationClient(
    model="roberta",
    task="roberta-qa",
    url=os.getenv(
        "QA_MODEL_SERVER_URL",
        os.getenv("MODEL_SERVER_URL"),
    ),
)

yolo_client = YoloClient(
    url=os.getenv("IMAGE_MODEL_SERVER_URL"),
)


def get_training_samples(
    user,
    token_info,
    status=None,
):
    user_obj = token_info.get("user_obj", None)
    if not user_obj or user_obj["is_admin"] is None or user_obj["is_admin"] is False:
        logger.info(
            f"{user} not authorized to retrieve training samples",
        )
        return make_response(
            jsonify({"status": "fail", "reason": "unauthorized"}),
            403,
        )

    training_samples = nosql_db.get_training_samples(status)
    return make_response(
        jsonify(
            training_samples,
        ),
        200,
    )


def get_unique_training_samples_status(
    user,
    token_info,
):
    user_obj = token_info.get("user_obj", None)
    if not user_obj or user_obj["is_admin"] is None or user_obj["is_admin"] is False:
        logger.info(
            f"{user} not authorized to retrieve training sample status",
        )
        return make_response(
            jsonify({"status": "fail", "reason": "unauthorized"}),
            403,
        )

    training_status = nosql_db.get_unique_training_sample_status()
    return make_response(
        jsonify(
            training_status,
        ),
        200,
    )


def update_training_sample_status(
    user,
    token_info,
    body,
):
    user_obj = token_info.get("user_obj", None)
    if not user_obj or user_obj["is_admin"] is None or user_obj["is_admin"] is False:
        logger.info(
            f"{user} not authorized to update training samples",
        )
        return make_response(
            jsonify({"status": "fail", "reason": "unauthorized"}),
            403,
        )
    logger.info("update_training_sample_status: ", body)
    nosql_db.update_saved_search_status([body["id"]], body["status"])
    return make_response(
        jsonify(
            id_with_message.IdWithMessage(
                id=body["id"],
                message=f"status updated to {body['status']}",
            ),
        ),
        200,
    )


def start_training(
    user,
    token_info,
):
    user_obj = token_info.get("user_obj", None)
    if not user_obj or user_obj["is_admin"] is None or user_obj["is_admin"] is False:
        logger.info(
            f"{user} not authorized to perform training",
        )
        return make_response(
            jsonify({"status": "fail", "reason": "unauthorized"}),
            403,
        )
    training_samples = nosql_db.get_training_samples()
    prepared_samples = {
        "qnli": {"sentences": [], "questions": [], "labels": []},
        "boolq": {"sentences": [], "questions": [], "labels": []},
        "qa": {"sentences": [], "questions": [], "answers": []},
        "phraseqa": {"sentences": [], "questions": [], "answers": []},
    }
    ids = []
    for sample in training_samples:
        ids.append(sample["id"])
        question = sample["question"]
        answer = sample["answer"]
        passage = sample["passage"]
        action = sample["action"]
        parent_text = sample["parent_text"]
        header_text_terms = sample["header_text_terms"]
        if qp_utils.is_bool_question(question):
            prepared_samples["boolq"]["questions"].append(question)
            prepared_samples["boolq"]["sentences"].append(parent_text + " " + passage)
            correct_answer = "Neutral"
            if action == "incorrect":
                if answer == "No" or answer == "Yes":
                    correct_answer = "Neutral"
                elif answer == "":
                    correct_answer = "True"
            else:
                if answer == "Yes" or answer == "":
                    correct_answer = "True"
                elif answer == "No":
                    correct_answer = "False"
            if action == "edited":
                if answer == "Yes":
                    correct_answer = "True"
                elif answer == "No":
                    correct_answer = "False"
                else:
                    correct_answer = "Neutral"
            prepared_samples["boolq"]["labels"].append(correct_answer)
        elif qp_utils.is_question(question):
            qa_text = passage
            if len(header_text_terms) > 0:
                qa_text = " ".join(header_text_terms) + ": " + passage
            prepared_samples["qnli"]["questions"].append(question)
            prepared_samples["qnli"]["sentences"].append(qa_text)
            correct_answer = "not_entailment" if action == "incorrect" else "entailment"
            prepared_samples["qnli"]["labels"].append(correct_answer)
            prepared_samples["qa"]["questions"].append(question)
            prepared_samples["qa"]["sentences"].append(qa_text)
            correct_answer = "" if action == "incorrect" else answer
            prepared_samples["qa"]["answers"].append(correct_answer)
        else:
            prepared_samples["phraseqa"]["questions"].append(question)
            prepared_samples["phraseqa"]["sentences"].append(passage)
            correct_answer = "" if action == "incorrect" else answer
            prepared_samples["phraseqa"]["answers"].append(correct_answer)

    task_body = {
        "ids": ids,
        "prepared_samples": prepared_samples,
        "clients": ["boolq", "qnli", "qa", "phraseqa"],
    }
    task = nosql_db.insert_task(user_obj["id"], "active_learning", task_body)
    # send task to rabbitmq producer
    res = producer.send(task)
    if res:
        nosql_db.update_saved_search_status(ids, "queued")
        logger.info("Active learning task queued")
    else:
        logger.info("Starting active learning without queue")
        update_workers = True
        # removing alternative code
        nosql_db.update_saved_search_status(ids, "training")
        try:
            if len(prepared_samples["boolq"]["questions"]):
                logger.info("training boolq")
                boolq_client.active_learning(
                    questions=prepared_samples["boolq"]["questions"],
                    sentences=prepared_samples["boolq"]["sentences"],
                    labels=prepared_samples["boolq"]["labels"],
                    update_workers=update_workers,
                )
                logger.info("training boolq complete")

            if len(prepared_samples["qnli"]["questions"]):
                logger.info("training qnli")
                qnli_client.active_learning(
                    questions=prepared_samples["qnli"]["questions"],
                    sentences=prepared_samples["qnli"]["sentences"],
                    labels=prepared_samples["qnli"]["labels"],
                    update_workers=update_workers,
                )
                logger.info("training qnli complete")

            if len(prepared_samples["qa"]["questions"]):
                logger.info("training qa")
                qa_client.active_learning(
                    questions=prepared_samples["qa"]["questions"],
                    sentences=prepared_samples["qa"]["sentences"],
                    answers=prepared_samples["qa"]["answers"],
                    update_workers=update_workers,
                )
                logger.info("training qa complete")

            if len(prepared_samples["phraseqa"]["questions"]):
                logger.info("training phraseqa")
                phrase_qa_client.active_learning(
                    questions=prepared_samples["phraseqa"]["questions"],
                    sentences=prepared_samples["phraseqa"]["sentences"],
                    answers=prepared_samples["phraseqa"]["answers"],
                    update_workers=update_workers,
                )
                logger.info("training phraseqa complete")
            nosql_db.update_saved_search_status(ids, "trained")
        except Exception as e:
            logger.exception("training failed", e)
            nosql_db.update_saved_search_status(ids, "failed")


def active_learning_on_field_value(
    user,
    token_info,
    field_id=None,
    field_bundle_id=None,
    workspace_id=None,
    doc_id=None,
    update_workers=False,
    body=None,
):
    user_obj = token_info.get("user_obj", None)
    if not user_obj or not user_obj.get("is_admin", False):
        logger.info(
            f"{user} not authorized to retrieve samples",
        )
        return make_response(
            jsonify({"status": "fail", "reason": "unauthorized"}),
            403,
        )
    query = {}
    if workspace_id:
        query["workspace_idx"] = workspace_id
    elif doc_id:
        query["file_idx"] = doc_id
    else:
        return err_response("Missing parameter for docId or workspaceId", 400)

    fields = {}
    if field_bundle_id:
        query["field_bundle_idx"] = field_bundle_id
        for field in nosql_db.get_fields_in_bundle(field_bundle_id):
            fields[field.id] = field
    elif field_id:
        query["field_idx"] = field_id
        fields[field_id] = nosql_db.get_field_by_id(field_id)
    else:
        return err_response("Missing parameter for fieldId or fieldBundleId", 400)

    query["top_fact.type"] = "approve"

    field_values = nosql_db.read_extracted_field(
        query,
        projection={"top_fact": 1, "field_idx": 1},
    )

    samples = {
        "qnli": {"sentences": [], "questions": [], "labels": []},
        "squad": {"sentences": [], "questions": [], "answers": []},
    }

    num_samples = 0
    for field_value in field_values:
        # table is not trainable
        if field_value["top_fact"].get("block_type", "") == "table":
            continue
        field = fields[field_value["field_idx"]]
        for criteria in field["search_criteria"]["criterias"]:
            if criteria["question"]:
                num_samples += 1
                # qnli
                samples["qnli"]["questions"].append(criteria["question"])
                samples["qnli"]["sentences"].append(field_value["top_fact"]["phrase"])
                samples["qnli"]["labels"].append("entailment")

                # squad
                samples["squad"]["questions"].append(criteria["question"])
                samples["squad"]["sentences"].append(field_value["top_fact"]["phrase"])
                samples["squad"]["answers"].append(field_value["top_fact"]["answer"])

    if num_samples > 0:
        qnli_client.active_learning(
            questions=samples["qnli"]["questions"],
            sentences=samples["qnli"]["sentences"],
            labels=samples["qnli"]["labels"],
            update_workers=update_workers,
        )

        # TODO define squad client
        # squad_client.active_learning(
        #     questions=samples["squad"]["questions"],
        #     sentences=samples["squad"]["sentences"],
        #     answers=samples["squad"]["answers"],
        #     update_workers=update_workers,
        # )
    return make_response(
        jsonify(
            {
                "learned_count": num_samples,
                "update_workers": update_workers,
            },
        ),
        200,
    )


def active_learning_yolo(
    user,
    token_info,
    update_workers=False,
):
    user_obj = token_info.get("user_obj", None)
    if not user_obj or not user_obj.get("is_admin", False):
        logger.error(
            f"{user} not authorized to retrieve samples",
        )
        return make_response(
            jsonify({"status": "fail", "reason": "unauthorized"}),
            404,
        )
    active_learn_samples, num_samples = nosql_db.get_yolo_samples(split="new")

    num_samples = len(active_learn_samples)
    if num_samples > 0:
        yolo_client.active_learning(active_learn_samples)

    test_samples, _ = nosql_db.get_yolo_samples(split="test")
    # nosql_db.update_yolo_samples(active_learn_samples, "training")

    # nosql_db.update_yolo_samples(active_learn_samples, "trained")

    yolo_client.get_accuracy(active_learn_samples)
    yolo_client.get_accuracy(test_samples)

    return make_response(
        jsonify(
            {
                "learned_count": num_samples,
                "update_workers": update_workers,
            },
        ),
        200,
    )
