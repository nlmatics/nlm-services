import json
import logging
import os
import re
import tempfile
import traceback

from nlm_ingestor.ingestor_utils.utils import NpEncoder
from nlm_utils.storage import file_storage
from server.controllers.extraction_controller import apply_template
from server.storage import nosql_db
from timeit import default_timer

from nlm_utils.rabbitmq import producer
from nlm_utils.model_client import YoloClient
import nlm_ingestor.ingestion_daemon.config as cfg
from server.utils.indexer_utils.request import DocumentInfo
from server.utils.indexer_utils.misc_utils import blocks_to_sents
from server.utils.indexer_utils.misc_utils import ingest_data_row_file
from server.utils.indexer_utils.bbox_detector import BBOXDetector
from server.utils.indexer_utils.es_client import es_client
from server.utils.indexer_utils.info_extractor import extract_key_data
from nlm_utils.utils import ensure_bool, file_utils
from bs4 import BeautifulSoup
from nlm_ingestor.ingestor import ingestor_api
import numpy as np

# initialize logging
logger = logging.getLogger(__name__)
logger.setLevel(cfg.log_level())
run_table_detection: bool = ensure_bool(os.getenv("RUN_TABLE_DETECTION", False))
title_text_only_pattern = re.compile(r"[^a-zA-Z]+")
title_delimiter_remove_pattern = re.compile(r"[.;'\"\-,\n\r]")


def process_message(message):
    logger.info(f"message received: {message}")
    try:
        msg_str = message.data.decode("utf-8")
        doc = DocumentInfo(**json.loads(msg_str))
        ingest_document(doc)
        message.ack()
        logger.info("Processing complete. Message ack'ed")
    except Exception:
        logger.error(
            f"error processing ingestion request, err: {traceback.format_exc()}",
        )
        if message:
            message.nack()
            logger.info("Message nack'ed")

def index_data_row_file(
    doc_id,
    data_row_file_info,
):
    database = nosql_db
    objectstore = file_storage
    if data_row_file_info.title:
        data_row_file_info.title = data_row_file_info.title.replace("\n", "")
    logger.info(f"Ingesting row file {doc_id} with title {data_row_file_info.title}")
    database.set_document_status(doc_id, "ingest_inprogress", None)
    blocks, _, _, file_data, result = ingest_data_row_file(
        data_row_file_info,
    )
    database.delete_document_blocks(doc_id)
    rendered_file_location = objectstore.save_file_data(doc_id, json.dumps(result[0], cls=NpEncoder))
    file_json_data = json.dumps(result[1], cls=NpEncoder)
    rendered_json_file_location = file_storage.save_file_data(f"{doc_id}_json", file_json_data)

    texts, infos, _, _, doc_ent_dict = es_client.add_to_index(doc_id, data_row_file_info.blocks)

    summaries, kv_pairs, reference_definitions = extract_key_data(texts, infos)
    metadata = {
        "title": data_row_file_info.title,
        "inferred_title": data_row_file_info.title,
        "inferred_subtitle": "",
        "first_level_font": "",
        "second_level_font": "",
        "third_level_font": "",
    }
    try:
        database.save_document_key_info(
            doc_id=doc_id,
            key_info=dict(
                section_summary=summaries,
                key_value_pairs=kv_pairs,
                metadata=metadata,
                doc_ent=doc_ent_dict,
                reference_definitions=reference_definitions,
            ),
        )
    except Exception:
        database.save_document_key_info(
            doc_id=doc_id,
            key_info=dict(
                section_summary=summaries,
                key_value_pairs=kv_pairs,
                metadata=metadata,
                doc_ent=doc_ent_dict,
            ),
        )

    database.set_document_status(
        doc_id,
        "ingest_ok",
        data_row_file_info.title,
        data_row_file_info.title,
        rendered_file_location,
        rendered_json_file_location
    )


def index_blocks(
        doc: DocumentInfo,
        ingestor: object,
        mime_type=None,
        parse_and_render_only: bool = False
):
    database = nosql_db
    objectstore = file_storage
    database.set_document_status(doc.id, "ingest_inprogress", None)
    database.delete_document_blocks(doc.id)
    # database.save_document_blocks(doc_id, blocks)
    num_pages = 0
    file_json_data = {"title": "", "document": ""}

    if not hasattr(ingestor, "file_data"):#when not a pdf
        file_data = {
            "title": "",
            "text": ingestor.html_str,
        }

        file_data = json.dumps(file_data)

        rendered_file_location = file_storage.save_file_data(doc.id, file_data)
        title = doc.name if doc.name else ""
        inferred_title = title
        file_json_data = {
            "title": title,
            "document": ingestor.json_dict,
        }
        metadata = {
            "title": title,
            "inferred_title": inferred_title
        }
        file_json_data = json.dumps(file_json_data, cls=NpEncoder)
        rendered_json_file_location = file_storage.save_file_data(f"{doc.id}_json", file_json_data)
    else:#when a pdf
        if len(ingestor.file_data) > 1:
            rendered_file_location = file_storage.save_file_data(doc.id, ingestor.file_data[0])
            file_json_data = ingestor.file_data[1]
        else:
            file_json_data = ingestor.file_data[0]
        # print("file_json_data", file_json_data)
        rendered_json_file_location = file_storage.save_file_data(f"{doc.id}_json", file_json_data)
   

    # no blocks found, skipping
    if not ingestor.blocks:
        # If no blocks found, set status to failed
        logger.info(f"No blocks found for: {doc.id}. Setting status to ingest_failed.")
        nosql_db.set_document_status(
            doc.id,
            "ingest_failed",
            num_pages=num_pages,
        )
        return 0
    if mime_type == "application/pdf":
        # Metadata calculation.
        doc_result_json = ingestor.doc_result_json
        # print("doc_result_json", ingestor.doc_metadata)
        first_level_data = doc_result_json["title_page_fonts"]["first_level"]
        # Remove words with numbers and filter out the empty string.
        inferred_title = " ".join(
            filter(None, (
                title_text_only_pattern.sub("", word).strip()
                if not len(title_text_only_pattern.sub("", word).strip()) else word
                for data in first_level_data for word in data.split())
                )
        )
        # Remove any occurrences of the delimiter.
        inferred_title = title_delimiter_remove_pattern.sub("", inferred_title)
        if doc_result_json["title"]:
            doc_result_json["title"] = doc_result_json["title"].replace("\n", "")

        inferred_subtitle = ""
        first_level_font = ""
        second_level_font = ""
        third_level_font = ""
        if "first_level_sub" in doc_result_json["title_page_fonts"]:
            inferred_subtitle = " ".join(doc_result_json["title_page_fonts"]["first_level_sub"])
        # TODO: Below snippet for calculation of font is wrong.
        #  result[0]["title_page_fonts"]["first_level"] ===> The actual text, not the font
        if "first_level" in doc_result_json["title_page_fonts"]:
            first_level_font = doc_result_json["title_page_fonts"]["first_level"]

        if "second_level" in doc_result_json["title_page_fonts"]:
            second_level_font = doc_result_json["title_page_fonts"]["second_level"]

        if "third_level" in doc_result_json["title_page_fonts"]:
            third_level_font = doc_result_json["title_page_fonts"]["third_level"]

        metadata = {
            "title": doc_result_json["title"],
            "inferred_title": inferred_title,
            "inferred_subtitle": inferred_subtitle,
            "first_level_font": first_level_font,
            "second_level_font": second_level_font,
            "third_level_font": third_level_font,
            "page_dim": ingestor.return_dict["page_dim"],
        }
        num_pages = ingestor.return_dict["num_pages"]
    
    if not parse_and_render_only:
        # BBOXDetector
        bbox_detector = None
        if mime_type == "text/tika_html":
            bbox_detector = BBOXDetector(doc.id, tika_html=file_loc, blocks=ingestor.blocks)

        # add blocks to elasticsearch
        texts, infos, _, _, doc_ent_dict = es_client.add_to_index(doc.id,
                                                                  ingestor.blocks,
                                                                  num_pages + 1,  # num_pages are starting @ 0
                                                                  bbox=bbox_detector.bboxes if bbox_detector else {})
        # repeating again due to some indexing issues - hack
        texts, infos = blocks_to_sents(ingestor.blocks)

        if bbox_detector:
            # save bbox features in json
            tmpfile_handler, tmpfile_name = tempfile.mkstemp()
            os.close(tmpfile_handler)

            with open(tmpfile_name, "w") as f:
                json.dump(bbox_detector.convert_tika_html_to_features(), f)

            file_storage.upload_document(tmpfile_name, f"bbox/features/{doc.id}.json")

            summaries, kv_pairs, reference_definitions = extract_key_data(texts, infos, bbox_detector.bboxes)
        else:
            summaries, kv_pairs, reference_definitions = extract_key_data(texts, infos)
        try:
            database.save_document_key_info(
                doc_id=doc.id,
                key_info=dict(
                    section_summary=summaries,
                    key_value_pairs=kv_pairs,
                    metadata=metadata,
                    doc_ent=doc_ent_dict,
                    reference_definitions=reference_definitions,
                ),
            )
        except Exception:
            database.save_document_key_info(
                doc_id=doc.id,
                key_info=dict(
                    section_summary=summaries,
                    key_value_pairs=kv_pairs,
                    metadata=metadata,
                    doc_ent=doc_ent_dict,
                ),
            )
    # print("file_json_data", file_json_data)
    database.set_document_status(
        doc.id,
        title=json.loads(file_json_data)["title"],
        inferred_title=inferred_title,
        rendered_file_location=rendered_file_location,
        rendered_json_file_location=rendered_json_file_location,
        num_pages=num_pages + 1,
    )

    return num_pages + 1


# add synonym dictionary to ES index
def add_synonym_dictionary(synonym_dictionary, workspace_idx=None):
    es_client.add_synonym_dictionary_to_index(
        synonym_dictionary,
        workspace_idx=workspace_idx,
    )


# Update document ACL from metadata
def update_document_meta(workspace_idx, file_idxs=None):
    es_client.update_document_meta(
        workspace_idx=workspace_idx,
        file_idxs=file_idxs,
    )




def ingest_document(doc: DocumentInfo,
                    rerun_extraction: bool = True,
                    parse_options: dict = None,
                    user_profile: dict = None,
                    re_ingest: bool = False,
                    ):
    logger.info("Ingestion started")
    print("parse options", parse_options)
    doc_id = doc.id
    mime_type = doc.mime_type
    file_to_ingest = None
    doc_location = doc.doc_location
    doc_name = doc.name
    ingestion_except = False
    ingestion_failure = False
    num_pages = 1
    # update = doc.update
    """
    # Read parse_options (developer invocation) when using PDF parsing only
        {
            "parse_and_render_only": True,
            "render_format": "all" or "html" or "json",
            "parse_pages": (start_page, end_page)
        }
    """
    parse_and_render_only = parse_options.get("parse_and_render_only", False) \
        if parse_options else False
    render_format = parse_options.get("render_format", "html") \
        if parse_options else "all"
    parse_pages = parse_options.get("parse_pages", False) \
        if parse_options else ()
    use_new_indent_parser = parse_options.get('useNewIndentParser', 'no')
    apply_ocr = parse_options.get('applyOcr', 'no')

    try:
        nosql_db.set_document_status(doc.id, "ingest_inprogress", None)
        nosql_db.delete_document_blocks(doc_id)
        parse_options = {
            "parse_and_render_only": True,
            "render_format": render_format,
            "use_new_indent_parser": use_new_indent_parser == "yes",
            "parse_pages": (),
            "apply_ocr": apply_ocr == "yes"
        }
        file_to_ingest = file_storage.download_document(doc_location)
        file_props = file_utils.extract_file_properties(file_to_ingest)
        ingest_mime_type = file_props["mimeType"]
        logger.info(f"Parsing document: {doc_name}")


        _, ingestor = ingestor_api.ingest_document(
            doc_name,
            file_to_ingest,
            ingest_mime_type,
            parse_options=parse_options,
        )

        num_pages = index_blocks(
            doc=doc,
            ingestor=ingestor,
            mime_type=ingest_mime_type,
            parse_and_render_only=parse_and_render_only,
        )

        if not num_pages:
            ingestion_failure = True
            rerun_extraction = False

    except Exception:
        logger.info(f"error running ingestion, stacktrace: {traceback.format_exc()}")
        nosql_db.set_document_status(
            doc_id,
            "ingest_failed",
            num_pages=0,
        )
        ingestion_except = True
    finally:
        if file_to_ingest and os.path.exists(file_to_ingest):
            os.unlink(file_to_ingest)
            logger.info(f"File {file_to_ingest} deleted")

    if not ingestion_except and not ingestion_failure:
        try:
            if rerun_extraction:
                for field_bundle in nosql_db.get_field_bundles_in_workspace(
                    doc.workspace_id,
                ):
                    if len(field_bundle.field_ids):
                        try:
                            apply_template(
                                file_idx=doc_id,
                                field_bundle_idx=field_bundle.id,
                                override_topic="ALL",
                            )
                            if not re_ingest:
                                nosql_db.update_fields_status_from_ingestor(field_bundle.field_ids)

                        except Exception:
                            logger.error(
                                f"error running extraction, stacktrace: {traceback.format_exc()}",
                            )
                            continue
            nosql_db.set_document_status(
                doc_id,
                "ingest_ok",
                num_pages=num_pages,
            )
            # Update the usage metrics here.
            if num_pages and not re_ingest and user_profile:
                if not parse_and_render_only:
                    res_data = {
                        "num_pages": num_pages,
                        "num_docs": 1,
                        "doc_size": doc.file_size,
                    }
                else:
                    res_data = {
                        "pdf_parser_pages": num_pages,
                    }
                if user_profile.get("m2m_email", None):
                    data = {
                        "dev_api_usage": res_data,
                    }
                else:
                    data = {
                        "general_usage": res_data,
                    }
                nosql_db.upsert_usage_metrics(user_profile["id"], data)
            return doc
        except Exception:
            logger.info("error running extraction")
            nosql_db.set_document_status(
                doc_id,
                "ingest_failed",
            )
            return "ingest_failed"
    else:
        return "ingest_failed"


def run_yolo_inference(user_id, document_id):
    task_body = {
        "doc_id": document_id,
    }

    task = nosql_db.insert_task(
        user_id,
        "yolo",
        task_body,
    )
    res = producer.send(task)
    if res:
        logger.info("Yolo inference task queued")
    else:
        nosql_db.delete_task(task["_id"])

        try:
            yolo = YoloClient(url=os.getenv("IMAGE_MODEL_SERVER_URL"))
            pages = yolo(task_body["doc_id"])

            nosql_db.save_inference_doc(task_body["doc_id"], pages)
            logger.info("yolo inference saved in the database")

        except Exception as e:
            logger.error(e)
            logger.error("Yolo inference task not saved")