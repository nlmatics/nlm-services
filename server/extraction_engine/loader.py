import logging
import os
from typing import Tuple

import pandas as pd
from nlm_utils.storage import file_storage

from .services import Loader
from server.storage.nosql_db import NoSqlDb
from server.utils import str_utils

# import server.config as cfg

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def read_template(fname):
    result = []
    templates = []
    queries = []
    headers = []
    answer_formats = []
    topic = None
    group = ""
    topic_list = []
    duplicate_topic = False
    with open(fname, encoding="utf8", errors="ignore") as f:
        content = f.readlines()
        for line in content[1:]:
            line = line.replace("\n", "")
            line = line.strip()
            if not line == "":
                if line.startswith("[["):
                    group = line[2:-2]
                elif line.startswith("["):
                    if topic and topic not in topic_list:
                        topic_list.append(topic)
                        result.append(
                            {
                                "topic": topic,
                                "topicId": topic,
                                "group": group,
                                "templates": templates,
                                "queries": queries,
                                "headers": headers,
                                "answer_formats": answer_formats,
                            },
                        )
                    if line[1:-1] in topic_list:
                        duplicate_topic = True
                        continue

                    else:
                        duplicate_topic = False
                        topic = line[1:-1]
                        templates = []
                        queries = []
                        headers = []
                        answer_formats = []

                elif line.startswith("??") and not duplicate_topic:
                    queries.append(line[2:].replace("\n", ""))
                elif line.startswith("^^") and not duplicate_topic:
                    headers.append(line[2:].replace("\n", ""))
                elif line.startswith("->") and not duplicate_topic:
                    answer_formats.append(line[2:].replace("\n", ""))
                elif not line.startswith("#--") and not duplicate_topic:
                    templates.append(line)

    if topic not in topic_list and not duplicate_topic:
        result.append(
            {
                "topic": topic,
                "topicId": topic,
                "group": group,
                "templates": templates,
                "queries": queries,
                "headers": headers,
                "answer_formats": answer_formats,
            },
        )

    return result


def _rows_to_df(result):
    topics = []
    topicIds = []
    templates = []
    queries = []
    groups = []
    headers = []
    post_processors = []
    ignore_blocks = []

    for item in result:
        topics.append(item["topic"])
        topicIds.append(item["topicId"])
        groups.append(item["group"])
        query = item["queries"][0] if len(item["queries"]) > 0 else None
        template = item["templates"]

        correct_template, correct_query = template, [query]
        correct_template = correct_template or ""
        template_str = "|||".join(correct_template)
        templates.append(template_str)

        queries.append(correct_query[0])

        # skip field with empty template and question
        # if len([x for x in templates if x]) == 0 and query is None:
        #     continue

        if len(item["headers"]) > 0:
            headers.append(item["headers"][0])
        else:
            headers.append(None)

        if "post_processors" in item:
            if len(item["post_processors"]) > 0:
                post_processors.append(item["post_processors"])
            else:
                post_processors.append(None)
        else:
            post_processors.append(None)

        if "ignore_blocks" in item:
            if len(item["ignore_blocks"]) > 0:
                ignore_blocks.append(item["ignore_blocks"])
            else:
                ignore_blocks.append(None)
        else:
            ignore_blocks.append(None)

    template_df = pd.DataFrame(
        {
            "topic": topics,
            "topicId": topicIds,
            "group": groups,
            "template": templates,
            "question": queries,
            "header": headers,
            "post_processors": post_processors,
            "ignore_blocks": ignore_blocks,
        },
    )

    # print(template_df)
    return template_df


class ContentLoader(Loader):
    def __init__(self, storage: file_storage, db: NoSqlDb):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self._storage = storage
        self._db = db

    def load_settings(self, workspace_idx):
        workspace = self._db.get_workspace_by_id(workspace_idx)
        if workspace:
            return workspace.settings

    # def get_ignore_blocks(self, workspace_id):
    #     results = []

    #     for ignore_block in self._db.get_ignore_blocks(workspace_id):

    #         ignore_block_info = {
    #             # "templates": None,
    #             # "id": ignore_block.id,
    #             "ignore_blocks": ignore_block.ignore_text,
    #             "ignoreAllAfter": ignore_block.ignore_all_after,
    #             # "workspaceId": ignore_block.workspace_id,
    #             "blockType": ignore_block.block_type,
    #         }
    #         results.append(ignore_block_info)
    #     return results

    def read_fieldbundle(self, field_bundle_id: str):
        bundle_name, bundle_location, workspace_id = self.resolve_field_bundle(
            field_bundle_id,
        )
        local_bundlefile_location = None
        try:
            if bundle_location:
                # bundlefile_location = self._db.find_bundlefile_storage_location(field_bundle_id)
                local_bundlefile_location = self._storage.resolve_location(
                    bundle_location,
                )
                result = read_template(local_bundlefile_location)
                return _rows_to_df(result)
            else:
                fields = self._db.get_fields_in_bundle(field_bundle_id)
                # ignore_blocks = self._db.get_ignore_blocks(workspace_id)  # TODO

                result = []
                for field in fields:
                    templates = field.patterns
                    questions = [field.question]
                    headers = [field.section_heading]
                    # self.logger.info(field)
                    topic_info = {
                        "topic": field.name,
                        "topicId": field.id,
                        "group": "N/A",
                        "templates": templates,
                        "queries": questions,
                        "headers": headers,
                        "post_processors": [field.answer_format]
                        if field.answer_format
                        else [],
                        # "ignore_blocks": [],
                    }
                    # self.logger.info(f"field: {field}")
                    # Appends the ignoreBlock texts to the template result

                    # for ignore_block in ignore_blocks:
                    #     ignore_block_info = {
                    #         # "templates": None,
                    #         # "id": ignore_block.id,
                    #         "ignore_blocks": ignore_block.ignore_text,
                    #         "ignoreAllAfter": ignore_block.ignore_all_after,
                    #         # "workspaceId": ignore_block.workspace_id,
                    #         "blockType": ignore_block.block_type,
                    #     }
                    #     topic_info["ignore_blocks"].append(ignore_block_info)

                    result.append(topic_info)
                return _rows_to_df(result)
        finally:
            if local_bundlefile_location and os.path.exists(local_bundlefile_location):
                os.unlink(local_bundlefile_location)

    def get_field_overrides(self, field_id: str, id_type: str):
        """
        Get user defined field overrides.
        Called at the end of the get_json pipeline in discovery_engine repo.

        Args:
            field_id (str): id value.
            id_type (str): Must be "workspace_id", "doc_id", or "field_id".

        Returns:
            overrides (List[FieldValue])
        """
        overrides = self._db.get_field_value_overrides(field_id, id_type)
        return overrides

    def is_folder(self, path: str):
        return False

    def load_document(self, doc_id):
        doc_info = self._db.get_document_info_by_id(doc_id)
        if doc_info:
            return self._storage.resolve_location(doc_info.doc_location)

    def load_document_info(self, doc_id):
        return self._db.get_document_info_by_id(doc_id)

    def load_workspace_info(self, workspace_id):
        return self._db.get_workspace_by_id(workspace_id)

    def get_contents_by_path(self, source_path):
        path_parts = source_path.split("/")
        if len(path_parts) == 1:
            workspace_id = path_parts[0]
            folder_id = "root"
        elif len(path_parts) == 2:
            workspace_id = path_parts[0]
            folder_id = path_parts[1]
        else:
            raise ValueError(f"source path {source_path} is not supported")
        docs = self._db.get_folder_contents(workspace_id, folder_id)["documents"]
        return docs

    def get_blocks(self, doc_id: str):
        return self._db.get_parsed_blocks_for_document(doc_id)

    def resolve_field_bundle(self, field_bundle_id: str) -> Tuple[str, str, str]:
        """Resolves the bundle name, temporary file location and workspace id
        for the given bundle_id.
        The caller is responsible for deleting the temporary file after use.
        :param field_bundle_id:
        :return: (bundle_name, temporary file with bundle contents, workspace id of bundle).
        """
        bundle_info = self._db.get_field_bundle_info(field_bundle_id)
        if bundle_info:
            return (
                bundle_info.bundle_name,
                bundle_info.cached_file,
                bundle_info.workspace_id,
            )
        else:
            raise ValueError(f"field bundle with id {field_bundle_id} not found")

    def update_search_history(self, user_id, workspace_id, doc_id, pattern, question):
        """
        Text/template/pattern are the same thing.
        """

        # Note: collapse this code into one line
        if pattern is None and question is None:
            return
        if pattern == "" and question == "":
            return
        if pattern is None and question == "":
            return
        if pattern == "" and question is None:
            return

        timestamp = str_utils.timestamp_as_str()
        uniq_id = str_utils.generate_search_history_id(timestamp)

        self._db.update_search_history(
            user_id,
            uniq_id=uniq_id,
            timestamp=timestamp,
            workspace_id=workspace_id,
            doc_id=doc_id,
            pattern=pattern,
            question=question,
        )
