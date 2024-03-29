import ahocorasick
import copy
import json
import logging
import os
import pickle
import requests
import socket
from collections import defaultdict
from timeit import default_timer

from bson.objectid import ObjectId
from elasticsearch import Elasticsearch, RequestError

from nlm_ingestor.ingestor_utils.ner_dict import NERDict, STOPWORDS_GENE
from nlm_utils.model_client import EncoderClient
from nlm_utils.model_client import NlpClient
from nlm_utils.storage import file_storage
from nlm_utils.utils import ensure_bool
from server.extraction_engine.loader import ContentLoader
from server.storage import nosql_db

from nlm_ingestor.ingestor import line_parser
from server.utils.indexer_utils.misc_utils import blocks_to_sents
from server.utils.indexer_utils.info_extractor import extract_key_data


from nlm_ingestor.ingestor.table_parser import TableParser
from nlm_ingestor.ingestor_utils.de_duplicate_engine import DeDuplicateEngine

from nlm_ingestor.ingestor_utils.utils import check_char_is_word_boundary

USE_NLM_BIO_NER_MODELS = ensure_bool(os.getenv("USE_NLM_BIO_NER_MODELS", True))
USE_BERN2_NER = ensure_bool(os.getenv("USE_BERN2_NER", False))
BERN2_SERVER_URL = os.getenv("BERN2_SERVER_URL")
# NER_DICTIONARIES = "/app/test.json /app/test1.json"
# Each JSON file will have data in the following order
"""
Sample Input Data
{
    'Abdominal Neoplasms': {
        'type': 'disease',
        'metadata': {
            'uuid': 'D000008',
            'derived_from': 'mesh',
            'tree_numbers': ['C04.588.033']
        }
    },
    'Abdominal Neoplasm': {
        'type': 'disease',
        'metadata': {
            'uuid': 'D000008',
            'derived_from': 'mesh',
            'tree_numbers': ['C04.588.033']
        }
    },
}
"""
NER_DICTIONARIES = os.getenv("NER_DICTIONARIES", "")

ner_dict = None
if NER_DICTIONARIES:
    ner_dict = NERDict()
    for dict_file in NER_DICTIONARIES.split():
        with open(dict_file) as read_file:
            ner_dict.create_ner_dict(json.load(read_file))


# from ingestor.processors import is_table_row
def get_analyzer(es_synonyms_list=None):
    # stopwords = [
    #     "a",
    #     "an",
    #     "and",
    #     "are",
    #     "as",
    #     "at",
    #     "be",
    #     "but",
    #     "by",
    #     "for",
    #     "if",
    #     "in",
    #     # "into",
    #     "is",
    #     "it",
    #     # "no",
    #     # "not",
    #     "of",
    #     "on",
    #     "or",
    #     "such",
    #     "that",
    #     "the",
    #     # "their",
    #     "then",
    #     "there",
    #     "these",
    #     "they",
    #     "this",
    #     "to",
    #     "was",
    #     "will",
    #     "with",
    #     "what",
    #     "who",
    #     "when",
    #     "where",
    #     "why",
    #     "which",
    #     "how",
    # ]
    stopwords = []

    analyzer = {
        "analyzer": {
            "nlm_text_analyzer": {
                "tokenizer": "standard",
                "filter": [
                    # lower case words
                    "lowercase",
                    # remove stopwords
                    "nlm_stopwords",
                    # stem words
                    "snowball",
                ],
                "char_filter": [
                    "remove_hyphen_between_numbers",
                    "split_on_dot",
                    "split_on_parens",
                    "remove_last_sp_char",
                ],
            },
            "nlm_keyword_analyzer": {"tokenizer": "whitespace"},
            "nlm_table_analyzer": {
                "tokenizer": "standard",
                "filter": [
                    # # remove stopwords
                    # "nlm_stopwords",
                    # lower case words
                    "lowercase",
                    # stem words
                    "snowball",
                ],
                "char_filter": [
                    "remove_hyphen_between_numbers",
                    "split_on_dot",
                    "split_on_parens",
                    "remove_last_sp_char",
                ],
            },
            "nlm_key_value_analyzer": {
                "tokenizer": "keyword",
                "filter": [
                    # lower case words
                    "lowercase",
                ],
            }
        },
        "filter": {
            "nlm_stopwords": {
                "type": "stop",
                "ignore_case": True,
                "stopwords": stopwords,
            },
        },
        "char_filter": {
            # concat "non-current" => "noncurrent"
            "remove_hyphen_between_numbers": {
                "type": "pattern_replace",
                "pattern": "(\\w+)-(?=\\w)",    # "(?=\\w)" ==> Non capturing group.
                "replacement": "$1",
            },
            # split "WSM.N" => "WSM" "N"
            "split_on_dot": {
                "type": "pattern_replace",
                "pattern": r"([a-zA-Z0-9]+)\.([a-zA-Z]+)",
                "replacement": "$1 $2",
            },
            # split "Margin(1)" => "Margin" "(1)"
            "split_on_parens": {
                "type": "pattern_replace",
                "pattern": r"([a-zA-Z0-9]+)(\([a-zA-Z0-9]\)+)",
                "replacement": "$1 $2",
            },
            # "counsel:"  => "counsel"
            # "counsel:123"  => "counsel:123"
            "remove_last_sp_char": {
                "type": "pattern_replace",
                "pattern": "(\\w+)[;:](?!\\w)",
                "replacement": "$1",
            }
        },
    }

    if es_synonyms_list:
        analyzer["analyzer"]["nlm_text_analyzer"]["filter"].append("customized_synonym")
        analyzer["filter"]["customized_synonym"] = {
            "type": "synonym",
            "lenient": True,  # skip bad synonyms list
            "synonyms": es_synonyms_list,
        }

    return analyzer


# define similarity
def get_similarity():
    score_function = """return query.boost * 1000"""

    return {
        "nlm_sentence_similarity": {
            "type": "scripted",
            "script": {
                "source": score_function,
            },
        },
        # "nlm_sentence_similarity": {
        #     "type": "BM25",
        #     # # default
        #     # "k1": 1.2,
        #     # "b": 0.75,
        #     # MSMARCO: Optimized for MRR@10/MAP
        #     "k1": 0.60,
        #     "b": 0.62,
        #     # # MSMARCO Optimized for recall@1000
        #     # "k1": 0.82,
        #     # "b":0.68,
        #     "discount_overlaps": True,
        # },
        "nlm_keyword_similarity": {
            "type": "BM25",
            "k1": 1.2,
            "b": 0,
            # "k1": 3.8,
            # "b": 0.87,
            "discount_overlaps": True,
        },
        "nlm_table_similarity": {
            "type": "BM25",
            "k1": 1.2,
            "b": 0.1,
            # "k1": 3.8,
            # "b": 0.87,
            "discount_overlaps": True,
        },
        "nlm_document_similarity": {
            "type": "BM25",
            # # default
            # "k1": 1.2,
            # "b": 0.75,
            # # MSMARCO: Default (k1=0.9, b=0.4)
            "k1": 0.9,
            "b": 0.4,
            #  MSMARCO: Optimized for MRR@100/MAP
            # "k1": 3.8,
            # "b":0.87,
            "discount_overlaps": True,
        },
    }


def get_index_settings(
        level,
        creating=True,
        es_synonyms_list=None,
        workspace_settings=None,
):
    workspace_settings = workspace_settings or {}
    settings = {
        "index": {
            # Disable auto refresh
            # "refresh_interval": -1,
            "number_of_shards": workspace_settings.get("index_settings", {}).get("number_of_shards", 1),
            "number_of_replicas": 0,
            "analysis": get_analyzer(es_synonyms_list),
            "similarity": get_similarity(),
        },
    }
    if level == "file":
        settings["index"]["highlight.max_analyzed_offset"] = 1_000_000_000

    if creating:
        settings["index"]["refresh_interval"] = -1
    return settings


def get_index_mappings(
        level,
        workspace_settings=None,
):
    workspace_settings = workspace_settings or {}
    if level == "file":
        return {
            "properties": {
                # attribute
                "id": {"type": "keyword"},
                "file_idx": {"type": "keyword"},
                "file_name": {
                    "type": "text",
                    "analyzer": "nlm_text_analyzer",
                    "similarity": "nlm_keyword_similarity",
                },
                # text
                "title_text": {
                    "type": "text",
                    "analyzer": "nlm_text_analyzer",
                    "similarity": "nlm_sentence_similarity",
                },
                "match_text": {
                    "type": "text",
                    "analyzer": "nlm_text_analyzer",
                    "similarity": "nlm_document_similarity",
                },
                "header_text": {
                    "type": "text",
                    "analyzer": "nlm_text_analyzer",
                    "similarity": "nlm_document_similarity",
                },
            },
        }
    else:
        index_mappings = {
            "properties": {
                # attribute
                "id": {"type": "keyword"},
                "match_idx": {"type": "long"},
                "file_idx": {"type": "keyword"},
                "file_name": {
                    "type": "text",
                    "analyzer": "nlm_text_analyzer",
                    "similarity": "nlm_keyword_similarity",
                },
                # text
                "match_text": {
                    "type": "text",
                    "analyzer": "nlm_text_analyzer",
                    "similarity": "nlm_sentence_similarity",
                },
                "block_text": {
                    "type": "text",
                    "analyzer": "nlm_text_analyzer",
                    "similarity": "nlm_sentence_similarity",
                    # "index_options": "docs"
                },
                "header_text": {
                    "type": "text",
                    "analyzer": "nlm_text_analyzer",
                    "similarity": "nlm_sentence_similarity",
                    "fields": {
                        "autocomplete": {
                            "type": "completion"
                        }
                    }
                    # "index_options": "docs"
                },
                "header_chain_text": {
                    "type": "text",
                    "analyzer": "nlm_text_analyzer",
                    "similarity": "nlm_sentence_similarity",
                    # "index_options": "docs"
                },
                "parent_text": {
                    "type": "text",
                    "analyzer": "nlm_text_analyzer",
                    "similarity": "nlm_sentence_similarity",
                },
                # metadata
                # "is_table_row": {"type": "boolean"},
                # "level": {"type": "keyword"},
                # "header_match_idx": {"type": "long"},
                "block_type": {"type": "keyword"},
                "page_idx": {"type": "long"},
                "reverse_page_idx": {"type": "short"},
                "block_idx": {"type": "long"},
                "table_idx": {"type": "long"},
                "child_idxs": {"type": "keyword"},
                "entity_types": {
                    "type": "text",
                    "analyzer": "nlm_keyword_analyzer",
                    "similarity": "nlm_sentence_similarity",
                },
                # "is_duplicated": {"type": "boolean"},
                # embeddings
                "embeddings.sif.match": {"type": "dense_vector", "dims": 300},
                "embeddings.sif.header": {"type": "dense_vector", "dims": 300},
                "embeddings.dpr.match": {"type": "dense_vector", "dims": 768},
                # # DPR phrase embeddings
                # "dpr_embeddings": {
                #     "type": "nested",
                #     "properties": {
                #         "dpr": {"type": "dense_vector", "dims": 768 },
                #     }
                # },
                # tables
                "table": {
                    "type": "nested",
                    "properties": {
                        "index": {
                            "type": "nested",
                            "properties": {
                                "text": {
                                    "type": "text",
                                    "analyzer": "nlm_table_analyzer",
                                    "similarity": "nlm_sentence_similarity",
                                },
                            },
                        },
                        "index_text": {
                            "type": "text",
                            "analyzer": "nlm_text_analyzer",
                            "similarity": "nlm_sentence_similarity",
                        },
                        "text": {
                            "type": "text",
                            "analyzer": "nlm_text_analyzer",
                            "similarity": "nlm_sentence_similarity",
                        },
                        "type": {"type": "keyword"},
                        "idx": {"type": "keyword"},
                    },
                },
                "key_values": {
                    "type": "text",
                    "analyzer": "nlm_key_value_analyzer",
                }
            },
        }
        if workspace_settings.get("index_settings", {}).get("index", ""):
            index_mappings["properties"]["workspace_idx"] = {"type": "keyword"}
        if workspace_settings.get("search_settings", {}).get("debug_search", False):
            index_mappings["properties"]["full_text"] = {
                "type": "text",
                "analyzer": "nlm_text_analyzer",
                "similarity": "nlm_sentence_similarity",
            }

        return index_mappings


class ElasticsearchClient:
    def __init__(self, url=None, secret=None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

        if not url:
            url = os.getenv("ES_URL", "")

        if not secret:
            secret = os.getenv("ES_SECRET", "")

        if url:
            if secret:
                self.client = Elasticsearch(
                    [url],
                    http_auth=("elastic", secret),
                    timeout=3600,
                    http_compress=True,
                )
            else:
                self.client = Elasticsearch(
                    [url],
                    timeout=3600,
                    http_compress=True,
                )

            self.sif_encoder = EncoderClient(
                model="sif",
                url=os.getenv("MODEL_SERVER_URL"),
            )

            self.dpr_encoder = EncoderClient(
                model="dpr-context",
                url=os.getenv("DPR_MODEL_SERVER_URL",
                              os.getenv("MODEL_SERVER_URL")),
                normalization=True,
                dummy_number=False,
                lower=True,
                retry=1,
                use_msgpack=True,
            )

            self.nlp_client = NlpClient(
                url=os.getenv("MODEL_SERVER_URL"),
            )

            self.bio_nlp_client = NlpClient(
                url=os.getenv("BIO_MODEL_SERVER_URL"),
                model="bio_ner/tag",
            )

            self.file_level_suffix = "_file_level"
            self.loader = ContentLoader(file_storage, nosql_db)

            self.use_dpr = ensure_bool(os.getenv("USE_DPR", False)) or ensure_bool(os.getenv("INDEX_DPR", False))
            self.use_qatype = ensure_bool(os.getenv("USE_QATYPE", False)) or ensure_bool(os.getenv("INDEX_QATYPE", False))

    def create_index(
        self,
        workspace_idx,
        workspace_settings={},
    ):
        try:
            index = workspace_idx
            if workspace_settings:
                index = workspace_settings.get("index_settings", {}).get("index", workspace_idx)

            # block level index
            if not self.client.indices.exists(index):
                # get block level index settings
                block_level_index_settings = {
                    "settings": get_index_settings("block", workspace_settings=workspace_settings),
                    "mappings": get_index_mappings("block", workspace_settings=workspace_settings),
                }
                # create block level index
                self.client.indices.create(
                    index,
                    body=block_level_index_settings,
                )

                if "private_dictionary" in workspace_settings:
                    self.add_synonym_dictionary_to_index(
                        workspace_settings["private_dictionary"],
                        workspace_idx=workspace_idx,
                        workspace_settings=workspace_settings,
                    )

                self.client.indices.reload_search_analyzers(index=index)

            create_file_level_index = True
            if workspace_settings:
                create_file_level_index = workspace_settings.get("index_settings", {})\
                    .get("create_file_level_index", True)

            # file level index
            if create_file_level_index:
                file_level_index_id = index + self.file_level_suffix
                if not self.client.indices.exists(file_level_index_id):
                    file_level_index_settings = {
                        "settings": get_index_settings("file"),
                        "mappings": get_index_mappings("file"),
                    }

                    self.client.indices.create(
                        file_level_index_id,
                        body=file_level_index_settings,
                        ignore=[400],
                    )

                    if "private_dictionary" in workspace_settings:
                        self.add_synonym_dictionary_to_index(
                            workspace_settings["private_dictionary"],
                            workspace_idx=file_level_index_id,
                            file_level=True,
                        )

                    self.client.indices.reload_search_analyzers(file_level_index_id)

        except RequestError as e:
            if e.status_code == 400 and e.error == "resource_already_exists_exception":
                return
            else:
                raise e

    def delete_from_index(
            self,
            file_idx,
            workspace_idx,
            workspace_settings=None,
    ):
        self.logger.info(f"Deleting document {file_idx} from workspace {workspace_idx}")
        workspace_settings = workspace_settings or {}

        # delete blocks from block level index
        index = workspace_idx
        delete_body = {
            "query": {
                "term": {
                    "file_idx": file_idx
                }
            },
        }
        if workspace_settings:
            index = workspace_settings.get("index_settings", {}).get("index", workspace_idx)
            if index != workspace_idx:
                delete_body = {
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "term": {
                                        "workspace_idx": workspace_idx
                                    }
                                },
                                {
                                    "term": {
                                        "file_idx": file_idx
                                    }
                                },
                            ]
                        }
                    }
                }

        file_level_index_id = index + self.file_level_suffix

        self.client.delete_by_query(
            index=[index, file_level_index_id],
            body=delete_body,
            ignore=[404],
            timeout="3600s",
            wait_for_completion=False,
        )

        # refresh index
        self.client.indices.refresh(index=[index, file_level_index_id], ignore=[404])

        # delete blocks from db
        nosql_db.remove_es_entry(file_idx, workspace_idx)

    def add_blocks_to_index(
        self,
        workspace_idx,
        file_idx,
        document,
        blocks,
        num_pages=None,
        level="sent",
        ignore_block_settings=[],
        domain_settings="",
        bbox={},
        index_dpr=False,
        workspace_settings=None,
    ):

        wall_time = default_timer()

        workspace_settings = workspace_settings or {}
        es_index = workspace_idx
        if workspace_settings:
            es_index = workspace_settings.get("index_settings", {}).get("index", workspace_idx)

        automaton = ahocorasick.Automaton()
        reference_dict = {}

        if not num_pages:
            num_pages = 0
        de_duplicate_engine = DeDuplicateEngine(ignore_block_settings)

        # add document to index
        texts, infos = blocks_to_sents(blocks, flatten_merged_table=True)
        if self.use_qatype:
            _, kv_pairs, _ = extract_key_data(texts, infos, add_info=True, do_summaries=False)
        else:
            kv_pairs = []

        table_parser = TableParser(infos)

        sif_embs = self.sif_encoder(texts)["embeddings"]

        if self.use_dpr or index_dpr:
            dpr_embs = self.dpr_encoder(texts)["embeddings"]
            # dpr_embs = self.dpr_encoder(
            #     texts, headers=[info["header_text"] for info in infos]
            # )["embeddings"]

        # sif_block_embs = self.sif_encoder([x["block_text"] for x in blocks])[
        #     "embeddings"
        # ]

        query = []

        is_ignore_all_after = False

        matches = []

        # init objectid for each match
        match_idx2objcet_idx = defaultdict(lambda: ObjectId())

        # Perform NER on text

        def query_plain(texts, url=BERN2_SERVER_URL):
            return requests.post(url, json={'texts': texts}).json()

        file_ent_dict = {}
        extracted_ents = []
        if domain_settings != "biology":
            extracted_ents = self.nlp_client(texts=texts, option="get_doc_ents", domain=domain_settings)
        else:
            if not USE_BERN2_NER and USE_NLM_BIO_NER_MODELS:
                extracted_ents = self.bio_nlp_client(texts=texts, option="get_doc_ents", domain=domain_settings)
            if USE_BERN2_NER:
                entity_list = query_plain(texts)
                for entity_dict in entity_list:
                    new_list = []
                    for entity in entity_dict.get("annotations", []):
                        item = [entity["mention"], [entity["obj"]]]
                        if item not in new_list:
                            new_list.append(item)
                    extracted_ents.append(new_list)
            if ner_dict:
                if not extracted_ents:
                    extracted_ents = [[] for _ in range(len(texts))]
                for idx, text in enumerate(texts):
                    res = ner_dict.find_keys_in_text(text, STOPWORDS_GENE)
                    if res:
                        for r in res:
                            did_add = False
                            if extracted_ents[idx]:
                                for [t, type_list] in extracted_ents[idx]:
                                    if t == r['result']:
                                        for i, meta in enumerate(r['meta']):
                                            type_list.insert(i, meta['type'])
                                        did_add = True
                                        break
                            if not did_add:
                                extracted_ents[idx] = [[r['result'], [meta['type'] for meta in r['meta']]]]
        doc_ent_dict = {}
        add_to_match_idx = 0
        new_cell_texts = []
        new_cell_match_idxs = []
        for match_idx, (raw_text, info) in enumerate(
            zip(texts, infos),
        ):
            # get header_match_idx
            header_match_idx = info["header_match_idx"]
            block_type = info["block_type"]
            header_text = info["header_text"]
            block_text = info["block_text"]
            block_idx = info["block_idx"]
            level = info["level"]
            level_chain = info["level_chain"]

            # get entity list
            if len(extracted_ents) == len(texts):
                ent_list = extracted_ents[match_idx]
                if info.get('list_type', ''):
                    line = line_parser.Line(block_text).to_json()
                    start_number = line.get("start_number", "")
                    if start_number:
                        new_ent_list = []
                        for item in ent_list:
                            ee, et = item
                            if ee == start_number and et == ['NUM:count', 'NUM:code']:
                                continue
                            else:
                                new_ent_list.append(item)
                        ent_list = new_ent_list
            else:
                ent_list = []
            str_page_idx = str(info["page_idx"])
            if ent_list:
                if not doc_ent_dict.get(str_page_idx, None):
                    doc_ent_dict[str_page_idx] = copy.deepcopy(ent_list)
                    for item in ent_list:
                        ee, et = item
                        for i in et:
                            if not file_ent_dict.get(i, None):
                                file_ent_dict[i] = {}
                            if ee not in file_ent_dict[i].keys():
                                file_ent_dict[i][ee] = []
                            if str_page_idx not in file_ent_dict[i][ee]:
                                file_ent_dict[i][ee].append(str_page_idx)
                else:
                    for item in ent_list:
                        if item not in doc_ent_dict[str_page_idx]:
                            doc_ent_dict[str_page_idx].append(item)
                        ee, et = item
                        for i in et:
                            if not file_ent_dict.get(i, None):
                                file_ent_dict[i] = {}
                            if ee not in file_ent_dict[i].keys():
                                file_ent_dict[i][ee] = []
                            if str_page_idx not in file_ent_dict[i][ee]:
                                file_ent_dict[i][ee].append(str_page_idx)
            # skipping empty text
            if not raw_text:
                continue

            # skip table_rows for resolved tables
            # keep first table_row, which contains the DF table
            try:
                if match_idx > 0:
                    pre_info = infos[match_idx - 1]
                    if (
                        # keep first table_row
                        info["table_idx"] == pre_info["table_idx"]
                        # table must been parsed into df
                        and info["table_idx"] in table_parser.resolved_tables
                        # keep two column text table
                        and match_idx not in table_parser.two_column_table_idx
                    ):
                        continue

            except KeyError:
                pass

            de_duplicate_report = de_duplicate_engine.check_duplicate(
                {
                    "header": sif_embs[header_match_idx],
                    # "paragraph": sif_block_embs[block_idx],
                    "sentence": sif_embs[match_idx],
                },
            )

            # match one of the ignore level
            is_duplicated = de_duplicate_report["is_duplicated"]

            # update is_ignore_all_after
            if is_duplicated:
                if de_duplicate_report["ignore_all_after"]:
                    is_ignore_all_after = True

            # skip ignored match
            if is_ignore_all_after or is_duplicated:
                continue

            search_text = raw_text
            qa_text = ""

            # table been parsed into df
            if info.get("table_idx", -1) in table_parser.resolved_tables:
                # index two column text table
                if match_idx in table_parser.two_column_table_idx:
                    qa_text = raw_text = ": ".join(info["cell_values"])
                # search_text for df is empty
                else:
                    search_text = ""
            # expand table to kv pairs
            elif "header_cell_values" in info and "cell_values" in info:
                if len(info["cell_values"]) == len(info["header_cell_values"]):
                    qa_text = []
                    for k, v in zip(info["header_cell_values"], info["cell_values"]):
                        qa_text.append(f"{k}: {v} ")
                    search_text = qa_text = " ".join(qa_text)

            # build match data for search
            match = {
                # attribute
                "match_idx": match_idx + add_to_match_idx,
                "block_idx": block_idx,
                "file_idx": document.id,
                # text
                "match_text": search_text,
                "raw_text": raw_text,
                "qa_text": qa_text,
                "block_text": block_text,
                "header_text": header_text,
                "header_chain_text": " ".join([x["block_text"] for x in level_chain]),
                "parent_text": "",
                # metadata
                "page_idx": info["page_idx"],
                "reverse_page_idx": -(num_pages - info["page_idx"]),
                "block_type": block_type,
                # embeddings
                "embeddings": {
                    "sif": {
                        "match": sif_embs[match_idx],
                        "header": sif_embs[header_match_idx],
                    },
                },
                "child_idxs": [],
                "group_type": "single",
                "level": level,
                "level_chain": level_chain,
                "key_values": [],
                "entity_types": " ".join(
                    [" ".join(x[1]).replace(":", " ") for x in ent_list],
                ),
                "entity_list": ent_list,
            }
            if workspace_settings.get("search_settings", {}).get("debug_search", False):
                match["full_text"] = ""
                if match["header_chain_text"].strip():
                    match["full_text"] = match["header_chain_text"] + ": "
                match["full_text"] += match["match_text"]

            if es_index != workspace_idx:
                match["workspace_idx"] = workspace_idx

            if self.use_dpr or index_dpr:
                if block_type in {"para", "list_item", "table_row"}:
                    match["embeddings"]["dpr"] = {"match": dpr_embs[match_idx]}
                else:
                    match["embeddings"]["dpr"] = {"match": [0] * 768}

            for kv in kv_pairs:
                if kv["match_text"] == raw_text and kv["key"] not in match["key_values"]:
                    match["key_values"].append(kv["key"])
                if kv['key'] in reference_dict:
                    if kv['value'] not in reference_dict[kv['key']]:
                        reference_dict[kv['key']].append(kv['value'])
                else:
                    reference_dict[kv['key']] = [kv['value']]
            # Create Aho-Corasick Trie structure
            if reference_dict:
                for idx, (key, value) in enumerate(reference_dict.items()):
                    automaton.add_word(key, (idx, (key, value)))
                automaton.make_automaton()

            if "table_idx" in info:
                match["table_idx"] = info["table_idx"]

            # two column table should index as text
            if match_idx in table_parser.two_column_table_idx:
                self.logger.debug(
                    f"match {match_idx} is part of two column table, \n{match}",
                )
            # passed df table is indexed as table
            elif match_idx in table_parser.tables:
                self.logger.debug(f"indexing table in {match_idx}")
                df = table_parser.tables[match_idx]
                # check if multi-level
                match["table"], cell_texts = table_parser.create_es_index(df)
                match["table_data"] = pickle.dumps(df)
                match["block_type"] = "table"
                match["group_type"] = "table"

                table_extracted_ents = self.nlp_client(texts=cell_texts, option="get_doc_ents",
                                                       domain=domain_settings)

                for i, cell_text in enumerate(cell_texts):
                    new_match = copy.deepcopy(match)
                    new_match["block_type"] = "table_cell"
                    new_match["group_type"] = "table_cell"
                    add_to_match_idx += 1
                    new_match["match_idx"] = match_idx + add_to_match_idx
                    new_match["match_text"] = cell_text
                    new_match["block_text"] = cell_text
                    new_match["raw_text"] = cell_text
                    table_ent_list = table_extracted_ents[i] if i < len(table_extracted_ents) else []
                    new_match["entity_types"] = " ".join(
                        [" ".join(x[1]).replace(":", " ") for x in table_ent_list],
                    )
                    new_match["entity_list"] = table_ent_list
                    if self.use_dpr or index_dpr:
                        new_cell_texts.append(cell_text)
                        new_cell_match_idxs.append(new_match["match_idx"])

                    matches.append(new_match)

            matches.append(match)
        if self.use_dpr or index_dpr and new_cell_texts:
            new_dpr_embs = self.dpr_encoder(new_cell_texts)["embeddings"]
            if new_dpr_embs:
                for match in matches:
                    if match["match_idx"] in new_cell_match_idxs:
                        idx = new_cell_match_idxs.index(match["match_idx"])
                        match["embeddings"]["dpr"] = {
                            "match": new_dpr_embs[idx]
                        }
        self.generate_match_groups(matches, match_idx2objcet_idx)

        db_data = []
        all_header_texts = []
        all_match_text = []
        for match in matches:
            # get current match_idx
            match_idx = match["match_idx"]
            # set index using object_id
            query.append({"index": {"_id": str(match_idx2objcet_idx[match_idx])}})
            # add body
            query.append(match)
            # BBOX
            db_bbox = [-1, -1, -1, -1]
            if bbox.get(match["block_idx"], False):
                db_bbox = bbox[match["block_idx"]]["bbox"]
            # only save the info we need to DB
            _db_data = {
                "_id": match_idx2objcet_idx[match_idx],
                "file_idx": match["file_idx"],
                "match_idx": match["match_idx"],
                "match_text": match.pop("raw_text"),
                "qa_text": match.pop("qa_text", ""),
                "block_text": match["block_text"],
                "page_idx": match["page_idx"],
                "header_text": match["header_text"],
                "header_chain_text": match["header_chain_text"],
                "parent_text": match.pop("parent_text", ""),
                "block_type": match["block_type"],
                "group_type": match.pop("group_type"),
                "level": match.pop("level"),
                "level_chain": match.pop("level_chain"),
                "bbox": db_bbox,
                "entity_list": match.pop("entity_list", []),
                "block_idx": match["block_idx"],
            }
            # Find all the occurrences of key definitions in the match_txt
            if reference_dict:
                haystack = _db_data.get('match_text', '')
                matched_refs = []
                match_keys = {}
                for end_index, (insert_order, original_value) in automaton.iter(haystack):
                    match_key = original_value[0]
                    start_index = end_index - len(match_key) + 1
                    start_checked = True
                    if start_index - 1 > 0:
                        start_checked = check_char_is_word_boundary(haystack[start_index - 1])

                    if start_checked and (end_index == len(haystack) - 1 or
                                          check_char_is_word_boundary(haystack[end_index + 1])):
                        found_longer_match = False
                        for matched_ref in matched_refs:
                            if start_index >= matched_ref[0] and end_index <= matched_ref[1]:
                                found_longer_match = True
                                break
                        if not found_longer_match:
                            if match_key not in match_keys:
                                matched_refs.append((start_index, end_index, (insert_order, original_value)))
                                match_keys[nosql_db.escape_mongo_data(match_key)] = original_value[1]
                if match_keys:
                    _db_data["cross_references"] = match_keys

            if "table_data" in match:
                _db_data["table_data"] = match.pop("table_data")

            if "table_idx" in match:
                _db_data["table_idx"] = match["table_idx"]

            db_data.append(_db_data)

            # modify search field for header and table
            if match["block_type"] == "header":
                match["block_text"] = ""
                # # header of a match can not be itself
                # if match["header_text"] == match["match_text"]:
                #     match["header_text"] = ""
            if block_type == "table":
                match["header_text"] = ""

            # create data for file level index
            if match["block_type"] == "header":
                all_header_texts.append(match["match_text"])
            else:
                all_match_text.append(match["match_text"])

        nosql_db.create_es_entries(db_data, workspace_idx)

        bulk_query_size = 500
        self.logger.info(f"sending {len(query)} index to ES")
        for index in range(0, len(query), bulk_query_size):
            # retry 10 times
            for _ in range(10):
                try:
                    res = self.client.bulk(
                        index=es_index,
                        body=query[index: index + bulk_query_size],
                        timeout="300s",
                        # _source=False,
                    )
                    self.logger.info(f"Took {res['took']} ms to bulk insert {bulk_query_size} items")
                    if res["errors"]:
                        self.logger.error(f"Failed to create index {res}")
                    break
                except socket.timeout:
                    continue

        self.client.indices.refresh(index=es_index)

        create_file_level_index = True
        if workspace_settings:
            create_file_level_index = workspace_settings.get("index_settings", {}) \
                .get("create_file_level_index", True)

        if create_file_level_index:
            file_level_index_id = es_index + self.file_level_suffix

            file_level_body = {
                # attribute
                "id": f"{file_idx}",
                "file_idx": document.id,
                "file_name": document.name,
                # text
                "title_text": document.title,
                "header_text": " ".join(all_header_texts),
                "match_text": " ".join(all_match_text),  # only index intro lines
                "meta": document.meta,
            }

            for _ in range(10):
                try:
                    res = self.client.index(
                        index=file_level_index_id,
                        body=file_level_body,
                        id=document.id,
                    )
                    self.logger.info(
                        f"result of adding {file_idx} to file level index: {res}",
                    )
                    break
                except socket.timeout:
                    continue

            self.client.indices.refresh(index=file_level_index_id)

        wall_time = (default_timer() - wall_time) * 1000
        self.logger.info(
            f"{self.__class__.__name__} Finished. Wall time: {wall_time:.2f}ms",
        )
        return texts, infos, all_header_texts, all_match_text, doc_ent_dict

    def add_to_index(self, file_idx, blocks, num_pages=None, level="sent", bbox={}):
        self.logger.info(f"Processing document {file_idx}")

        document = self.loader.load_document_info(file_idx)
        workspace_idx = document.workspace_id

        workspace = nosql_db.get_workspace_by_id(workspace_idx)
        if not workspace:
            self.logger.error(f"Can not find workspace {workspace_idx}, aborting")
            return

        ignore_block_settings = workspace.settings.get("ignore_block", [])
        domain_settings = workspace.settings.get("domain", "")
        index_dpr = workspace.settings.get("index_settings", {}).get("index_dpr", False)

        # create index if not exist
        self.create_index(
            workspace_idx,
            workspace_settings=workspace.settings,
        )

        # delete from old index
        self.delete_from_index(
            file_idx=file_idx,
            workspace_idx=workspace_idx,
            workspace_settings=workspace.settings,
        )

        return self.add_blocks_to_index(
            workspace_idx,
            file_idx,
            document,
            blocks,
            num_pages,
            level,
            ignore_block_settings,
            domain_settings,
            bbox=bbox,
            index_dpr=index_dpr,
            workspace_settings=workspace.settings
        )

    def delete_index(
            self,
            workspace_idx,
            workspace_settings=None,
    ):
        self.logger.info(f"Deleting index for workspace {workspace_idx}")
        wall_time = default_timer()

        workspace_settings = workspace_settings or {}

        # delete blocks from block level index
        index = workspace_idx
        delete_body = None

        if workspace_settings:
            index = workspace_settings.get("index_settings", {}).get("index", workspace_idx)
            delete_body = {
                "query": {
                    "term": {
                        "workspace_idx": workspace_idx
                    }
                },
            }

        if index != workspace_idx and delete_body:
            self.client.delete_by_query(
                index=index,
                body=delete_body,
                ignore=[404],
                timeout="3600s",
                wait_for_completion=False,
            )

            # refresh index
            self.client.indices.refresh(index=index, ignore=[404])
        else:
            self.client.indices.delete(index=f"{index}*", ignore=[404])

        wall_time = (default_timer() - wall_time) * 1000
        self.logger.info(
            f"{self.__class__.__name__} Finished. Wall time: {wall_time:.2f}ms",
        )

    def add_synonym_dictionary_to_index(
            self,
            synonym_dictionary,
            workspace_idx=None,
            file_level=False,
            workspace_settings=None,
    ):
        self.logger.info(f"Add synonym dictionary to workspace {workspace_idx}")
        workspace_settings = workspace_settings or {}
        wall_time = default_timer()

        es_synonyms_list = []

        for key, synonyms in synonym_dictionary.items():
            for synonym in synonyms:
                es_synonyms_list.append(f"{synonym} => {', '.join(synonyms)}")
            self.logger.info(f"Adding {key} => {synonyms}")

        index = workspace_idx
        if workspace_settings:
            index = workspace_settings.get("index_settings", {}).get("index", workspace_idx)

        # close index to allow change of settings
        self.client.indices.close(f"{index}" or "_all")

        # update index analyzer
        settings = get_index_settings(
            "block",
            creating=False,
            es_synonyms_list=es_synonyms_list,
            workspace_settings=workspace_settings,
        )

        settings["index"].pop("number_of_shards")
        self.client.indices.put_settings(
            {"settings": settings},
            index=f"{index}" or "_all",
        )

        # update index mappings
        if file_level:
            mappings = get_index_mappings("file")
        else:
            mappings = get_index_mappings("block", workspace_settings=workspace_settings)

        self.client.indices.put_mapping(mappings, index=f"{index}" or "_all")

        # open index to allow operations
        self.client.indices.open(f"{index}" or "_all")

        self.client.indices.reload_search_analyzers(index=f"{index}")
        self.client.indices.refresh(index=f"{index}")

        wall_time = (default_timer() - wall_time) * 1000

        self.logger.info(
            f"{self.__class__.__name__} Finished. Wall time: {wall_time:.2f}ms",
        )

    def update_document_meta(
            self,
            workspace_idx,
            file_idxs=None,
    ):
        self.logger.info(f"Updating document meta for workspace {workspace_idx}")
        wall_time = default_timer()
        query = {
            "is_deleted": False,
            "parent_folder": "root",
            "workspace_id": workspace_idx,
            "status": "ingest_ok",
        }
        if file_idxs:
            query["id"] = {"$in": file_idxs}

        for doc in nosql_db.db['document'].find(
                query,
                {"_id": 0, "id": 1, "meta": 1, "name": 1}
        ):

            body = {
                "doc": {
                    "meta": doc.get("meta", {}),
                },
            }
            self.client.update(index=f"{workspace_idx}_file_level", body=body, id=doc["id"])

        self.client.indices.refresh(index=f"{workspace_idx}_file_level")

        wall_time = (default_timer() - wall_time) * 1000

        self.logger.info(
            f"{self.__class__.__name__} Finished. Wall time: {wall_time:.2f}ms",
        )

    def generate_match_groups(self, matches, match_idx2objcet_idx):

        # loop over matches to build child structure
        for idx, match in enumerate(matches):
            # add child info for matches

            # QA text is depend on the block type of current match:
            # Possible block_type defined in line_parser.py:
            # "table_row"
            # "header"
            # "numbered_list_item" or "list_item"
            # "para"

            # When match a header
            if match["block_type"] == "header":
                # looking for next non-header element to make a decision
                for next_idx in range(idx + 1, len(matches)):
                    # Break when we encounter the next header which is on the same level
                    # or lesser level than the current one
                    if matches[next_idx]["level"] <= match["level"]:
                        break
                    # Add only the immediate sub level as child_idx
                    if matches[next_idx]["level"] - 1 > match["level"]:
                        continue
                    match["group_type"] = "header_summary"
                    if matches[next_idx]["block_type"] == "header":
                        next_match_idx = matches[next_idx]["match_idx"]
                        str_next_match_idx = str(match_idx2objcet_idx[next_match_idx])
                        if str_next_match_idx not in match["child_idxs"]:
                            match["child_idxs"].append(
                                str_next_match_idx,
                            )
                    # table does not need child_idx
                    elif matches[next_idx]["block_type"] == "table":
                        match["group_type"] = "table"
                        next_match_idx = matches[next_idx]["match_idx"]
                        str_next_match_idx = str(match_idx2objcet_idx[next_match_idx])
                        if str_next_match_idx not in match["child_idxs"]:
                            match["child_idxs"].append(
                                str_next_match_idx,
                            )
                    # table_row does not need child_idx
                    # elif matches[next_idx]["block_type"] == "table_row":
                    #     continue
                    # Reach to the first para under the header, add to child list and break
                    elif matches[next_idx]["block_type"] == "para":
                        # next sent under block in the starting of a paragraph. e.g.
                        # # Header
                        # here is some examples:
                        # - example 1
                        # - exmaple 2

                        # next paragraph is the first candidate

                        next_match_idx = matches[next_idx]["match_idx"]
                        str_next_match_idx = str(match_idx2objcet_idx[next_match_idx])
                        if str_next_match_idx not in match["child_idxs"]:
                            match["child_idxs"].append(
                                str(match_idx2objcet_idx[next_match_idx]),
                            )
                        # match["group_type"] = "header_summary"

                        future_idx = next_idx + 1
                        # Found list, add all of them as child
                        while future_idx < len(matches) and (
                            matches[next_idx]["block_idx"]
                            == matches[future_idx]["block_idx"]
                            # matches[future_idx]["block_type"] == "list_item"
                            # or (
                            #     len(matches[future_idx]["match_text"].split()) < 5
                            #     and matches[future_idx]["header_text"]
                            #     == match["header_text"]
                            # )
                        ):

                            next_match_idx = matches[future_idx]["match_idx"]
                            str_next_match_idx = str(match_idx2objcet_idx[next_match_idx])
                            if str_next_match_idx not in match["child_idxs"]:
                                match["child_idxs"].append(
                                    str(match_idx2objcet_idx[next_match_idx]),
                                )

                            # assign block_type
                            # match["group_type"] = "list_item"
                            future_idx += 1

                    # found list_item
                    elif matches[next_idx]["block_type"] == "list_item":
                        # Found list, add all of them as child
                        while next_idx < len(matches) and (
                            matches[next_idx]["block_type"] == "list_item"
                            # list below header is one level up than header
                            and matches[next_idx]["level"] - 1 == match["level"]
                        ):
                            next_match_idx = matches[next_idx]["match_idx"]
                            str_next_match_idx = str(match_idx2objcet_idx[next_match_idx])
                            if str_next_match_idx not in match["child_idxs"]:
                                match["child_idxs"].append(
                                    str(match_idx2objcet_idx[next_match_idx]),
                                )
                            # assign block_type
                            match["group_type"] = "list_item"
                            next_idx += 1

            # When match is a para
            elif match["block_type"] == "para":
                for next_idx in range(idx + 1, len(matches)):
                    # check for table
                    if matches[next_idx]["block_type"] == "table":
                        match["group_type"] = "table"

                        next_match_idx = matches[next_idx]["match_idx"]
                        match["child_idxs"].append(
                            str(match_idx2objcet_idx[next_match_idx]),
                        )
                        break
                    # check list
                    else:
                        # Found list, add all of them as child
                        while next_idx < len(matches) and (
                            matches[next_idx]["block_type"] == "list_item"
                        ):
                            next_match_idx = matches[next_idx]["match_idx"]
                            match["child_idxs"].append(
                                str(match_idx2objcet_idx[next_match_idx]),
                            )
                            next_idx += 1
                            match["group_type"] = "list_item"
                        break
            elif match["block_type"] == "list_item":
                for next_idx in range(idx + 1, len(matches)):
                    # Found list, add all of them as child
                    while next_idx < len(matches) and (
                        matches[next_idx]["block_type"] == "list_item"
                        and matches[next_idx]["level"] - 1 == match["level"]
                    ):
                        next_match_idx = matches[next_idx]["match_idx"]
                        match["child_idxs"].append(
                            str(match_idx2objcet_idx[next_match_idx]),
                        )
                        match["group_type"] = "list_item"
                        matches[next_idx]["parent_text"] = match["block_text"]
                        next_idx += 1
                    break

            # table match
            elif match["block_type"] == "table":
                match["group_type"] = "table"


es_client = ElasticsearchClient()

if __name__ == "__main__":
    client = es_client.client

    # update settings
    es_client.add_synonym_dictionary_to_index({}, "b53ef66c")

    # templates = ["acquisition", "acquisition cost"]
    # questions = []
    # headers = []
    # matches_per_doc = 20

    # workspace_idx, file_idx = "abf049b4", "f418fcf3"  # multiple documents

    workspace_idx = "c227c4db"

    # es_client.create_index(workspace_idx)
    # es_client.add_to_index(workspace_idx)

    es_client.delete_index(workspace_idx)
    # asdf

    # index = client.indices.get_settings(workspace_idx)
    # print(index)

    # es_client.add_synonym_dictionary_to_index(
    #     # {"weather": ["wazzz", "asdf"]},
    #     {},
    #     workspace_idx=workspace_idx,
    # )

    # index = client.indices.get_settings(workspace_idx)
    # print(index)

    # response = client.search(
    #     index=workspace_idx,
    #     body={"query": {"match": {"match_text": "weather"}}},
    #     size=0,
    #     filter_path=[
    #         "-aggregations.matches.buckets.docs.hits.hits._source.embeddings",
    #     ],
    # )
    # print(response)

    # response = client.search(
    #     index=workspace_idx,
    #     body={"query": {"match": {"match_text": "asdf"}}},
    #     size=0,
    #     filter_path=[
    #         "-aggregations.matches.buckets.docs.hits.hits._source.embeddings",
    #     ],
    # )
    # print(response)
