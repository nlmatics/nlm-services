import logging
import os
import re
import string
import uuid
from datetime import datetime

import connexion
import dateparser
from faker import Faker
from flask import jsonify
from flask import make_response
from nlm_utils.model_client import NlpClient

from server.storage import nosql_db

# import cachetools

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

nlp_client = NlpClient(
    url=os.getenv("MODEL_SERVER_URL"),
)
# Quote Pattern
quotation_pattern = re.compile(r'[”“"‘’\']')
# Crete a Faker object.
fake = Faker()
fake.seed_instance(4321)


def preprocess_text(text: str):
    """
    Normalizes the quotes.
    :param text: Input text
    :return: Text after normalizing quotes.
    """
    return quotation_pattern.sub('"', text)


def create_uuid():
    """
    Creates a unique identifier with timestamp information.
    :return: Generated UUID
    """
    t_stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return str(uuid.uuid1()) + "-" + t_stamp


def retrieve_doc_entities(texts):
    """
    Executes "get_doc_tags" for the input text and returns the response.
    :param texts: Input list of text strings.
    :return: Response from Model Server.
    """
    return nlp_client(texts=texts, option="get_doc_tags")


# Faker functions
def fake_name(_x):
    """
    Returns a fake name
    :param _x:
    :return: Returns a fake name
    """
    return fake.name()


def fake_date(_x):
    """
    Returns a fake date
    :param _x:
    :return: Returns a fake date
    """
    return fake.date()


def fake_company(_x=None):
    """
    Returns a fake company name
    :param _x:
    :return: Returns a company name
    """
    fake_company_str = fake.company().split()[0]
    for p in list(string.punctuation):
        fake_company_str = fake_company_str.replace(p, " ")
    return fake_company_str.strip()


def fake_money(_x):
    """
    Returns a fake currency
    :param _x:
    :return: Returns a fake currency
    """
    money = fake.pricetag()
    if money[0] == "$":
        money = money[1:]
    return money


def fake_country(_x):
    """
    Returns a fake country
    :param _x:
    :return: Returns a fake country name
    """
    return fake.country()


ENTITIES_OF_INTEREST = {
    "DATE": fake_date,
    "PERSON": fake_name,
    "ORG": fake_company,
    "MONEY": fake_money,
    "GPE": fake_country,
}


def encode_text(txt):
    ret_text = txt
    if not txt:
        return ret_text

    ner_resp = retrieve_doc_entities([txt]) or []
    replaced_texts_dict = {}
    anonymized_entities = []
    not_anonymized_entities = []

    for idx, r in enumerate(ner_resp):
        for ner_entry in r.get("ner", []):
            [ner_text, ner_label] = ner_entry
            if ner_label in ENTITIES_OF_INTEREST:
                fake_text = ENTITIES_OF_INTEREST[ner_label](ner_text)
                if ner_text in replaced_texts_dict:
                    fake_text = replaced_texts_dict[ner_text][0]
                else:
                    replaced_texts_dict[ner_text] = [fake_text]
                ret_text = ret_text.replace(ner_text, fake_text, 1)
                anonymized_entities.append(ner_entry)
            else:
                not_anonymized_entities.append(ner_entry)
    return ret_text, replaced_texts_dict, anonymized_entities, not_anonymized_entities


def encode_and_create_replaced_dictionary(txt):
    (
        ret_text,
        replaced_texts_dict,
        anonymized_entities,
        not_anonymized_entities,
    ) = encode_text(txt)
    anonymize_uuid = None
    if ret_text and replaced_texts_dict:
        anonymize_uuid = create_uuid()
        nosql_db.save_anonymized_dict(anonymize_uuid, replaced_texts_dict)
    return ret_text, anonymize_uuid, anonymized_entities, not_anonymized_entities


def decode_text(txt, g_uid):
    """
    De Anonymize the text using the g_uid passed
    :param txt: text that needs to be de-anonymized.
    :param g_uid: UUID from input request
    :return: De-anonymized text
    """

    ret_text = txt
    if not txt or not g_uid:
        logger.info(f"Empty Text or GUID passed to decoder: {txt} or {g_uid}")
        return ret_text

    replaced_dict = nosql_db.retrieve_anonymized_dict(g_uid) or {}
    logger.info(f"Found dictionary for GUID : {g_uid} ... {replaced_dict}")

    # Run the entity detection on the input text,
    # so that we can normalize any DATE entities.
    ner_resp = retrieve_doc_entities([txt]) or []
    date_entities = {}
    for idx, r in enumerate(ner_resp):
        for ner_entry in r.get("ner", []):
            [ner_text, ner_label] = ner_entry
            if ner_label == "DATE" and ner_text:
                date_entities[
                    dateparser.parse(ner_text).strftime("%Y-%m-%d")
                ] = ner_text

    for k, v_list in replaced_dict.items():
        for v in v_list:
            ret_text = ret_text.replace(v, k)
            if v in date_entities:
                ret_text = ret_text.replace(date_entities[v], k)

    return ret_text


def anonymize_text():  # noqa: E501
    """Anonymize the provided text.

     # noqa: E501

    :rtype: JSON Structure
    {
        "uuid": uuid,
        "text": text,  # anonymized text
        "anonymized_entities": list of entities which are anonymized,
        "not_anonymized_entities": uuid,
    }
    """
    if connexion.request.is_json:
        json_req = connexion.request.get_json()
        (
            ret_text,
            anonymize_uuid,
            anonymized_entities,
            not_anonymized_entities,
        ) = encode_and_create_replaced_dictionary(json_req.get("text", ""))
        ret_dict = {
            "uuid": anonymize_uuid,
            "text": ret_text,
            "anonymized_entities": anonymized_entities,
            "not_anonymized_entities": not_anonymized_entities,
        }

        return make_response(
            jsonify(
                ret_dict,
            ),
            200,
        )
    else:
        status, rc, msg = "fail", 422, "invalid json"
    return make_response(jsonify({"status": status, "reason": msg}), rc)


def de_anonymize_text():  # noqa: E501
    """De-Anonymize the provided text.

     # noqa: E501

    :rtype: IdWithMessage
    """
    if connexion.request.is_json:
        json_req = connexion.request.get_json()
        ret_text = decode_text(json_req.get("text", ""), json_req.get("uuid", ""))
        ret_dict = {
            "text": ret_text,
        }

        return make_response(
            jsonify(
                ret_dict,
            ),
            200,
        )
    else:
        status, rc, msg = "fail", 422, "invalid json"
    return make_response(jsonify({"status": status, "reason": msg}), rc)
