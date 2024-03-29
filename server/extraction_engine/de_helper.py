import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

question_words = [
    "who",
    "what",
    "when",
    "where",
    "why",
    "how",
    "which",
    "whose",
    "whom",
]
bool_question_words = [
    "is",
    "do",
    "does",
    "will",
    "can",
    "is",
    "are",
    "was",
    "were",
    "has",
    "did",
]


def is_question(sent: str):
    if sent:
        tokens = sent.lower().split()
        return len(tokens) > 1 and (
            tokens[0] in question_words or tokens[0] in bool_question_words
        )
    else:
        return False


def is_bool_question(sent: str):
    tokens = sent.lower().split()
    return len(tokens) > 1 and tokens[0] in bool_question_words


def make_question(sent: str):
    if not sent.endswith("?"):
        return sent + "?"
    else:
        return sent


def validate_text(texts):
    if texts is None:
        texts = [""]
    for idx, text in enumerate(texts):
        if text is not None:
            if isinstance(text, str):
                texts[idx] = text.strip()
            elif isinstance(text, list):
                texts[idx] = [t.strip() for t in text]
        if text is None or text == "":
            texts[idx] = None
    # remove None
    return texts


def resolve_query_params(tn_sents, qn):

    # logger.info("in resolve_query_params")
    # logger.info(tn_sents, qn)

    # validate text, remove empty string and None
    tn_sents = validate_text(tn_sents)
    qn = validate_text(qn)
    # padding the length
    max_len = max(len(tn_sents), len(qn))
    tn_sents += [None] * (max_len - len(tn_sents))
    qn += [None] * (max_len - len(qn))

    for idx, (template, question) in enumerate(zip(tn_sents, qn)):
        # both template and question is provided
        if template and question:
            if is_question(question):
                qn[idx] = make_question(qn[idx])
        # only template or question is provided, make question if needed
        else:
            if template:
                if is_question(template):
                    qn[idx] = make_question(template)
            else:
                if is_question(question):
                    qn[idx] = make_question(question)
                else:
                    qn[idx] = None
            # assign template
            tn_sents[idx] = template or question

    return tn_sents, qn
