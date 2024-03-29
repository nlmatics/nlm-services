import requests
import os
from server.storage import nosql_db as nosqldb
from nlm_utils.storage import file_storage
from werkzeug.utils import secure_filename
import tempfile
import logging


def ocr_first_page(raw_file, doc):
    """
    params: pdf file path, swagger doc object
    call tika server to extract text from image of first page
    store extracted ocr data in db
    """

    try:
        logging.info("OCR first page")
        tika_server_endpoint = os.environ["TIKA_SERVER_ENDPOINT"]
        with open(raw_file, "rb") as file:
            files = {'file': file}
            r = requests.get(f'{tika_server_endpoint}/tika/ocrFirstPage', files=files,
                             headers={"Content-type": "application/pdf", "X-Tika-PDFOcrStrategy": "ocr_only"})

        if r.status_code == 200:
            nosqldb.add_document_attribute(doc.id, "ocr_first_page_text", r.text.strip().replace("\n\n", "\n"))

    except Exception as e:
        logging.error("Error running OCR on page", e)
    # store in db


def get_page_thumbnail(raw_file, doc):
    """
    params: pdf file path, swagger doc object
    call tika server to create image thumbnail of first page
    store image of first page in db for thumbnail
    """
    try:
        logging.info("Creating Thumbnail")
        tika_server_endpoint = os.environ["TIKA_SERVER_ENDPOINT"]
        with open(raw_file, "rb") as file:
            files = {'file': file}
            r = requests.get(f'{tika_server_endpoint}/tika/createThumbNail',
                             files=files,
                             headers={"Content-type": "application/pdf"})

        tempfile_handler, tmp_file = tempfile.mkstemp()
        os.close(tempfile_handler)
        with open(tmp_file, "wb") as fp:
            fp.write(r.content)

        # given doc id, store in  gcp bucket
        # give path to doc object, propagate to db
        if r.status_code == 200:
            dest_blob_name = os.path.join(doc.user_id, doc.workspace_id, "images", doc.id)
            file_storage.upload_blob(tmp_file, dest_blob_name, "image/jpeg")
            # update document entry in db with path
            nosqldb.add_document_attribute(doc.id, "thumbnail_location", dest_blob_name)


    except Exception as e:
        logging.error("Error creating thumbnail", e)


def ocr_first_page_by_doc_id(doc):
    """
    this is different from the above function with the added
    step of creating a temp file, given an existing document
    """
    try:
        # download document
        tmp_file = file_storage.download_document(doc.doc_location)

        ocr_first_page(tmp_file, doc)

    except Exception as e:
        logging.error("Error creating thumbnail", e)


def get_page_thumbnail_by_doc_id(doc):
    """
    this is different from the above function with the added
    step of creating a temp file, given an existing document
    """
    try:
        # download document
        tmp_file = file_storage.download_document(doc.doc_location)

        get_page_thumbnail(tmp_file, doc)

    except Exception as e:
        logging.error("Error creating thumbnail", e)


