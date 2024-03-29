import base64
import io
import json
import logging
import os
import time
from datetime import datetime

import requests
import xmltodict
from bs4 import BeautifulSoup
from nlm_utils.utils import ensure_bool
from pymongo import MongoClient
from sec_api import MappingApi
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from werkzeug.datastructures import FileStorage

from .base_task import BaseTask
from server.controllers.document_controller import upload_document as controller_upload


class SecRssTask(BaseTask):
    task_name = "sec_rss"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self):
        exception_queue = self._kwargs.get("exception_queue")
        try:
            get_doc()
        except Exception as e:
            self.logger.error(e, exc_info=True)
            if exception_queue:
                exception_queue.put(e)


db_client = MongoClient(os.getenv("MONGO_HOST", "localhost"))
db = db_client[os.getenv("MONGO_DATABASE", "doc-store-dev")]
update_rss_db = ensure_bool(os.getenv("UPDATE_RSS_DB", True))
use_rss_new_version = ensure_bool(os.getenv("USE_RSS_NEW_VERSION", False))
sec_api_secret = os.getenv("SEC_API_SECRET", "")
sec_mapping = None
if sec_api_secret:
    sec_mapping = MappingApi(api_key=sec_api_secret)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"
request_headers = {
    "content-type": "text/html;charset=UTF-8",
    "Accept-Encoding": "gzip, deflate, sdch",
    "Accept-Language": "en-US,en;q=0.8",
    "User-Agent": user_agent,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "host": "www.sec.gov",
}

service = Service()
webdriver_options = webdriver.ChromeOptions()
webdriver_options.add_argument(f"--user-agent={user_agent}")
webdriver_options.add_argument("--headless")
webdriver_options.add_argument("--disable-gpu")
webdriver_options.add_argument("--disable-dev-shm-usage")
webdriver_options.add_argument("--no-sandbox")
driver = webdriver.Chrome(
    service=service,
    options=webdriver_options,
)


def get_access_token(api_key, app_id, server):
    payload = {"api_key": api_key, "app_id": app_id}
    resp = requests.post(
        f"{server}/api/developerApiKey/accessToken",
        json=payload,
    )
    return resp.text


def upload_file_api(
    filename,
    workspace_id,
    server,
    access_token="",
):

    headers = {"authorization": f"Bearer {access_token}"}
    with open(filename, "rb") as f:
        resp = requests.post(
            f"{server}/api/document/workspace/{workspace_id}",
            files={"file": f},
            headers=headers,
        )
    print(resp.text)
    return resp.status_code


def upload_file_from_url(
    url,
    filename,
    workspace_idx,
    prod_worspace_idx="",
    file_meta=None,
    workspace_config_data=None,
):
    def send_devtools(driver, cmd, params={}):
        resource = (
            "/session/%s/chromium/send_command_and_get_result" % driver.session_id
        )
        url = driver.command_executor._url + resource
        body = json.dumps({"cmd": cmd, "params": params})
        response = driver.command_executor._request("POST", url, body)
        if response.get("status"):
            logger.error(response.get("value"))
        else:
            return response.get("value")

    def get_pdf_from_html(path, print_options={}):
        global driver
        try:
            driver.get(path)
            calculated_print_options = {
                "landscape": False,
                "displayHeaderFooter": False,
                "printBackground": True,
                "preferCSSPageSize": True,
            }
            calculated_print_options.update(print_options)
            result = send_devtools(driver, "Page.printToPDF", calculated_print_options)

            if not result:
                logger.error(f"Document from {path} cannot be uploaded")
            else:
                return base64.b64decode(result["data"])
        except Exception as e:
            logger.info(
                f"Error in get_pdf_from_html: {str(e)}",
                exc_info=True,
            )
            driver = webdriver.Chrome(
                service=service,
                options=webdriver_options,
            )
            driver.get(path)
            calculated_print_options = {
                "landscape": False,
                "displayHeaderFooter": False,
                "printBackground": True,
                "preferCSSPageSize": True,
            }
            calculated_print_options.update(print_options)
            result = send_devtools(driver, "Page.printToPDF", calculated_print_options)

            if not result:
                logger.error(f"Document from {path} cannot be uploaded")
            else:
                return base64.b64decode(result["data"])

    # download file
    result = get_pdf_from_html(url)
    if result:
        # upload file via controller
        if workspace_idx:
            file = FileStorage(io.BytesIO(result), filename=filename)
            doc_id = controller_upload(
                os.getenv("ADMIN_USER", "admin@nlmatics.com"),  # sec_user@nlmatics.com
                {},
                workspace_idx,
                file,
                return_raw=True,
                file_meta=file_meta,
            )

            logger.info(
                f"Finished uploading {doc_id} {filename} to workspace {workspace_idx}",
            )
            # Add sec-api mapping.
            if (
                sec_mapping
                and doc_id
                and workspace_config_data
                and workspace_config_data.get(
                    "field_id_to_sec_api_response_mapping",
                    {},
                )
            ):
                edited_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                field_bundle_id = workspace_config_data.get("field_bundle_id", "")
                for doc in db["document"].find(
                    {"id": doc_id},
                    {"_id": 0, "id": 1, "name": 1, "meta.cik": 1},
                ):
                    cik = doc.get("meta", {}).get("cik", "").lstrip("0")
                    if cik:
                        # Sample Response.
                        # {
                        #     'name': 'TESLA INC',
                        #      'ticker': 'TSLA',
                        #      'cik': '1318605',
                        #      'cusip': '88160R101',
                        #      'exchange': 'NASDAQ',
                        #      'isDelisted': False,
                        #      'category': 'Domestic Common Stock',
                        #      'sector': 'Consumer Cyclical',
                        #      'industry': 'Auto Manufacturers',
                        #      'sic': '3711',
                        #      'sicSector': 'Manufacturing',
                        #      'sicIndustry': 'Motor Vehicles & Passenger Car Bodies',
                        #      'famaSector': '',
                        #      'famaIndustry': 'Automobiles and Trucks',
                        #      'currency': 'USD',
                        #      'location': 'California; U.S.A',
                        #      'id': 'eaeafc4ffc04a49da153adebf1f6960a'
                        #  }
                        sec_response_list = sec_mapping.resolve("cik", cik)
                        if sec_response_list and len(sec_response_list) > 0:
                            sec_response = sec_response_list[0]
                            insert_items = []
                            for k, v in workspace_config_data.get(
                                "field_id_to_sec_api_response_mapping",
                                {},
                            ).items():
                                field_value = sec_response[v]
                                top_fact = {
                                    "answer": field_value,
                                    "formatted_answer": field_value,
                                    "answer_details": {
                                        "raw_value": field_value,
                                        "formatted_value": field_value,
                                    },
                                    "type": "override",
                                    "match_idx": "manual",
                                    "is_override": True,
                                }
                                history = {
                                    "username": "script_task",
                                    "edited_time": edited_time,
                                    "previous": None,
                                    "modified": top_fact,
                                }
                                insert_item = {
                                    "field_idx": k,
                                    "file_idx": doc["id"],
                                    "workspace_idx": workspace_idx,
                                    "field_bundle_idx": field_bundle_id,
                                    "field_value_history": [history],
                                    "file_name": doc["name"],
                                    "top_fact": top_fact,
                                    "last_modified": datetime.utcnow(),
                                }
                                insert_items.append(insert_item)
                            db["field_value"].insert_many(insert_items)

                # Update the unique values of the fields.
                for k, _v in workspace_config_data.get(
                    "field_id_to_sec_api_response_mapping",
                    {},
                ).items():
                    dist_data_ref = db["field_value"].aggregate(
                        [
                            {
                                "$match": {
                                    "workspace_idx": workspace_idx,
                                    "field_bundle_idx": field_bundle_id,
                                    "field_idx": k,
                                },
                            },
                            {
                                "$group": {
                                    "_id": "$top_fact.answer_details.raw_value",
                                },
                            },
                        ],
                    )
                    distinct_values = [d1["_id"] for d1 in dist_data_ref]
                    db["field"].update_one(
                        {
                            "id": k,
                        },
                        {
                            "$set": {
                                "distinct_values": distinct_values,
                            },
                        },
                    )
        # upload to enterprise via api
        if prod_worspace_idx:
            # server = "https://enterprise.nlmatics.com/"
            server = os.getenv("RSS_API_SERVER")
            access_token = ""  # to be refreshed automatically
            api_key = os.getenv(
                "PROD_API_KEY"
            )
            app_id = os.getenv("PROD_APP_ID")

            # save file
            with open(filename, "wb") as file:
                file.write(result)

            # if access token is expired, refresh access token
            r_code = upload_file_api(
                f"{os.getcwd()}/{filename}",
                prod_worspace_idx,
                server,
                access_token,
            )
            if r_code == 401:
                access_token = get_access_token(api_key, app_id, server)
                r_code = upload_file_api(
                    f"{os.getcwd()}/{filename}",
                    prod_worspace_idx,
                    server,
                    access_token,
                )
            if r_code != 200:
                logger.error(
                    f"Failed to upload {os.getcwd()}/{filename} to production workspace {prod_worspace_idx}, error {r_code}",
                )

            # clean up
            os.remove(filename)
            logger.info(
                f"Finished uploading {filename} to production workspace {workspace_idx}",
            )


def create_rss_links_from_forms(allowed_forms, cik="", company="", count="100"):
    rss_links = []
    for form in allowed_forms:
        # Default the Form-type to 8-K
        rss_links.append(
            f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK={cik}"
            f"&type={form.get('rss_form_type', '8-K')}&company={company}&dateb=&owner=include"
            f"&start=0&count={count}&output=atom",
        )
    return rss_links


def check_form(allowed_form, form):
    allowed_form_type_match = allowed_form.get("form_type_match", "prefix")
    allowed_form_types = allowed_form.get("form_types", [])
    if allowed_form_type_match == "exact" and any(
        form == x for x in allowed_form_types
    ):
        return True
    elif allowed_form_type_match == "prefix" and any(
        form.startswith(x) for x in allowed_form_types
    ):
        return True
    return False


def get_doc_from_rss(arg_cik="", arg_company="", count="100"):
    current_time = 0
    used_links = []
    used_titles = []
    used_filenames = []
    check_count = 1
    allowed_forms = [
        a
        for a in db["sec_rss"].find(
            {"type": "workspace_config", "active": True},
        )
    ]
    rss_links = create_rss_links_from_forms(allowed_forms, arg_cik, arg_company, count)
    current_start_time = None

    while True:
        try:
            # Check every hour for DB Update.
            if not check_count % 60:
                logger.info(f"Fetching the data from DB {check_count}")
                allowed_forms = [
                    a
                    for a in db["sec_rss"].find(
                        {"type": "workspace_config", "active": True},
                    )
                ]
                rss_links = create_rss_links_from_forms(
                    allowed_forms,
                    arg_cik,
                    arg_company,
                    count,
                )
                check_count = 1

            all_documents = []

            for rss_link, allowed_form in zip(rss_links, allowed_forms):
                r = None
                try:
                    r = requests.get(rss_link, headers=request_headers)
                except Exception as e:
                    logger.info(
                        f"Error in RSS Requests {str(e)} .... {rss_link}",
                        exc_info=True,
                    )
                if not r:
                    continue

                parsed = xmltodict.parse(r.text)
                feed_entries = parsed.get("feed", {}).get("entry", [])
                if feed_entries and len(feed_entries) > 0:
                    try:
                        current_start_time = datetime.fromisoformat(
                            parsed["feed"]["entry"][0]["updated"],
                        ).timestamp()
                    except Exception as e:
                        logger.info(
                            f"Error {rss_link} in Current Time stamp calculation {str(e)} .... {parsed['feed']['entry']}",
                            exc_info=True,
                        )
                for entry in parsed["feed"]["entry"]:
                    url = entry["link"]["@href"]
                    title = entry["title"]
                    # processing timestamp
                    time_str = entry["updated"].split("T")
                    timestamp = time_str[1] if len(time_str) > 1 else "00:00:00-04:00"
                    parsed_time = datetime.fromisoformat(entry["updated"])
                    time_int = datetime.fromisoformat(entry["updated"]).timestamp()
                    # check if the current time has past the previous stored time
                    if time_int < current_time or (
                        time_int == current_time and title in used_titles
                    ):
                        break

                    used_titles.append(title)
                    # check if form type is wanted
                    form_type = entry["category"]["@term"].strip()
                    if not check_form(allowed_form, form_type):
                        continue

                    date = time_str[0]
                    year = parsed_time.year
                    qtr = (parsed_time.month - 1) // 3 + 1

                    # skip if it's the same filing
                    if url in used_links:
                        continue
                    used_links.append(url)

                    # get individual filings under the index url
                    r = None
                    try:
                        r = requests.get(url, headers=request_headers)
                    except Exception as e:
                        logger.info(
                            f"Error in Main URL Requests {str(e)} .... {url}",
                            exc_info=True,
                        )
                    if not r:
                        continue
                    time.sleep(0.1)  # sleep so SEC doesn't time out

                    soup = BeautifulSoup(r.text, features="lxml")

                    # get Company name & CIK from index url
                    name_str = soup.find("span", {"class": "companyName"}).text
                    company_name = name_str.split("\n")[0]
                    company_name = company_name[: company_name.rfind("(")].strip()
                    cik = name_str.split("\n")[1]
                    cik = str(int(cik[cik.find(":") + 1 : cik.rfind("(")].strip()))

                    # get document url
                    table = soup.find("table", {"class": "tableFile"})
                    if not table:
                        continue

                    workspace_idx = allowed_form.get("workspace_idx", "")
                    allowed_form_types = allowed_form.get("form_types", [])
                    allowed_form_sub_types = allowed_form.get("form_subtypes", [])
                    not_allowed_types = allowed_form.get("form_not_match_strings", [])

                    tab_rows = table.find_all("tr") or []
                    for row in tab_rows:
                        cols = row.find_all("td")
                        if not cols or not cols[0] or not cols[0].text.strip():
                            continue
                        type_ = cols[3]
                        if not any(ele == type_.text for ele in allowed_form_types):
                            continue
                        link = "https://www.sec.gov" + cols[2].find("a").attrs["href"]
                        allowed_prefix_url = link.replace("ix?doc=/", "")
                        # Retrieve the 8-K document and then check for a table with "Credit Agreement" in there.
                        if "credit agreement" in allowed_form_sub_types:
                            r1 = None
                            try:
                                r1 = requests.get(
                                    allowed_prefix_url,
                                    headers=request_headers,
                                )
                            except Exception as e:
                                logger.info(
                                    f"Error in 8-K Requests {str(e)} .... {allowed_prefix_url}",
                                )

                            if not r1:
                                continue

                            soup_t = BeautifulSoup(r1.text, features="lxml")
                            # Retrieve all the a_tags
                            a_tags = soup_t.find_all("a") or []
                            download_cnt = 0
                            for a_tag in a_tags:
                                if (
                                    any(
                                        ele in a_tag.text.lower()
                                        for ele in allowed_form_sub_types
                                    )
                                    and not (
                                        any(
                                            ele in a_tag.text.lower()
                                            for ele in not_allowed_types
                                        )
                                    )
                                    and a_tag.attrs.get("href", "")
                                ):
                                    download_url = allowed_prefix_url
                                    if (
                                        "http" not in a_tag.attrs["href"]
                                        or a_tag.attrs["href"].index("http") > 0
                                    ):
                                        download_url = (
                                            download_url.rsplit("/", 1)[0]
                                            + "/"
                                            + a_tag.attrs["href"]
                                        )
                                    else:
                                        download_url = a_tag.attrs["href"]
                                    logger.info(
                                        f"Final download URL for {company_name} --- {date} is {download_url}",
                                    )
                                    if not download_cnt:
                                        filename = (
                                            f"{company_name} - {year} QTR{qtr}.pdf"
                                        )
                                    else:
                                        filename = f"{company_name} - {year} QTR{qtr}_{download_cnt}.pdf"
                                    download_cnt += 1

                                    if filename in used_filenames:
                                        continue
                                    # save and upload
                                    used_filenames.append(filename)
                                    # Update DB
                                    document = {
                                        "company_name": company_name,
                                        "form_type": form_type,
                                        "cik": cik,
                                        "date": date,
                                        "time_stamp": timestamp,
                                        "index_url": url,
                                        "allowed_prefix_url": allowed_prefix_url,
                                        "download_url": download_url,
                                        "year": year,
                                        "quarter": qtr,
                                        "from_rss": True,
                                    }

                                    all_documents.append(document)

                                    file_meta = {
                                        "title": company_name,
                                        "pubDate": datetime.strptime(date, "%Y-%m-%d"),
                                        "type": "Credit Agreements",
                                        "source": "SEC-EDGAR",
                                        "url": download_url,
                                        "cik": cik,
                                        "description": a_tag.text,
                                    }
                                    upload_file_from_url(
                                        download_url,
                                        filename,
                                        workspace_idx,
                                        "",
                                        file_meta=file_meta,
                                        workspace_config_data=allowed_form,
                                    )

            if current_start_time:
                current_time = current_start_time
                used_titles = used_titles[-int(count) :]
            # insert to db
            if update_rss_db and len(all_documents) > 0:
                db["sec"].insert_many(all_documents)
        except Exception as e:
            logger.error(e, exc_info=True)

        logger.info("sleeping for next cycle")
        time.sleep(60)  # check again in a minute
        check_count += 1


def get_doc(form="", cik="", company="", count="100"):
    current_time = 0
    used_links = []
    used_titles = []
    used_filenames = []
    rss_link = (
        f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK={cik}&type={form}"
        f"&company={company}&dateb=&owner=include&start=0&count={count}&output=atom"
    )

    def check_form(allowed_forms, form):
        for f in allowed_forms:
            if f["form_type_match"] == "exact" and f["form_type"] == form:
                return f
            if f["form_type_match"] == "prefix" and form.startswith(f["form_type"]):
                return f

    while True:
        try:
            # check db for updates
            allowed_forms = [
                a
                for a in db["sec_rss"].find(
                    {"type": "workspace_config", "active": True},
                )
            ]
            all_documents = []
            r = requests.get(rss_link, headers=request_headers)
            parsed = xmltodict.parse(r.text)
            current_start_time = datetime.fromisoformat(
                parsed["feed"]["entry"][0]["updated"],
            ).timestamp()
            for entry in parsed["feed"]["entry"]:
                url = entry["link"]["@href"]
                title = entry["title"]
                # processing timestamp
                time_str = entry["updated"].split("T")
                timestamp = time_str[1] if len(time_str) > 1 else "00:00:00-04:00"
                parsed_time = datetime.fromisoformat(entry["updated"])
                time_int = datetime.fromisoformat(entry["updated"]).timestamp()
                # check if the current time has past the previous stored time
                if time_int < current_time or (
                    time_int == current_time and title in used_titles
                ):
                    break

                used_titles.append(title)

                # check if form type is wanted
                form_type = entry["category"]["@term"].strip()
                form_obj = check_form(allowed_forms, form_type)
                if not form_obj:
                    continue

                workspace_idx = form_obj["workspace_idx"]
                sub_type = form_obj["form_subtype"]

                date = time_str[0]
                year = parsed_time.year
                qtr = (parsed_time.month - 1) // 3 + 1

                # skip if it's the same filing
                if url in used_links:
                    continue
                used_links.append(url)

                # get individual filings under the index url
                r = requests.get(url, headers=request_headers)
                time.sleep(0.1)  # sleep so SEC doesn't time out
                soup = BeautifulSoup(r.text, features="lxml")

                # get name from index url
                name_str = soup.find("span", {"class": "companyName"}).text
                company_name = name_str.split("\n")[0]
                company_name = company_name[: company_name.rfind("(")].strip()
                cik = name_str.split("\n")[1]
                cik = str(int(cik[cik.find(":") + 1 : cik.rfind("(")].strip()))

                # get document url
                table = soup.find("table", {"class": "tableFile"})
                forms = []
                upload_url = ""
                for row in table.find_all("tr"):
                    cols = row.find_all("td")
                    if not cols or not cols[0] or not cols[0].text.strip():
                        continue
                    link = "https://www.sec.gov" + cols[2].find("a").attrs["href"]
                    text = cols[2]
                    type_ = cols[3]
                    forms.append(
                        {
                            "type": type_.text,
                            "text": text.text,
                            "link": link,
                        },
                    )
                    if type_.text == sub_type:
                        upload_url = link
                        upload_url = upload_url.replace("ix?doc=/", "")
                # insert to db
                document = {
                    "company_name": company_name,
                    "form_type": form_type,
                    "cik": cik,
                    "date": date,
                    "time_stamp": timestamp,
                    "index_url": url,
                    "forms": forms,
                    "year": year,
                    "quarter": qtr,
                    "from_rss": True,
                }

                all_documents.append(document)

                # ignore if the form subtype is not found
                if not upload_url:
                    continue

                # skip if it's the same filing content (but different entry)
                filename = f"{company_name} - {form_type} ({document['year']} Q{document['quarter']}).pdf"
                if filename in used_filenames:
                    continue
                # save and upload
                used_filenames.append(filename)
                logger.info(f"Uploading {filename}")
                prod_worspace_idx = (
                    form_obj["prod_worspace_idx"]
                    if "prod_worspace_idx" in form_obj
                    else ""
                )
                upload_file_from_url(
                    upload_url,
                    filename,
                    workspace_idx,
                    prod_worspace_idx,
                )

            current_time = current_start_time
            used_titles = used_titles[-int(count) :]
            # insert to db
            if update_rss_db and len(all_documents) > 0:
                db["sec"].insert_many(all_documents)
        except Exception as e:
            logger.error(e, exc_info=True)

        logger.info("sleeping for next cycle")
        time.sleep(60)  # check again in a minute


if use_rss_new_version:
    get_doc_from_rss(count="10")
else:
    get_doc(form="10-K", count="10")
