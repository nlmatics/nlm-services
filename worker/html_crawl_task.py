#!/usr/bin/env python
import io
import re
import urllib.request
import gzip
import os

import requests
from bs4 import BeautifulSoup
from nlm_utils.rabbitmq import producer
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from werkzeug.datastructures import FileStorage
import xml.etree.ElementTree as ET

from .base_task import BaseTask
from server.controllers.document_controller import upload_document as controller_upload
from server.storage import nosql_db
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class HTMLCrawlTask(BaseTask):
    task_name = "html_crawling"

    def run(self):
        # get the arguments from Thread
        (task_body,) = self._args
        exception_queue = self._kwargs.get("exception_queue")

        try:
            crawl(task_body)
        except Exception as e:
            self.logger.error(e, exc_info=True)
            if exception_queue:
                exception_queue.put(e)


def html_crawl(task, exception_queue):
    try:
        crawl(task)
    except Exception as e:
        exception_queue.put(e)


def _find_main(soup):
    soup = soup.find("body")
    total_words = len(soup.text.split())
    containers = {}
    main_div = None

    for c in soup.findChildren("div", recursive=True):
        name, id_, class_, role = c.name, c.get("id"), c.get("class"), c.get("role")
        # stop if the html is labeled with "main"
        if name == "main" or id_ == "main" or class_ == "main" or role == "main":
            main_div = c
            break

        # find container word count
        direct_child_list = c.findChildren(recursive=False)
        if direct_child_list:
            containers[c] = len(c.text.split())

    # sort by word count
    containers = {
        k: v
        for k, v in sorted(containers.items(), key=lambda item: item[1], reverse=True)
    }

    # find main container by word count
    if not main_div:
        for c in containers:
            if 0.2 * total_words < containers[c] < 0.95 * total_words:
                main_div = c
                break

    # find the biggest container instead
    if not main_div and len(containers) > 0:
        main_div = containers[0]

    return main_div


def _remove_tags(data):
    data = re.sub("(<img.*?>)", "", data, 0, re.IGNORECASE | re.DOTALL | re.MULTILINE)
    data = re.sub(
        "(<script.*?>)",
        "",
        data,
        0,
        re.IGNORECASE | re.DOTALL | re.MULTILINE,
    )
    return data


def _get_html_headless(url):
    chrome_options = Options()
    chrome_options.add_argument("start-maximized")
    # open Browser in maximized mode
    chrome_options.add_argument("disable-infobars")
    # disabling infobars
    chrome_options.add_argument("--disable-extensions")
    # disabling extensions
    chrome_options.add_argument("--disable-gpu")
    # applicable to windows os only
    chrome_options.add_argument("--disable-dev-shm-usage")
    # overcome limited resource problems
    chrome_options.add_argument("--no-sandbox")
    # Bypass OS security model
    chrome_options.add_argument("--headless")
    broswer = webdriver.Chrome(options=chrome_options)
    broswer.get(url)
    page_source = broswer.page_source
    broswer.quit()

    return page_source


def _get_html_request(url, headers={}, cookies={}):
    r = requests.get(url, headers=headers, cookies=cookies)
    return r.text


def crawl(task):

    task_body = task["body"]
    # task_body = task # uncomment to test local
    user = task_body["user"]
    user_obj = {"user_obj": task_body.get("user_obj", {})}
    workspace_id = task_body["workspace_idx"]
    url = task_body["url"]
    if url and "." not in url and not url.endswith("/"):
        url = url + "/"
    html_tag = task_body["html_tag"]
    html_selector = task_body["html_selector"]
    upload_pdf = task_body["upload_pdf"]
    pdf_only = task_body["pdf_only"]
    titles = task_body["titles"]
    headless_broswer = task_body["headless_broswer"]
    max_depth = task_body["max_depth"]
    allowed_domain = task_body["allowed_domain"]
    root_domain = task_body["root_domain"]
    request_headers = task_body["request_headers"]
    start_depth = task_body["start_depth"]
    used_links = task_body["used_links"]
    cookie_string = task_body["cookie_string"]
    bearer_token = task_body["bearer_token"]
    cookie_payload = {}
    if cookie_string and cookie_string != "":
        for cookie in cookie_string.split(";"):
            parts = cookie.split('=')
            cookie_payload[parts[0].strip()] = parts[1]

    logger.info(f"crawling url: {url}")

    # function to upload pdf from url
    def _upload_pdf_from_url(download_url):
        logger.info(f"uploading pdf: {download_url}")
        is_indexed = nosql_db.db["document"].count_documents(
            {"workspace_id": workspace_id, "source_url": download_url},
        ) > 0
        if not is_indexed:
            logger.info(f"uploading pdf: {download_url}")
            pdf_request = urllib.request.Request(download_url)
            pdf_request.add_header("Cookie", cookie_string)
            pdf_request.add_header('User-Agent',
                                   'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36')
            response = urllib.request.urlopen(pdf_request)
            filename = download_url.split("/")[-1]
            stream = io.BytesIO(response.read())
            file = FileStorage(stream, filename=filename)
            doc_id = controller_upload(user, user_obj, workspace_id, file, return_raw=True)
            nosql_db.db["document"].find_one_and_update(
                {"id": doc_id},
                {"$set": {"source_url": download_url}},
            )
        else:
            logger.info(f"skipping pdf - already indexed: {download_url}")

    def _upload_pubmed_gz(url):
        is_indexed = nosql_db.db["document"].count_documents(
            {"workspace_id": workspace_id, "source_url": url},
        ) > 0
        if not is_indexed:
            logger.info(f"uploading pubmed gz: {url}")
            # to do test this with cookie later
            byte_stream = io.BytesIO(urllib.request.urlopen(url).read())
            gz_stream = gzip.GzipFile(fileobj=byte_stream, mode='rb')
            contexts = [ET.iterparse(gz_stream, events=('end',))]
            i=0
            for context in contexts:
                for event, elem in context:
                    if elem.tag == 'PubmedArticle':
                        article = elem
                        pmid = article.findall("./MedlineCitation/PMID")
                        if len(pmid) > 0:
                            filename = pmid[0].text + ".xml"
                            logger.info(f"uploading pubmed extracted file: {i} - {filename}")
                            file_stream = io.BytesIO(ET.tostring(article))
                            file = FileStorage(file_stream, filename=filename)
                            doc_id = controller_upload(user, user_obj, workspace_id, file, return_raw=True)
                            nosql_db.db["document"].find_one_and_update(
                                {"id": doc_id},
                                {"$set": {"source_url": url}},
                            )
                        else:
                            logger.error(f"skipping pubmed gz - no pmid: {url}")
                        i = i + 1
            else:
                logger.info(f"skipping pubmed gz - already indexed: {url}")

    # function to upload html from bs4
    def upload_html_page(soup):
        is_indexed = nosql_db.db["document"].count_documents(
            {"workspace_id": workspace_id, "source_url": url},
        ) > 0
        if not is_indexed:
            title_elem = soup.find(titles)
            filename = soup.find(titles).text if title_elem else None
            if not filename:
                filename = soup.find("p").text
            filename = filename[:80] + ".html"

            if html_selector or html_tag:
                container = soup.find(html_tag, html_selector)
            else:
                container = _find_main(soup)

            stream = io.BytesIO(str.encode(str(container)))
            file = FileStorage(stream, filename=filename)
            logger.info(f"uploading html page: {url}")
            doc_id = controller_upload(
                user,
                user_obj,
                workspace_id,
                file,
                return_raw=True,
                parent_task="html_crawling",
            )

            nosql_db.db["document"].find_one_and_update(
                {"id": doc_id},
                {"$set": {"source_url": url}},
            )
        else:
            logger.info(f"skipping html page - already indexed: {url}")

    # upload pdf if it crawls to one
    if url.endswith(".pdf") and upload_pdf:
        _upload_pdf_from_url(url)
        return

    if "ftp.ncbi.nlm.nih.gov/pubmed" in url and url.endswith(".gz"):
        _upload_pubmed_gz(url)
        return
    # get url with request or headless
    try:
        if headless_broswer:
            text = _get_html_headless(url)
        else:
            text = _get_html_request(url, headers=request_headers, cookies=cookie_payload)
        text = _remove_tags(text)
        soup = BeautifulSoup(text, features="lxml")
    except Exception as ex:
        return


    # empty page
    if not soup:
        return

    # upload this page
    if not pdf_only:
        logger.info(f"uploading html: {url}")
        upload_html_page(soup)

    logger.info(f"depths: {start_depth}, {max_depth}")
    # check depth before searching for more links to crawl
    if start_depth >= max_depth:
        return

    # search by "a" tag and href for more links to crawl
    for a in soup.find_all("a"):
        try:
            # "a" tag with no links
            if "href" not in a.attrs:
                continue
            href = a.attrs["href"].strip()
            # cases where it's just "/" or "/#"
            if len(href) < 3:
                continue
            is_file = "/" not in href
            # get the root url
            link = href
            if href.startswith("/"):
                link = root_domain + href
            elif not href.startswith("http"):
                link = os.path.join(os.path.dirname(url), href)

            logger.info(f"has link: {link}")

            if is_file and link.endswith(".pdf") and upload_pdf:
                _upload_pdf_from_url(link)
                continue

            if "#" in link:
                link = link[: link.find("#")]
            if "?" in link:
                link = link[: link.find("?")]

            # check if this link is allowed or has been crawled
            # also do not crawl parent pages
            if (
                allowed_domain != "*" and allowed_domain not in link
            ) or link in used_links or link in url:
                logger.info(f"skipping link: {link}, {allowed_domain}, {allowed_domain not in link}, {link in url}, {link in used_links}")
                continue
            else:
                used_links.append(link)
            # go to next depth
            # crawl(link, depth=depth + 1) # uncomment to test without pubsub
            task_body["start_depth"] = start_depth + 1
            task_body["url"] = link
            # crawl(task_body)
            task["body"] = task_body
            res = producer.send(task)
            if not res:
                # raise RuntimeError("can not send task to queue")
                crawl(task)

        except RuntimeError as e:
            raise e
        except Exception as e:
            logger.error(e, exc_info=True)
            continue
