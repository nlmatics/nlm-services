#!/usr/bin/env python3
import logging
import os
import subprocess
from threading import Thread

from tika.tika import checkPortIsOpen

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def check_tika():
    if "TIKA_SERVER_JAR" not in os.environ:
        return
    tika_file = os.getenv("TIKA_SERVER_JAR")
    if "file:///" in tika_file:
        tika_file = tika_file.replace("file:///", "")

    cmd = ["java", "-jar", tika_file]
    tika_config_file = os.getenv("TIKA_CONFIG_FILE", "")
    if tika_config_file:
        cmd = cmd + ["-config", tika_config_file]

    while True:
        if checkPortIsOpen():
            logger.info("tika server is maintained by third-party process, exiting")
            return

        with subprocess.Popen(cmd) as p:
            logger.info("running tika server")
            p.wait()
            logger.info("tika server dead, restarting")


# Server Hooks
def on_starting(server):
    background_thread = Thread(target=check_tika)
    background_thread.daemon = True
    background_thread.start()
