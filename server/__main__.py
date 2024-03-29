#!/usr/bin/env python3
import logging
import os

import connexion
from flask_cors import CORS
from nlm_utils.utils import ensure_bool

from server import encoder

LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logging.getLogger("pika").setLevel(logging.ERROR)

enable_python_debug = ensure_bool(os.getenv("PYTHON_DEBUG", False))
nlm_parser_openapi = ensure_bool(os.getenv("NLM_PARSER_OPENAPI", False))
specification_dir = "./swagger/"
if nlm_parser_openapi:
    specification_dir = "./swagger_openapi/"

app = connexion.App(__name__, specification_dir=specification_dir)
app.app.json_encoder = encoder.JSONEncoder
app.add_api(
    "swagger.yaml",
    arguments={"title": "NLM Service API"},
    pythonic_params=True,
)
CORS(
    app.app,
    resources={
        r"/api/*": {
            "origins": os.getenv("DOMAIN", "*"),
            "methods": ["GET, POST, OPTIONS"],
            "send_wildcard": False,
        },
    },
)


@app.app.after_request
def add_security_headers(resp):
    resp.headers["X-Frame-Options"] = "SAMEORIGIN"
    resp.headers["X-XSS-Protection"] = "1; mode=block"
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["Content-Security-Policy"] = (
        "base-uri 'self'; upgrade-insecure-requests; "
        "script-src 'self' 'sha256-BJOmd/Baqs9eCh2+CgN3XKXGZG9EEnJq9/Kfbdjb+II=' "
        "'sha256-2SB4rwnV3AwnkK9/orfbiXyudVukHhbEULqewMDlWoM='; object-src 'none'"
    )
    return resp


if __name__ == "__main__":
    app.run(port=5001, debug=enable_python_debug)
