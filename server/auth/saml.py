import base64
import hashlib
import os
from urllib.parse import urlparse

from flask import request
from nlm_utils.utils import ensure_bool
from onelogin.saml2.auth import OneLogin_Saml2_Auth

from server.auth.auth_provider import AuthProvider
from server.storage import nosql_db

ENABLE_SAML_HTTPS = ensure_bool(os.getenv("ENABLE_SAML_HTTPS", False))
DEV_API_PROVIDER = os.getenv("DEV_API_PROVIDER", "")
DEV_API_SECRET = os.getenv("LICENSE_KEY", "")
M2M_EMAIL_DOMAIN = os.getenv("M2M_EMAIL_DOMAIN")


def init_saml_auth(req):
    # uses onelogin library to initialize saml config
    return OneLogin_Saml2_Auth(
        req,
        custom_base_path=os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "saml_config",
        ),
    )


def prepare_flask_request(request):
    url_data = urlparse(request.url)
    return {
        "https": "on"
        if ENABLE_SAML_HTTPS
        else "on"
        if request.scheme == "https"
        else "off",
        "http_host": request.host,
        "server_port": url_data.port,
        "script_name": request.path,
        "get_data": request.args.copy(),
        "post_data": request.form.copy(),
    }


def generate_unique_token(email_id):
    """
    Generate unique token from the email id & ClientSecret (M2M APP)
    :param email_id: fake email id of the user @ M2M_EMAIL_DOMAIN
    :return: base64 encoded (ASCII) of the unique token
    """
    sha2 = hashlib.sha256()
    sha2.update(str(email_id).encode())
    sha2.update(str(DEV_API_SECRET).encode())
    hex_digest = sha2.hexdigest()
    b64 = base64.b64encode(hex_digest.encode()).decode("ascii")
    return b64


def generate_app_id(email_id):
    """
    Generate b64 version of the email ID
    :param email_id:
    :return: base64 encoded (ASCII) of the unique token
    """
    b64 = base64.b64encode(email_id.encode("ascii")).decode("ascii")
    return b64


def decode_app_id(app_id):
    """
    Retrieves the user information from the app_id
    :param app_id:
    :return: Email ID
    """
    dec_res = base64.b64decode(app_id.encode("ascii")).decode("ascii")
    return dec_res


class SAMLAuthProvider(AuthProvider):
    def __init__(self):
        super().__init__()

    def callback(self, request):
        req = prepare_flask_request(request)
        # prepare and process saml response
        auth = init_saml_auth(req)
        auth.process_response()

        # check error from saml response
        if auth.get_errors():
            error_reason = auth.get_last_error_reason()
            raise Exception(f"Authentication failed: {error_reason}")

        self.logger.info(
            f"Authenticated via SAML: {auth.get_attributes()}, NameId: {auth.get_nameid()}",
        )

        # generate access and refresh tokens
        user = self.auth_user(
            verified=True,
            email=auth.get_nameid(),
            first_name=auth.get_attributes().get("firstname", [""])[0],
            last_name=auth.get_attributes().get("lastname", [""])[0],
            return_user=True,
        )
        return self.generate_token(user=user)

    def login_url(self):
        req = prepare_flask_request(request)
        auth = init_saml_auth(req)
        return auth.login()

    def logout_url(self, _error=None, _err_code=None):
        req = prepare_flask_request(request)
        auth = init_saml_auth(req)
        return auth.logout()

    def generate_developer_key(self, user):
        """
        1. Generate / Regenerate developer key from the user profile (email_id) and secret.
        2.
            a. For SAML Auth Provider the developer key and secret will be generated locally.
        :param user: User email_id object retrieved from the database.
        :return: The generated unique developer token
        """
        # Check the DEV_API_PROVIDER
        if DEV_API_PROVIDER != "local" or not DEV_API_SECRET:
            err_msg = "Unknown DEV_API_PROVIDER or DEV_API_SECRET"
            self.logger.info(err_msg)
            raise RuntimeError(err_msg)
        # Generate the developer token
        app_id = generate_app_id(user)
        m2m_email_id = app_id + "@" + M2M_EMAIL_DOMAIN
        developer_key = generate_unique_token(m2m_email_id)

        return app_id, developer_key

    def generate_developer_access_token(self, app_id, developer_key):
        """
        1. Generate access token for use with the APIs.
        :param app_id: app_id associated with this developer key.
        :param developer_key: This will act as the secret
        :return: Access Token
        """
        if not app_id or not developer_key:
            raise RuntimeError(
                "Input requirements failed. Requires developer_key and app_id",
            )

        email_id = decode_app_id(app_id)
        m2m_email_id = app_id + "@" + M2M_EMAIL_DOMAIN
        dev_key = generate_unique_token(m2m_email_id)
        if developer_key != dev_key:
            raise RuntimeError(
                "Invalid APP ID, developer key combination",
            )
        # Retrieve the user information
        user = nosql_db.get_user_by_email(email_id)
        if not user:
            raise RuntimeError(
                "Invalid User trying to access",
            )
        if not user.has_developer_account:
            raise RuntimeError(
                "User does not have a developer account",
            )
        # generate access
        tokens = self.generate_token(user=user, m2m_email_id=m2m_email_id)
        return tokens["access_token"]
