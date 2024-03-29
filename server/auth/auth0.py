import base64
import hashlib
import json
import logging
import os

import jose
import urllib3
from jose import jwt

from server.auth.auth_provider import AuthProvider
from server.utils import str_utils

CONTENT_TYPE_JSON = "application/json"
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Auth0Provider(AuthProvider):
    def __init__(self):
        super().__init__()

        self.ALGORITHMS = ["RS256"]

        self.AUTH0_DOMAIN = os.getenv("AUTH0_DOMAIN")
        self.CLIENT_ID = os.getenv(
            "AUTH0_CLIENT_ID"
        )
        self.AUDIENCE = os.getenv("AUTH0_AUDIENCE", "http://localhost/api")
        self.CLIENT_SECRET = os.getenv(
            "AUTH0_CLIENT_SECRET"
        )
        # M2M Application specific
        self.M2M_EMAIL_DOMAIN = os.getenv("M2M_EMAIL_DOMAIN")
        self.M2M_AUTH0_DOMAIN = os.getenv("M2M_AUTH0_DOMAIN")
        self.M2M_CLIENT_ID = os.getenv(
            "M2M_AUTH0_CLIENT_ID"
        )
        self.M2M_AUDIENCE = os.getenv(
            "M2M_AUTH0_AUDIENCE"
        )
        self.M2M_CLIENT_SECRET = os.getenv(
            "M2M_AUTH0_CLIENT_SECRET"
        )
        # MANAGEMENT API specific
        self.MANAGEMENT_AUDIENCE = os.getenv(
            "MANAGEMENT_AUDIENCE"
        )

        self.conn = urllib3.PoolManager()
        self.mgmt_access_key = None

    def callback(self, request):
        """
        convert authorization code to
        """

        if "error" in request.args:  # Authentication/Authorization failure
            raise Exception(
                request.args["error"],
                request.args.get("error_description", ""),
            )

        if "code" not in request.args:
            raise Exception("invalid request")

        payload = {
            "grant_type": "authorization_code",
            "client_id": self.CLIENT_ID,
            "code": request.args["code"],
            "redirect_uri": self.BACKEND_URL,
        }

        resp = self.conn.request(
            "POST",
            f"https://{self.AUTH0_DOMAIN}/oauth/token",
            body=json.dumps(payload),
            headers={"content-type": CONTENT_TYPE_JSON},
        )

        if resp.status != 200:
            raise RuntimeError(resp.data)

        return json.loads(resp.data)

    def login_url(self):
        url = (
            f"https://{self.AUTH0_DOMAIN}/authorize?"
            "response_type=code"
            "&scope=openid+profile+email+offline_access"
            "&response_mode=query"
            f"&client_id={self.CLIENT_ID}"
            f"&redirect_uri={self.BACKEND_URL}/api/auth/callback"
            f"&audience={self.AUDIENCE}"
        )
        return url

    def logout_url(self, error=None, error_code=None):
        url = (
            f"https://{self.AUTH0_DOMAIN}/v2/logout?"
            f"returnTo={self.BACKEND_URL}/api/auth/logout"
            f"&client_id={self.CLIENT_ID}"
        )
        if error:
            url = (
                f"https://{self.AUTH0_DOMAIN}/v2/logout?"
                f"returnTo={self.BACKEND_URL}/api/auth/logout?errorCode={error_code}"
                f"&client_id={self.CLIENT_ID}"
            )
        return url

    def verify_token(self, token, is_management_token=False):
        jsonurl = self.conn.request(
            "GET",
            f"https://{self.AUTH0_DOMAIN}/.well-known/jwks.json",
        )
        jwks = json.loads(jsonurl.data)
        unverified_header = jwt.get_unverified_header(token)
        rsa_key = {}
        for key in jwks["keys"]:
            if key["kid"] == unverified_header["kid"]:
                rsa_key = {
                    "kty": key["kty"],
                    "kid": key["kid"],
                    "use": key["use"],
                    "n": key["n"],
                    "e": key["e"],
                }
        if rsa_key:
            try:
                payload = jwt.decode(
                    token,
                    rsa_key,
                    algorithms=self.ALGORITHMS,
                    audience=self.AUDIENCE,
                    issuer=f"https://{self.AUTH0_DOMAIN}/",
                )
                # translate auth0 customized profile in payload.
                # Check Auth0 Dashboard -> rules -> 'add email in access token'
                for key in list(payload.keys()):
                    if key.startswith("https://nlmatics.com/email"):
                        payload[key.split("/")[-1]] = payload.pop(key)

                return payload
            except jose.jwt.JWTClaimsError:
                logger.info("Trying to validate using M2M AUDIENCE")
                return self.validate_m2m_token(token, rsa_key, is_management_token)
            except Exception as e:
                logger.error(f"Error while validating the token: {e}")
                raise e

        raise RuntimeError("Unknown Error")

    def validate_m2m_token(self, token, rsa_key, is_management_token=False):
        """
        Validates the received token using M2M Audience.
        :param token: Received token from the user.
        :param rsa_key: RSA Key
        :param is_management_token: Are we dealing with management token?
        :return: Payload with user profile information embedded in it
        """
        # Try validating using M2M AUDIENCE
        try:
            payload = jwt.decode(
                token,
                rsa_key,
                algorithms=self.ALGORITHMS,
                audience=self.MANAGEMENT_AUDIENCE
                if is_management_token
                else self.M2M_AUDIENCE,
                issuer=f"https://{self.AUTH0_DOMAIN}/",
            )
            # translate auth0 customized profile in payload.
            # Check Auth0 Dashboard -> rules -> 'add email in access token'
            for key in list(payload.keys()):
                # for M2M Users, the actual email of the user is in the name field of payload
                if key.startswith("https://nlmatics.com/name"):
                    payload["email"] = payload.pop(key)
                if key.startswith("https://nlmatics.com/email"):
                    payload["m2m_email"] = payload.pop(key)

            return payload
        except Exception as ex:
            logger.error(f"Error while validating M2M token: {ex}")
            raise ex

    def generate_token(self, token):
        payload = {
            "client_id": self.CLIENT_ID,
            "grant_type": "refresh_token",
            "refresh_token": token,
        }
        resp = self.conn.request(
            "POST",
            f"https://{self.AUTH0_DOMAIN}/oauth/token",
            body=json.dumps(payload),
            headers={"content-type": CONTENT_TYPE_JSON},
        )

        if resp.status != 200:
            raise RuntimeError(resp.data)

        return_data = json.loads(resp.data)
        payload = self.verify_token(return_data["access_token"])
        return_data["email"] = payload.get("email", "")
        return return_data

    def generate_developer_key(self, user):
        """
        1. Generate / Regenerate developer key from the user profile (email_id) and secret.
        2.
            a. Create a fake user in Auth0 with the generated email id and "developer key" as password
              (if generating for the first time).
            b. Else retrieve the user and then update the password.
        :param user: User email_id object retrieved from the database.
        :return: The generated unique developer token
        """
        # Generate the developer token
        app_id = self.generate_unique_app_id(user)
        m2m_email_id = app_id + "@" + self.M2M_EMAIL_DOMAIN
        developer_key = self.generate_unique_token(app_id)

        # Try creating a new user
        payload = {
            "client_id": self.M2M_CLIENT_ID,
            "email": m2m_email_id,
            "password": developer_key,
            "connection": "Username-Password-Authentication",
            "name": user,
        }

        resp = self.conn.request(
            "POST",
            f"https://{self.M2M_AUTH0_DOMAIN}/dbconnections/signup",
            body=json.dumps(payload),
            headers={"content-type": CONTENT_TYPE_JSON},
        )

        if resp.status == 400:
            json_resp = json.loads(resp.data)
            if json_resp["code"] == "invalid_signup":
                self.retrieve_and_update_m2m_data(m2m_email_id, developer_key)
            else:
                raise RuntimeError(resp.data)
        elif resp.status != 200:
            raise RuntimeError(resp.data)

        return app_id, developer_key

    def generate_new_mgmt_access_key(self):
        """
        Generate a new management access key
        :return: new management access key
        """
        # Retrieve the Management Access Key
        access_key_payload = {
            "client_id": self.M2M_CLIENT_ID,
            "client_secret": self.M2M_CLIENT_SECRET,
            "audience": self.MANAGEMENT_AUDIENCE,
            "grant_type": "client_credentials",
        }

        access_key_resp = self.conn.request(
            "POST",
            f"https://{self.M2M_AUTH0_DOMAIN}/oauth/token",
            body=json.dumps(access_key_payload),
            headers={"content-type": CONTENT_TYPE_JSON},
        )

        if access_key_resp.status != 200:
            raise RuntimeError(access_key_resp.data)

        self.mgmt_access_key = json.loads(access_key_resp.data)["access_token"]
        return self.mgmt_access_key

    def retrieve_mgmt_access_key(self):
        """
        Validates the current saved management access key and
        if expired retrieve management access key.
        :return: Management Access Key
        """
        try:
            if self.mgmt_access_key and self.verify_token(
                self.mgmt_access_key,
                is_management_token=True,
            ):
                # Management Access Key is valid.
                return self.mgmt_access_key
        except jose.ExpiredSignatureError as e:
            logger.info(f"Management Access Key Expired {str(e)}")
        return self.generate_new_mgmt_access_key()

    def retrieve_user_data(self, mgmt_access_key, email):
        """
        Retrieve user data from Auth0 using Management Access Key and Email
        :param mgmt_access_key: Management Access Key
        :param email: Email Id of the user
        :return: Returns user profile in dictionary
        """
        resp = self.conn.request(
            "GET",
            f"https://{self.M2M_AUTH0_DOMAIN}/api/v2/users-by-email?email={email.lower()}",
            headers={
                "content-type": CONTENT_TYPE_JSON,
                "Authorization": f"Bearer {mgmt_access_key}",
            },
        )

        if resp.status != 200:
            raise RuntimeError(
                "Cannot create / retrieve information for generating developer key",
            )
        return json.loads(resp.data)

    def retrieve_and_update_m2m_data(self, m2m_email_id, developer_key):
        """
        Retrieves the user using their m2m_email_id and then updates the password
        :param m2m_email_id: M2M mail of the user.
        :param developer_key: New DeveloperApiKey
        :return: Void / Exception
        """
        # Try to retrieve the m2m_user_info
        mgmt_access_token = self.retrieve_mgmt_access_key()
        # Try to retrieve the user using the m2m_email_id
        users_data = self.retrieve_user_data(mgmt_access_token, m2m_email_id)
        if len(users_data) == 0:
            raise RuntimeError(
                "Cannot retrieve information for generating developer key",
            )
        else:
            auth_userid = users_data[0]["user_id"]
            # Update the new secret / password
            self.update_m2m_user_password(developer_key, auth_userid, mgmt_access_token)

    def update_user_profile(self, user_id, mgmt_access_token, payload):
        """
        Updates the user profile using the Management Access Key
        :param user_id: Auth0 User ID
        :param mgmt_access_token: Management Access Key
        :param payload: Payload structure that needs to be updated.
        :return: VOID
        """
        resp = self.conn.request(
            "PATCH",
            f"https://{self.M2M_AUTH0_DOMAIN}/api/v2/users/{user_id}",
            body=json.dumps(payload),
            headers={
                "content-type": CONTENT_TYPE_JSON,
                "Authorization": f"Bearer {mgmt_access_token}",
            },
        )

        if resp.status != 200:
            raise RuntimeError(resp.data)

    def update_m2m_user_password(self, new_password, user_id, mgmt_access_token):
        """
        Updates the password for an m2m user (fake user we have created).
        :param new_password: New password to be updated.
        :param user_id: Auth0 user id for which we have to update the password.
        :param mgmt_access_token: Auth0 Management Access Token
        :return: void
        """
        payload = {
            "password": new_password,
        }
        self.update_user_profile(user_id, mgmt_access_token, payload)

    def update_user_details_and_plan(
        self,
        email_id,
        first_name=None,
        last_name=None,
        subscription_plan=None,
    ):
        """
        Updates the user details (name & plan)
        :param email_id: Email id of the user.
        :param first_name: First Name (Given Name)
        :param last_name: Last name (Family Name)
        :param subscription_plan: Plan for the user.
        :return:
        """
        if (not first_name) and (not last_name) and (not subscription_plan):
            logger.info(f"Nothing to update for {email_id}")
        # Retrieve the Access Token
        mgmt_access_token = self.retrieve_mgmt_access_key()
        # Try to retrieve the user
        users_data = self.retrieve_user_data(mgmt_access_token, email_id)
        payload = {}
        if first_name:
            payload["given_name"] = first_name
        if last_name:
            payload["family_name"] = last_name
        if subscription_plan:
            payload["app_metadata"] = {
                "subscription_plan": subscription_plan,
            }
        self.update_user_profile(users_data[0]["user_id"], mgmt_access_token, payload)

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

        m2m_email_id = app_id + "@" + self.M2M_EMAIL_DOMAIN
        # Create a new user
        payload = {
            "client_id": self.M2M_CLIENT_ID,
            "client_secret": self.M2M_CLIENT_SECRET,
            "username": m2m_email_id,
            "password": developer_key,
            "realm": "Username-Password-Authentication",
            "scope": "openid+profile+email+offline_access",
            "audience": self.M2M_AUDIENCE,
            "grant_type": "http://auth0.com/oauth/grant-type/password-realm",
        }

        resp = self.conn.request(
            "POST",
            f"https://{self.M2M_AUTH0_DOMAIN}/oauth/token",
            body=json.dumps(payload),
            headers={"content-type": CONTENT_TYPE_JSON},
        )

        if resp.status != 200:
            if resp.status == 403:
                raise RuntimeError("Invalid API Key")
            else:
                raise RuntimeError(resp.data)

        return json.loads(resp.data)["access_token"]

    def generate_unique_token(self, email_id):
        """
        Generate unique token from the email id & ClientSecret (M2M APP)
        :param email_id: fake email id of the user @ M2M_EMAIL_DOMAIN
        :return: base64 encoded (ASCII) of the unique token
        """
        sha2 = hashlib.sha256()
        sha2.update(str(email_id).encode())
        sha2.update(str(self.M2M_CLIENT_SECRET).encode())
        sha2.update(str_utils.timestamp_as_str().encode())
        hex_digest = sha2.hexdigest()
        b64 = base64.b64encode(hex_digest.encode()).decode("ascii")
        return b64

    def generate_unique_app_id(self, email_id):
        """
        Generate unique app_id from the email id & ClientSecret (M2M APP)
        :param email_id: Email id of the user for whom we are generating unique app_id
        :return: base64 encoded (ASCII) of the unique app_id
        """
        sha2 = hashlib.blake2b(digest_size=10)
        sha2.update(str(email_id).encode())
        sha2.update(str(self.M2M_CLIENT_SECRET).encode())
        hex_digest = sha2.hexdigest()
        b64 = base64.b64encode(hex_digest.encode()).decode("ascii")
        return b64

    def revoke_refresh_token(self, token):
        payload = {
            "client_id": self.CLIENT_ID,
            "client_secret": self.CLIENT_SECRET,
            "token": token,
        }
        resp = self.conn.request(
            "POST",
            f"https://{self.AUTH0_DOMAIN}/oauth/revoke",
            body=json.dumps(payload),
            headers={"content-type": CONTENT_TYPE_JSON},
        )

        if resp.status != 200:
            raise RuntimeError(resp.data)
