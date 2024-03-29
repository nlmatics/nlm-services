import logging
import os

import google.auth.transport.requests
import google.oauth2.id_token

from server.auth.auth_provider import AuthProvider

# import server.config as cfg

HTTP_REQUEST = google.auth.transport.requests.Request()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ALLOWED_DOMAINS = os.getenv("ALLOWED_DOMAINS", "").split()


class FirebaseAuthProvider(AuthProvider):
    def __init__(self):
        pass

    def auth_user(self, token):
        claims = google.oauth2.id_token.verify_firebase_token(token, HTTP_REQUEST)
        if claims:
            email = claims["email"]
            # !!!!!Security sensitive code - DO NOT CHANGE WITHOUT REVIEW
            if ALLOWED_DOMAINS:
                domain = email.split("@")[1]
                if domain in ALLOWED_DOMAINS:
                    return email
                else:
                    logger.error(f"request from domain: {domain} blocked")
                    raise Exception("Unauthorized access")
            else:
                return email
        #     user = nosql_db.get_user_by_email(claims['email'])
        #     if user:
        #         logger.info(f"user {claims['email']} found")
        #         return user
        else:
            logger.error("auth claims object is null")
            return None
