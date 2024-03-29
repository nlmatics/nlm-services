import logging

# import server.config as cfg

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class AuthProvider:
    __provider = None
    supported_implementations = ["fakeauth", "azure_ad", "saml", "auth0"]

    @classmethod
    def instance(cls, impl):
        if impl not in AuthProvider.supported_implementations:
            raise Exception(f"unknown security provider {impl}")

        if AuthProvider.__provider is None:
            logger.info(f"Using auth provider: {impl.lower()}")
            if impl.lower() == "firebase":
                from server.auth.firebase import FirebaseAuthProvider

                AuthProvider.__provider = FirebaseAuthProvider()

            elif impl.lower() == "fakeauth":
                from server.auth.fakeauth import FakeAuthProvider

                AuthProvider.__provider = FakeAuthProvider()

            elif impl.lower() == "azure_ad":
                from server.auth.azure_ad import AzureADAuthProvider

                AuthProvider.__provider = AzureADAuthProvider()

            elif impl.lower() == "auth0":
                from server.auth.auth0 import Auth0Provider

                AuthProvider.__provider = Auth0Provider()

            elif impl.lower() == "saml":
                from server.auth.saml import SAMLAuthProvider

                AuthProvider.__provider = SAMLAuthProvider()
        return AuthProvider.__provider


# def authenticate(token):
#     try:
#         # logger.info(f"Authenticating request using token {token}")
#         if token:
#             auth_provider = AuthProvider.instance(
#                 os.getenv("AUTH_PROVIDER", "firebase").lower(),
#             )
#             user = auth_provider.auth_user(token, return_email=True)
#             if user:
#                 logger.info(f"token authenticated, user {user}")
#                 return {"scope": ["user"], "sub": user, "email_id": user}
#         logger.error("auth failed, invalid token")
#         return None
#     except Exception:
#         logger.error(f"exception while authenting user, err: {traceback.format_exc()}")
#         return None
