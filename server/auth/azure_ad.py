import logging
import os

import msal

from server.auth.auth_provider import AuthProvider

logger = logging.getLogger(__name__)


class AzureADAuthProvider(AuthProvider):
    def __init__(self):
        super().__init__()

        # CLIENT_ID ID of app registration
        self.CLIENT_ID = os.getenv(
            "AZURE_AD_CLIENT_ID"
        )

        # TENANT_ID ID of app registration
        self.TENANT_ID = os.getenv(
            "AZURE_AD_TENANT_ID"
        )

        self.CLIENT_SECRET = os.getenv(
            "AZURE_AD_CLIENT_SECRET"
        )

        self.AUTHORITY = f"https://login.microsoftonline.com/{self.TENANT_ID}"  # For multi-tenant app

        self.SCOPE = ["User.ReadBasic.All"]

    def callback(self, request):
        """
        convert authorization code to
        """

        if "error" in request.args:  # Authentication/Authorization failure
            raise Exception(request.args["error"])

        if "code" not in request.args:
            raise Exception("invalid request")

        result = self._build_msal_app().acquire_token_by_authorization_code(
            request.args["code"],
            scopes=self.SCOPE,  # Misspelled scope would cause an HTTP 400 error here
            redirect_uri=f"{self.BACKEND_URL}/api/auth/callback",
        )
        if "error" in result:  # Authentication/Authorization failure
            raise Exception(result["error"])

        email = result["id_token_claims"]["preferred_username"]
        name = result["id_token_claims"]["name"]

        if "first_name" not in name or "last_name" not in name:
            name = name.rsplit(" ", 1)
            if len(name) == 2:
                first_name, last_name = name
            elif len(name) == 1:
                first_name = name[0]
                last_name = ""
            else:
                first_name = ""
                last_name = ""
        else:
            first_name = name.get("first_name", "")
            last_name = name.get("last_name", "")

        user = self.auth_user(
            verified=True,
            email=email,
            first_name=first_name,
            last_name=last_name,
            return_user=True,
        )
        return self.generate_token(user=user)

    def login_url(self):
        url = self._build_msal_app().get_authorization_request_url(
            self.SCOPE,
            redirect_uri=f"{self.BACKEND_URL}/api/auth/callback",
        )
        return url

    def logout_url(self, _error=None, _err_code=None):

        url = f"{self.AUTHORITY}/oauth2/v2.0/logout?post_logout_redirect_uri={self.BACKEND_URL}/api/auth/logout"
        return url

    def _build_msal_app(self):
        """
        build msal app to communicate with Azure AD
        """
        return msal.ConfidentialClientApplication(
            self.CLIENT_ID,
            authority=self.AUTHORITY,
            client_credential=self.CLIENT_SECRET,
        )
