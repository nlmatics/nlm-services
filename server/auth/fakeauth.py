import os
import re

from server.auth.auth_provider import AuthProvider


class FakeAuthProvider(AuthProvider):
    def __init__(self):
        self.email = os.getenv("DEFAULT_USER", "default@nlmatics.com")
        super().__init__()

    def callback(self, *args, **kwargs):
        name = email = self.email

        user = self.auth_user(
            verified=True,
            email=email,
            first_name=name,
            last_name="",
            return_user=True,
        )
        return self.generate_token(user=user)
    
    def auth_user(self, token=None, verified=False, return_user=False, **kwargs):
        # token as email
        if token:
            regex = r"^[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w{2,3}$"
            if re.search(regex, token):
                if token != self.email:
                    raise ValueError(f"{token} is not allowed.")
                return super().auth_user(
                    email=token,
                    verified=True,
                    return_user=return_user,
                    firstname=self.email,
                    last_name="",
                    **kwargs,
                )
        return super().auth_user(
            token=token,
            verified=verified,
            return_user=return_user,
            **kwargs,
        )

    def login_url(self):
        print("returning: ", f"{self.BACKEND_URL}/api/auth/callback")
        return f"{self.BACKEND_URL}/api/auth/callback"

    def logout_url(self, _error=None, _err_code=None):
        url = f"{self.BACKEND_URL}/api/auth/logout"
        return url
