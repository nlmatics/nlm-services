import logging
import os
import re
from datetime import datetime
from datetime import timedelta

import jose
from jose import jwt
from nlm_utils.utils import ensure_bool
from nlm_utils.utils import ensure_integer
from pytz import timezone

from server.controllers.subscription_controller import create_stripe_customer
from server.models import FieldBundle
from server.models import User
from server.models.prefered_workspace import PreferedWorkspace
from server.models.workspace import Workspace
from server.storage import nosql_db
from server.utils import metric_utils
from server.utils import str_utils

DEFAULT_SUBSCRIPTION_PLAN = os.getenv("DEFAULT_SUBSCRIPTION_PLAN", "BASIC")
# ADMIN > EDITOR > VIEWER
DEFAULT_USER_ACCESS_TYPE = os.getenv("DEFAULT_USER_ACCESS_TYPE", "VIEWER")
DEFAULT_WORKSPACE_DOMAIN = os.getenv("DEFAULT_WORKSPACE_DOMAIN", "general")


class AuthProvider:

    ACCESS_TOKEN_EXPIRE_SECONDS = ensure_integer(
        os.getenv("ACCESS_TOKEN_EXPIRE_SECONDS", 60 * 60),
    )
    REFRESH_TOKEN_EXPIRE_SECONDS = 60 * 60 * 24 * 30

    def __init__(self, *args, **kwargs):

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        self.domain = os.getenv("DOMAIN", None)

        self.BACKEND_URL = os.getenv("BACKEND_URL")
        self.FRONTEND_URL = os.getenv("FRONTEND_URL")
        self.CHECK_ACCESS_TIME = ensure_bool(os.getenv("CHECK_ACCESS_TIME", False))

        self.allowed_tenants = set(os.getenv("ALLOWED_TENANTS", "*").split(","))
        self.allowed_tenant_rules = []
        for domain in self.allowed_tenants:
            if domain == "*":
                domain = r"^.*@.*\..*"
            elif domain.startswith("*"):
                domain = f"^.*@.*{domain[1:]}$"
            self.allowed_tenant_rules.append(re.compile(domain))

        self.logger.info(f"Allowed tenant: {self.allowed_tenants}")

        self.keys = os.getenv(
            "AUTH_SECRET"
        )

    def verify_token(self, token):
        try:
            return jwt.decode(token, self.keys)
        except jose.ExpiredSignatureError as e:
            # if the token has expired, it is at least from this provider.
            raise e
        except jose.JOSEError as e:  # the catch-all of Jose
            raise e
        except Exception as e:
            raise e

    def auth_user(
        self,
        token=None,
        verified=False,
        return_user=False,
        return_json=False,
        check_last_login_time=True,
        **kwargs,
    ):
        # verify by token
        payload = None
        app_name = None
        first_name = kwargs.get("first_name", "")
        last_name = kwargs.get("last_name", "")
        try:
            if not verified:
                payload = self.verify_token(token=token)
                email = payload["email"]
                app_name = payload.get("https://nlmatics.com/application_name", None)
                if not first_name:
                    first_name = payload.get(
                        "https://nlmatics.com/first_name",
                        payload.get("https://nlmatics.com/name", ""),
                    )
                if not last_name:
                    last_name = payload.get("https://nlmatics.com/last_name", "")
            else:
                email = kwargs["email"]
        except KeyError:
            raise ValueError("email is required when auth an verified user")

        try:
            # no need to verify existing user
            user = nosql_db.get_user_by_email(email, include_stripe_conf=True)
        # user not exist, create a new one
        except ValueError:
            # check if user in allowed tenant
            if any([len(x.findall(email)) != 0 for x in self.allowed_tenant_rules]):
                app_settings = None
                if app_name:
                    # Retrieve the application specific settings.
                    app_settings = nosql_db.get_application_settings(app_name)
                user = self.create_user(
                    first_name=first_name,
                    last_name=last_name,
                    email=email,
                    app_settings=app_settings,
                    app_name=app_name,
                )
            else:
                raise ValueError(
                    f"Your account {email} is not allowed to access NLMatics. Please contact our support team.",
                )
        if payload and payload.get("m2m_email", ""):
            check_last_login_time = False
        if (
            payload
            and check_last_login_time
            and user.is_logged_in
            and self.CHECK_ACCESS_TIME
        ):
            last_login_time = datetime.strptime(
                user.last_login,
                "%Y-%m-%d %H:%M:%S %Z%z",
            ).timestamp()
            if abs(payload["iat"] - last_login_time) > 30:
                self.logger.info(
                    f" Wrong access token with invalid issue time received for {user.email_id}",
                )
                raise ValueError("Invalid access token")
        if return_user:
            if return_json:
                user_json = user.to_dict()
                user_json["m2m_email"] = payload.get("m2m_email", "") if payload else ""
                user_json["app_name"] = app_name
                return user_json
            else:
                return user
        else:
            return user.email_id

    def create_user(
        self,
        first_name,
        last_name,
        email,
        app_settings=None,
        app_name="",
    ):
        app_settings = app_settings or {}
        user = User(email_id=email, first_name=first_name, last_name=last_name)
        user.id = str_utils.generate_user_id(user.email_id)
        user.created_on = str_utils.timestamp_as_str()
        user.active = True
        # Add default 30 days expiry_time
        tz = timezone("UTC")
        default_expiry_days = app_settings.get("users", {}).get(
            "default_expiry_time",
            30,
        )
        user.expiry_time = (
            datetime.now(tz) + timedelta(days=default_expiry_days)
        ).strftime(
            "%Y-%m-%d %H:%M:%S %Z%z",
        )
        # Add DEFAULT_SUBSCRIPTION_PLAN if none provided.
        if not user.subscription_plan:
            user.subscription_plan = app_settings.get("users", {}).get(
                "default_subscription_plan",
                DEFAULT_SUBSCRIPTION_PLAN,
            )
        if not user.access_type:
            user.access_type = app_settings.get("users", {}).get(
                "default_user_access_type",
                DEFAULT_USER_ACCESS_TYPE,
            )
        nosql_db.create_user(user)
        user = nosql_db.get_user_by_email(user.email_id)
        # Update user ACL if present with the email id.
        nosql_db.update_user_id_for_acl(user.id, user.email_id)

        # Create default usage metrics for the user.
        metric_utils.add_default_usage_metrics(user.id)

        create_default_workspace = app_settings.get("users", {}).get(
            "create_default_workspace",
            True,
        )

        if create_default_workspace:
            # create default workspace for user
            default_workspace = Workspace(
                id=str_utils.generate_workspace_id(user.id, "default"),
                name=app_settings.get("workspace", {}).get("default_name", "default"),
                user_id=user.id,
                active=True,
                created_on=str_utils.timestamp_as_str(),
            )

            default_workspace_settings = app_settings.get("workspace", {}).get(
                "default_settings",
                {
                    "domain": DEFAULT_WORKSPACE_DOMAIN,
                    "search_settings": {
                        "table_search": False,
                    },
                },
            )
            index = default_workspace_settings.get("index_settings", {}).get(
                "index",
                "",
            )
            if index and index.endswith("_"):
                last_used_es_index = (
                    nosql_db.get_nlm_settings("last_used_es_index") or 0
                )
                default_workspace_settings["index_settings"]["index"] = index + str(
                    last_used_es_index,
                )
                max_indices_allowed = app_settings.get("workspace", {}).get(
                    "max_indices_to_create",
                    300,
                )
                nosql_db.update_nlm_settings(
                    "last_used_es_index",
                    (last_used_es_index + 1) % max_indices_allowed,
                )

            default_workspace.settings = default_workspace_settings

            default_workspace.statistics = {
                "document": {
                    "total": 0,
                },
                "field_bundle": {
                    "total": 0,
                },
                "fields": {
                    "total": 0,
                },
            }
            nosql_db.create_workspace(default_workspace)
            default_ws_id = default_workspace.id
            # create preferred workspace
            # TODO: remove preferred workspace and use attribute from user
            prefered_workspace = PreferedWorkspace(
                default_workspace_id=default_ws_id,
                user_id=user.id,
            )
            nosql_db.create_prefered_workspace(prefered_workspace)

            if not app_settings.get("disable_field_bundle", False):
                # create default field bundle for default workspace
                bundle = {
                    "id": str_utils.generate_unique_fieldbundle_id(
                        default_workspace.user_id,
                        default_workspace.id,
                        "default",
                    ),
                    "cachedFile": None,
                    "userId": default_workspace.user_id,
                    "workspaceId": default_workspace.id,
                    "createdOn": str_utils.timestamp_as_str(),
                    "active": True,
                    "fieldIds": [],
                    "tags": None,
                    "bundleName": "Default Field Set",
                    "parentBundleId": None,
                    "bundleType": "DEFAULT",
                }
                nosql_db.create_field_bundle(FieldBundle.from_dict(bundle))

            self.logger.info(
                f"default workspace and preferred workspace created for user {user.email_id}",
            )
        # Create stripe customer
        create_stripe_customer_on_first_login = app_settings.get("users", {}).get(
            "create_stripe_customer_on_first_login",
            False,
        )
        if create_stripe_customer_on_first_login:
            self.create_stripe_customer(user, app_name)

        return user

    def create_stripe_customer(self, user, app_name=""):
        stripe_customer_id = create_stripe_customer(
            user.email_id,
            user_name=user.first_name + " " + user.last_name,
            app_name=app_name,
        )
        if stripe_customer_id:
            nosql_db.update_user(
                {
                    "stripe_conf": {
                        "stripe_customer_id": stripe_customer_id,
                    },
                },
                user_id=user.id,
                email=user.email_id,
            )
            self.logger.info(
                f"Stripe customer ID created for {user.email_id}",
            )
        # subs_name = user.subscription_plan
        # plan = nosql_db.retrieve_subscription_plans(subs_name)
        # stripe_product_id = plan[subs_name].get(
        #     "stripe_product_id",
        #     "",
        # )
        # if stripe_product_id:
        #     stripe_default_product_plan_id = plan[subs_name].get(
        #         "stripe_default_product_plan_id",
        #         "",
        #     )
        #     stripe_customer_id, stripe_subscription_id = \
        #         create_stripe_customer_and_subscription(
        #             user.email_id,
        #             stripe_default_product_plan_id,
        #             user_name=user.first_name + " " + user.last_name,
        #             app_name=app_name,
        #         )
        #     if stripe_customer_id:
        #         nosql_db.update_user(
        #             {
        #                 "stripe_conf": {
        #                     "stripe_customer_id": stripe_customer_id,
        #                     "stripe_product_id": stripe_product_id,
        #                     "stripe_default_product_plan_id": stripe_default_product_plan_id,
        #                     "stripe_default_subscription_id": stripe_subscription_id,
        #                     "stripe_subscription_id": stripe_subscription_id,
        #                 },
        #             },
        #             user_id=user.id,
        #             email=user.email_id,
        #         )
        #         self.logger.info(
        #             f"Stripe customer ID created for {user.email_id}",
        #         )

    def generate_token(
        self,
        token=None,
        user=None,
        m2m_email_id=None,
    ):
        """
        generate token by refresh_token or user object
        """
        if token:
            user = self.auth_user(token=token, return_user=True)

        payload = {
            "email": user.email_id,
            "first_name": user.first_name,
            "last_name": user.last_name,
        }
        if m2m_email_id:
            payload["m2m_email_id"] = m2m_email_id

        return {
            "access_token": self._create_token(
                payload,
                self.ACCESS_TOKEN_EXPIRE_SECONDS,
            ),
            "refresh_token": self._create_token(
                payload,
                self.REFRESH_TOKEN_EXPIRE_SECONDS,
            ),
            "email": user.email_id,
        }

    def _create_token(self, payload, expiry_time):
        """
        Helper to generate token expires in expire_time second
        """
        expire = datetime.utcnow() + timedelta(seconds=expiry_time)
        payload["exp"] = expire
        encoded_jwt = jwt.encode(payload, self.keys, algorithm="HS256")
        return encoded_jwt
