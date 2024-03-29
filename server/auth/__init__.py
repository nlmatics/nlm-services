import logging
import os

import connexion
import jose
import werkzeug
from nlm_utils.utils import ensure_bool

import server.config as cfg
from .auth_provider_factory import AuthProvider
from server.storage import nosql_db
from server.utils.auth_utils import perform_rate_limit

logger = logging.getLogger(__name__)
logger.setLevel(cfg.log_level())


auth_provider = AuthProvider.instance(os.getenv("AUTH_PROVIDER", "azure_ad").lower())
DEV_ACCESS_APIS = [
    "/api/developerApiKey/access_token",
    "/api/document/developer/upload-and-parse",
    "/api/document/download/",
]

VIEWER_UNRESTRICTED_POST_APIS = [
    "/api/fieldValue",  # Field value creation / modify
    "/api/fieldValue/field/delete/",  # Field value delete
    "/api/fieldBundle/modify/",  # field Bundle re-ordering etc
    "/api/adhocExtraction",  # Search
    "/api/searchResult",  # BookMark, Training Sample creation, Associate additional question.
    "/api/extractFieldBundle/gridData",  # Retrieval of Grid Data
]

VIEWER_RESTRICTED_GET_APIS = [
    "/api/document/reIngestWorkspace/",  # Re-ingest workspace
    "/api/document/reIngestDocument/",  # Re-ingest a document
]

subscription_plans: dict = {}
excluded_domains: list = []
app_viewer_unrestricted_post_apis: list = []
viewer_unrestricted_post_apis_tuple: tuple = ()
viewer_restricted_get_apis_tuple: tuple = ()
do_rate_limit: bool = ensure_bool(os.getenv("RATE_LIMIT", True))
limit_developer_access: bool = ensure_bool(os.getenv("LIMIT_DEVELOPER_ACCESS", False))
app_name: str = os.getenv("APP_NAME", "")


def authenticate(token):
    global subscription_plans
    global excluded_domains
    global app_viewer_unrestricted_post_apis
    global viewer_unrestricted_post_apis_tuple
    global viewer_restricted_get_apis_tuple
    user = None
    access_log = {
        "user": user,
        "token": token,
        "status": None,
    }
    send_403 = False
    if app_name and not app_viewer_unrestricted_post_apis:
        app_settings = nosql_db.get_application_settings(app_name) or {}
        app_viewer_unrestricted_post_apis = app_settings.get(
            "viewer_unrestricted_post_apis",
            [],
        )

        if app_viewer_unrestricted_post_apis:
            VIEWER_UNRESTRICTED_POST_APIS.extend(app_viewer_unrestricted_post_apis)
        else:
            # Do this so that we don't read every time
            app_viewer_unrestricted_post_apis = VIEWER_UNRESTRICTED_POST_APIS

    if not viewer_unrestricted_post_apis_tuple:
        viewer_unrestricted_post_apis_tuple = tuple(VIEWER_UNRESTRICTED_POST_APIS)

    if not viewer_restricted_get_apis_tuple:
        viewer_restricted_get_apis_tuple = tuple(VIEWER_RESTRICTED_GET_APIS)

    try:
        # logger.info(f"Authenticating request using token {token}")
        if token:
            user = auth_provider.auth_user(token, return_user=True, return_json=True)
            if user:
                if not user["is_logged_in"]:
                    raise ValueError("User is not logged in yet.")
                # Restrict the POST API call from users with "viewer" access_type
                # Allow all GET method requests to pass through
                if user.get("access_type", "").lower() == "viewer" and (
                    (
                        connexion.request.method == "POST"
                        and (
                            not connexion.request.path.startswith(
                                viewer_unrestricted_post_apis_tuple,
                            )
                        )
                    )
                    or (
                        connexion.request.method == "GET"
                        and connexion.request.path.startswith(
                            viewer_restricted_get_apis_tuple,
                        )
                    )
                ):
                    log_str = (
                        f"{user.get('access_type')} Access (from {user['email_id']})"
                        f' to restricted {connexion.request.method} invocation of "{connexion.request.path}"'
                    )
                    logger.info(log_str)
                    err_str = f"{user.get('access_type')} Access to restricted API"
                    send_403 = True
                    raise werkzeug.exceptions.Forbidden(err_str)

                access_log.update({"status": "success", "user": user["email_id"]})
                logger.info(
                    f"token authenticated, user {user['email_id']} for API({connexion.request.method}) "
                    f"access to {connexion.request.path}",
                )
                nosql_db.create_access_log(access_log)
                is_access_allowed = True
                if (
                    user.get(
                        "m2m_email",
                        None,
                    )
                    and limit_developer_access
                    and (not connexion.request.path.startswith(tuple(DEV_ACCESS_APIS)))
                ):
                    err_str = f"Invalid Developer Access to {connexion.request.path}"
                    update_access_log("failed", err_str, access_log)
                    is_access_allowed = False

                if is_access_allowed:
                    (
                        is_access_allowed,
                        err_resp,
                        subscription_plans,
                        excluded_domains,
                    ) = perform_rate_limit(
                        do_rate_limit,
                        subscription_plans,
                        user,
                        excluded_domains,
                        connexion.request,
                    )
                    if not is_access_allowed:
                        log_err = f"Rate limit in Action for {connexion.request.path} by {user['email_id']}"
                        update_access_log("failed", err_resp, access_log, log_err)
                        send_403 = True
                if is_access_allowed:
                    # The entire return object will be in token_info argument of the controller.
                    return {
                        "scope": ["user"],
                        "sub": user["email_id"],
                        "email_id": user["email_id"],
                        "user_obj": user,
                    }
    except jose.ExpiredSignatureError as e:
        access_log.update({"status": "expired", "error": e})
    except jose.JOSEError as e:  # the catch-all of Jose
        access_log.update({"status": "invalid", "error": e})
    except jose.exceptions.JWTError as e:
        access_log.update({"status": "invalid", "error": e})
    except Exception as e:
        access_log.update({"status": "failed", "error": e})
    error = access_log["error"]
    access_log["error"] = str(error)

    if os.getenv("ENABLE_ACCESS_LOG", False):
        nosql_db.create_access_log(access_log)
    if send_403:
        logger.info(access_log["error"])
        raise werkzeug.exceptions.Forbidden(access_log["error"])
    else:
        return None


def update_access_log(status, err_str, access_log, log_err=None):
    """
    Updates the access log and log the error
    :param status: Status of the operation
    :param err_str: Error string
    :param access_log: Access log
    :param log_err: Error log that needs to be logged.
    :return: VOID
    """
    access_log.update(
        {
            "status": status,
            "error": err_str,
        },
    )
    logger.error(log_err if log_err else err_str)


def update_global_params():
    """
    Update the Global Parameters. Basically set the global variables to None,
    so that it will get picked up in the next invocation of the function
    :return: VOID
    """
    global subscription_plans
    global excluded_domains
    global app_viewer_unrestricted_post_apis
    global viewer_unrestricted_post_apis_tuple
    global viewer_restricted_get_apis_tuple
    subscription_plans = {}
    excluded_domains = []
    app_viewer_unrestricted_post_apis = []
    viewer_unrestricted_post_apis_tuple = ()
    viewer_restricted_get_apis_tuple = ()
