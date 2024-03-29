import datetime
import logging
import os
import traceback

from pytz import timezone

import server.config as cfg
from server.storage import nosql_db
from server.utils.metric_utils import create_default_metric_for

logger = logging.getLogger(__name__)
logger.setLevel(cfg.log_level())
# Constants
api_metric_dict = {
    "/api/workspace/undoArchiveWorkspace/": (
        "POST",
        ["num_workspaces"],
        "NA",
    ),  # "POST"
    "/api/workspace/clone/": ("POST", ["num_workspaces"], "NA"),  # "POST"
    "/api/workspace/subscribe/": ("POST", ["num_workspaces"], "NA"),  # "POST"
    "/api/workspace": ("POST", ["num_workspaces"], "PATH_EXACT_MATCH"),  # "POST"
    "/api/document/modify/": (
        "POST",
        ["num_pages", "num_docs", "doc_size"],
        "NA",
    ),  # PUT
    "/api/document/workspace/": (
        "POST",
        ["num_pages", "num_docs", "doc_size"],
        "NA",
    ),  # POST
    "/api/document/uploadByUrl/": (
        "POST",
        ["num_pages", "num_docs", "doc_size"],
        "NA",
    ),  # POST
    "/api/document/developer/upload-and-parse": (
        "POST",
        ["num_pages", "num_docs", "doc_size"],
        "NA",
    ),  # POST
    "/api/replicateFieldBundle/workspace/": ("GET", ["num_fields"], "NA"),  # GET
    "/api/fieldBundle": ("POST", ["num_fields"], "PATH_EXACT_MATCH"),  # POST
    "/api/fieldBundle/": (
        "POST",
        ["num_fields"],
        "NOT_IN_LIST",
        [
            "/api/fieldBundle/field",
            "/api/fieldBundle/modify/",
            "/api/fieldBundle/delete/",
        ],
    ),  # POST
    "/api/field": ("POST", ["num_fields"], "PATH_EXACT_MATCH"),  # POST
    "/api/adhocExtraction/workspace/": ("POST", ["num_search"], "NA"),  # POST
    "/api/adhocExtraction/doc/": ("POST", ["num_search"], "NA"),  # POST
}
DEFAULT_SUBSCRIPTION_PLAN = os.getenv("DEFAULT_SUBSCRIPTION_PLAN", "BASIC")
tz = timezone("UTC")


def get_subscription_plans():
    """
    Returns a dictionary of allowed subscription plans with Key being the plan_name
    and Value being the details of the plan
    :return: Dictionary of subscription plans
    """
    return nosql_db.retrieve_subscription_plans()


def allow_api_request(usage_metric, subscription_limit, user_profile, req):
    """
    Checks whether the API Request is within the limits or not.
    :param usage_metric: Current Usage Metric for the user
    :param subscription_limit: Subscription limit for the plan
    :param user_profile: User Profile
    :param req: Incoming request
    :return:
    """
    err_resp = None
    email_id = user_profile["email_id"]
    expiry_time = user_profile.get("expiry_time", None)
    if not expiry_time:
        logger.info(
            f"Rejecting the API Request. " f"{email_id} do not have any expiry_time",
        )
        return False, "User does not have any expiry_time"

    if expiry_time < datetime.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S %Z%z"):
        logger.info(
            f"Rejecting the API Request. "
            f"Access for {email_id} expired. Please re-subscribe",
        )
        return False, "User Access expired. Re-subscribe to continue using the API."
    metric_keys = api_to_metric_convertor(req)
    over_limit = False
    over_usage_key = None
    total_usage = 0
    subs_limit = 0
    if subscription_limit:
        for m_key in metric_keys:
            total_usage = usage_metric.get("dev_api_usage", {m_key: 0}).get(
                m_key,
                0,
            ) + usage_metric.get("general_usage", {m_key: 0}).get(m_key, 0)
            subs_limit = (
                subscription_limit["quota_limits"]
                .get("dev_api_usage", {m_key: 0})
                .get(m_key, 0)
            ) + subscription_limit["quota_limits"].get("general_usage", {m_key: 0}).get(
                m_key,
                0,
            )
            # Check whether we have a subscription limit for this key. Else skip it.
            if not subs_limit:
                if (
                    subscription_limit["quota_limits"]
                    .get("dev_api_usage", {})
                    .get(m_key, None)
                    is None
                    and subscription_limit["quota_limits"]
                    .get("general_usage", {})
                    .get(m_key, None)
                    is None
                ):
                    continue

            if total_usage + 1 > subs_limit:
                over_limit = True
                over_usage_key = m_key
                break

    if over_limit:
        logger.info(
            f"Rejecting the API Request. "
            f"Access for '{email_id}' over_limit for API '{req.path}' in metric '{over_usage_key}'. "
            f"Usage({total_usage}) / Quota({subs_limit})",
        )
        err_resp = f"Subscription limit reached in metric {over_usage_key}."
    return not over_limit, err_resp


def perform_rate_limit(
    do_rate_limit,
    subscription_plans,
    user_profile,
    excluded_domains,
    req,
):
    """
    Check whether we can allow the incoming requests from the user, based on the plan he has subscribed to.
    :param do_rate_limit: Do we need to do a rateLimit
    :param subscription_plans: Subscription plans allowed in dictionary format.
    :param user_profile: User profile for which we need to perform the rate limit.
    :param excluded_domains: List of domains excluded from the rate check
    :param req: Incoming request
    :return: Tuple detailing whether we allow the request to process or not and the subscription plans
    """
    try:
        # Read the subscription plans
        if (not subscription_plans) and do_rate_limit:
            logger.info("Reading subscription plans")
            subscription_plans = get_subscription_plans()
        # Basic Rate Limit logic.
        if not excluded_domains:
            logger.info("Reading Excluded domains")
            excluded_domains = nosql_db.get_nlm_settings("rate_limit_excluded_domains")
            excluded_domains = excluded_domains or []
        # Check if rate_limit check flag is disabled or if the domain is excluded from rate_limit.
        if (not do_rate_limit) or user_profile["email_id"].split("@")[
            1
        ].lower() in excluded_domains:
            return True, None, subscription_plans, excluded_domains
        # All users should have a subscription_plan associated with them.
        # TODO. Remove the DEFAULT_SUBSCRIPTION_PLAN
        subs_name = user_profile.get("subscription_plan", DEFAULT_SUBSCRIPTION_PLAN)
        if not subs_name:
            logger.info(
                f"Rejecting the API Request. {user_profile['email_id']} do not have any subscription_plans attached",
            )
            return (
                False,
                "No Plans attached to the user",
                subscription_plans,
                excluded_domains,
            )
        # Try to retrieve the subscription plan.
        if subs_name not in subscription_plans.keys():
            plan_data = nosql_db.retrieve_subscription_plans(subs_name=subs_name)
            if plan_data:
                subscription_plans[subs_name] = plan_data[subs_name]
        # If the subscription plan has mentioned not to do perform_rate_limit, respect it.
        if subs_name in subscription_plans and not subscription_plans[subs_name].get("perform_rate_limit", False):
            return True, None, subscription_plans, excluded_domains
        # Retrieve the usage metrics for the current month
        usage_metric_list = nosql_db.retrieve_usage_metrics(user_profile["id"])
        if not usage_metric_list:
            latest_usage_metric = nosql_db.retrieve_latest_usage(user_profile["id"])
            if latest_usage_metric:
                usage_metric_list = [latest_usage_metric]
            else:
                usage_metric_list = create_default_metric_for(user_profile["id"])
        allow_access, err_resp = allow_api_request(
            usage_metric_list[0],
            subscription_plans.get(subs_name, None),
            user_profile,
            req,
        )
        return allow_access, err_resp, subscription_plans, excluded_domains
    except Exception as e:
        logger.error(
            f"Error while performing rate limit, err: {traceback.format_exc()} .. {str(e)}",
        )
        return False, "Internal Server Error", subscription_plans, excluded_domains


def api_to_metric_convertor(req):
    """
    Returns the metric section and key corresponding to the request.
    :param req: Incoming request
    :return: Returns the catalog key corresponding to the request.
    """
    matching = [item for item in api_metric_dict.keys() if req.path.startswith(item)]
    if len(matching):
        for k in matching:
            if (
                k in req.path
                and api_metric_dict[k][0] == req.method
                and (
                    api_metric_dict[k][2] == "NA"
                    or (api_metric_dict[k][2] == "PATH_EXACT_MATCH" and k == req.path)
                    or (
                        api_metric_dict[k][2] == "NOT_IN_LIST"
                        and not req.path.startswith(tuple(api_metric_dict[k][3]))
                    )
                )
            ):
                return api_metric_dict[k][1]
    return []


def update_last_login(user_id="", email=""):
    """
    Updates the last login time for user.
    :param user_id: User ID
    :param email: Email id
    :return:
    """
    nosql_db.update_user(
        {
            "last_login": datetime.datetime.now(tz).strftime(
                "%Y-%m-%d %H:%M:%S %Z%z",
            ),
            "is_logged_in": True,
        },
        user_id=user_id,
        email=email,
    )


def update_logged_in(user_id="", email="", is_logged_in=False):
    """
    Updates is_logged_in status.
    :param user_id: User ID
    :param email: Email id
    :param is_logged_in: Whether the user is logged in or not
    :return:
    """
    nosql_db.update_user(
        {
            "is_logged_in": is_logged_in,
        },
        user_id=user_id,
        email=email,
    )
