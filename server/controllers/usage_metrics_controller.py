import logging
import os

from flask import jsonify
from flask import make_response

from server import unauthorized_response
from server.models.usage_metric import UsageMetric  # noqa: E501
from server.storage import nosql_db as nosqldb
from server.utils.metric_utils import get_catalogs

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
DEFAULT_SUBSCRIPTION_PLAN = os.getenv("DEFAULT_SUBSCRIPTION_PLAN", "BASIC")


def get_usage_metrics(user, token_info, year=None, month=None):  # noqa: E501
    """Returns usage metrics for the user with the options

     # noqa: E501

    :param user: User email id.
    :param token_info: Return data from Authentication
    :param year: Year for which metrics has to be retrieved
    :type year: str
    :param month: Month for which metrics has to be retrieved
    :type month: str

    :rtype: List[UsageMetric]
    """
    try:
        user_obj = token_info.get("user_obj", None) if token_info else None
        if not user_obj:
            return unauthorized_response()
        # Retrieve the Metrics list
        metric_list = nosqldb.retrieve_usage_metrics(user_obj["id"], year, month)
        # Subscription plan for the user.
        subs_name = user_obj.get("subscription_plan", DEFAULT_SUBSCRIPTION_PLAN)
        plan = nosqldb.retrieve_subscription_plans(subs_name)
        if plan:
            metric_data = []
            for metric in metric_list:
                calculate_usage(metric, plan[subs_name])
                metric["subscription_detail"] = plan[subs_name].get(
                    "subs_details",
                    subs_name,
                )
                metric_data.append(UsageMetric(**metric))
            return make_response(
                jsonify(metric_data),
                200,
            )
        return make_response(
            jsonify(
                {
                    "status": "fail",
                    "reason": f"Error retrieving Usage metrics for {user}. No plan associated.",
                },
            ),
            500,
        )
    except Exception as e:
        logger.error(
            f"Error retrieving Usage metrics {user}, err: {str(e)}",
            exc_info=True,
        )
        return make_response(jsonify({"status": "fail", "reason": str(e)}), 500)


def calculate_usage(metric, plan_quota):
    """
    Calculate the metric usage from usage metrics and plan and updates the metrics
    {
            catalog_id: 'num_pages',
            feature: 'PDF Pages',
            used: 1000,
            quota: 10000,
            percentUsed: '10',
        },
    :param metric: Usage metrics retrieved from Database
    :param plan_quota: Subscription plan quota for the user
    :return: Updates the metrics inplace.
    """
    catalog_data_dict = get_catalogs()
    discard_metric_keys = list(
        {"general_usage", "dev_api_usage"} - set(plan_quota["quota_limits"].keys()),
    )

    for key in discard_metric_keys:
        del metric[key]

    for key in plan_quota["quota_limits"]:
        metric_data = metric.get(key, None)
        metric[key] = []
        for catalog_key in plan_quota["quota_limits"][key]:
            used_val = metric_data.get(catalog_key, 0) if metric_data else 0
            quota_val = plan_quota["quota_limits"][key][catalog_key]
            cat_dict_data = {
                "catalog_id": catalog_key,
                "feature": catalog_data_dict[catalog_key]["feature"],
                "used": str(used_val),
                "quota": str(quota_val),
                "percentUsed": str(
                    round(((used_val / (quota_val if quota_val else 1)) * 100), 1),
                ),
            }
            if catalog_key == "doc_size":
                cat_dict_data["used"] = convert_doc_size_to_string(used_val)
                cat_dict_data["quota"] = convert_doc_size_to_string(quota_val)
            metric[key].append(cat_dict_data)


def convert_doc_size_to_string(size):
    """
    Convert document size in numbers to string format with Size unit attached.
    :param size: Size in number format
    :return: String format with size unit attached.
    """
    if (size / (10 ** 9)) > 1:
        return str(round(size / (10 ** 9), 2)) + " GB"
    elif (size / (10 ** 6)) > 1:
        return str(round(size / (10 ** 6), 2)) + " MB"
    elif (size / (10 ** 3)) > 1:
        return str(round(size / (10 ** 3), 2)) + " KB"
    else:
        return str(size) + " Bytes"
