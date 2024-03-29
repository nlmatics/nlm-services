import datetime

from server.storage import nosql_db

catalog_data_dict: dict = {}


def update_metric_data(user_json, metric_data):
    """
    Update the number of fields / searches created by the user
    :param user_json: User Profile
    :param metric_data: Num fields created or searches done (Key, Value) format
    :return: VOID
    """
    if not user_json:
        return
    res_data = {}
    for data in metric_data:
        res_data[data[0]] = data[1]
    if user_json.get("m2m_email", None):
        data = {
            "dev_api_usage": res_data,
        }
    else:
        data = {
            "general_usage": res_data,
        }
    nosql_db.upsert_usage_metrics(user_json["id"], data)


def get_catalogs():
    """
    Retrieves all catalogs
    :return: catalogs dict
    """
    global catalog_data_dict
    if not catalog_data_dict:
        # Retrieve the catalog_data
        catalog_data_dict = nosql_db.retrieve_catalogs()
    return catalog_data_dict


def create_default_metric_for(user):
    metric = {
        "user_id": user,
        "reported_on": datetime.datetime.now().strftime("%Y-%m"),
    }
    for key in ["general_usage", "dev_api_usage"]:
        metric[key] = {}
        for cat_key in get_catalogs():
            metric[key][cat_key] = 0
    return [metric]


def add_default_usage_metrics(user_id):
    """
    Add a default usage metrics whenever the user registers.
    :param user_id: User ID for the newly generated metric
    :return: VOID
    """
    metric_data = {}
    for key in ["general_usage", "dev_api_usage"]:
        metric_data[key] = {}
        for cat_key in get_catalogs():
            metric_data[key][cat_key] = 0
    nosql_db.upsert_usage_metrics(user_id, metric_data, upsert=True)


def update_global_params():
    """
    Update the Global Parameters. Basically set the global variables to None,
    so that it will get picked up in the next invocation of the function
    :return: VOID
    """
    global catalog_data_dict
    catalog_data_dict = None
