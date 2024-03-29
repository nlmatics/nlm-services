import datetime

from pytz import timezone

from server.storage import nosql_db as sandbox_db

COLLECTION_LIST = [
    {
        "col_name": "usage",
        "indices": [
            [("user_id", 1)],
            [("user_id", 1), ("reported_on", 1)],
        ],
    },
    {
        "col_name": "nlm_subscriptions",
        "indices": [
            [("subs_name", 1)],
        ],
    },
    {
        "col_name": "nlm_settings",
        "indices": [
            [("id", 1)],
        ],
    },
    {
        "col_name": "nlm_catalog",
        "indices": [
            [("id", 1)],
        ],
    },
]

NLM_SETTINGS = [
    {
        "id": "rate_limit_excluded_domains",
        "value": ["nlmatics.com", "nlmatics.io"],
    },
]

CATALOGS = [
    {
        "id": "num_workspaces",
        "feature": "Workspace",
        "renewable": {
            "general_usage": False,
            "dev_api_usage": True,
        },
    },
    {
        "id": "num_docs",
        "feature": "Documents",
        "renewable": {
            "general_usage": False,
            "dev_api_usage": True,
        },
    },
    {
        "id": "num_fields",
        "feature": "Custom Fields",
        "renewable": {
            "general_usage": False,
            "dev_api_usage": False,
        },
    },
    {
        "id": "num_search",
        "feature": "Search",
        "renewable": {
            "general_usage": True,
            "dev_api_usage": True,
        },
    },
    {
        "id": "num_pages",
        "feature": "PDF Pages",
        "renewable": {
            "general_usage": False,
            "dev_api_usage": True,
        },
    },
    {
        "id": "doc_size",
        "feature": "Size",
        "renewable": {
            "general_usage": False,
            "dev_api_usage": True,
        },
    },
    {
        "id": "pdf_parser_pages",
        "feature": "Parser API PDF Pages",
        "renewable": {
            "general_usage": False,
            "dev_api_usage": True,
        },
    },
    {
        "id": "search_api_search",
        "feature": "Search API Search",
        "renewable": {
            "general_usage": False,
            "dev_api_usage": True,
        },
    },
]

SUBSCRIPTIONS = [
    {
        "subs_name": "plan0",
        "quota_limits": {
            "general_usage": {
                "num_workspaces": 1,
                "num_docs": 10,
                "num_pages": 10,
                "doc_size": 10000,
                "num_search": 50,
                "num_fields": 10,
            },
            "dev_api_usage": {
                "num_workspaces": 1,
                "num_docs": 10,
                "num_pages": 10,
                "doc_size": 10000,
                "num_search": 50,
                "num_fields": 10,
            },
        },
    },
    {
        "subs_name": "BASIC",
        "quota_limits": {
            "general_usage": {
                "num_workspaces": 10,
                "num_docs": 1000,  # Average 100 document per workspace
                "num_pages": 20000,  # Average 20 pages per document
                "doc_size": 5000000000,  # 5 MB per document
                "num_search": 20000,  # 20 searches per document
                "num_fields": 50,  # 5 per Workspace
            },
            "dev_api_usage": {
                "num_workspaces": 10,
                "num_docs": 1000,  # Average 100 document per workspace
                "num_pages": 20000,  # Average 20 pages per document
                "doc_size": 5000000000,  # 5 MB per document
                "num_search": 20000,  # 20 searches per document
                "num_fields": 50,  # 5 per Workspace
                "pdf_parser_pages": 20000,  # PDF Parser Pages
                "search_api_search": 20000,  # Search API Search
            },
        },
    },
]

INDICES_LIST = [
    {
        "col_name": "document",
        "indices": [
            [
                ("is_deleted", 1),
                ("parent_folder", 1),
                ("workspace_id", 1),
                ("created_on", 1),
            ],
            [
                ("is_deleted", 1),
                ("parent_folder", 1),
                ("workspace_id", 1),
                ("status", 1),
            ],
            [
                ("is_deleted", 1),
                ("parent_folder", 1),
                ("workspace_id", 1),
                ("name", "text"),
            ],
            [
                ("is_deleted", 1),
                ("parent_folder", 1),
                ("workspace_id", 1),
                ("meta.pubDate", 1),
            ],
        ],
    },
]


def create_collections():
    """
    Creates collections and the associated indices for each of the collection.
    :return: VOID
    """
    col_names = sandbox_db.db.list_collection_names()
    for col in COLLECTION_LIST:
        if col["col_name"] not in col_names:
            for index in col["indices"]:
                sandbox_db.db[col["col_name"]].create_index(index)


def add_nlm_settings():
    for entry in NLM_SETTINGS:
        query = {
            "id": entry["id"],
        }
        sandbox_db.db["nlm_settings"].update_one(
            query,
            {"$set": entry},
            upsert=True,
        )
    print("NLM Settings added.")


def add_catalog():
    sandbox_db.db["nlm_catalog"].insert_many(
        CATALOGS,
        ordered=False,
    )
    print("NLM Catalogs added.")


def add_subscriptions(subscription_plans):
    sandbox_db.db["nlm_subscriptions"].insert_many(
        subscription_plans,
        ordered=False,
    )
    print("NLM Subscriptions added.")


def update_users_with_plan_and_expiry_time():
    tz = timezone("UTC")
    for user in sandbox_db.db["user"].find({}):
        if user["email_id"].split("@")[1] in ["nlmatics.com", "nlmatics.io"]:
            user["expiry_time"] = (
                datetime.datetime.now(tz) + datetime.timedelta(days=3650)
            ).strftime("%Y-%m-%d %H:%M:%S %Z%z")
        else:
            user["expiry_time"] = (
                datetime.datetime.now(tz) + datetime.timedelta(days=30)
            ).strftime("%Y-%m-%d %H:%M:%S %Z%z")
        user["subscription_plan"] = "BASIC"
        sandbox_db.db["user"].replace_one({"id": user["id"]}, user)


def add_default_usage_metrics():
    catalogs = sandbox_db.retrieve_catalogs()
    user_find_res = sandbox_db.db["user"].find({}, {"_id": 0})
    user_list = [u["id"] for u in user_find_res]
    for user in user_list:
        metric_data = {
            "user_id": user,
            "reported_on": datetime.datetime.now().strftime("%Y-%m"),
        }
        for key in ["general_usage", "dev_api_usage"]:
            metric_data[key] = {}
            for cat_key in catalogs:
                metric_data[key][cat_key] = 0
        sandbox_db.db["usage"].insert_one(metric_data)


def add_bundle_type():
    import re

    ws_stream = sandbox_db.db["workspace"].find({"active": True}, {"id": 1})
    for ws in ws_stream:
        f_bundle_stream = sandbox_db.db["field_bundle"].find(
            {"workspace_id": ws["id"]},
            {"bundle_name": 1, "id": 1},
        )
        for bundle in f_bundle_stream:
            if bundle.get("id", None) and bundle.get("bundle_name", None):
                if re.search(r"(?i)Default", bundle["bundle_name"]):
                    sandbox_db.db["field_bundle"].update_one(
                        {"id": bundle.get("id")},
                        {"$set": {"bundle_type": "DEFAULT"}},
                    )
                    print(f"Updated to DEFAULT {bundle.get('id')} for {ws['id']}")
                else:
                    sandbox_db.db["field_bundle"].update_one(
                        {"id": bundle.get("id")},
                        {"$set": {"bundle_type": "PUBLIC"}},
                    )
                    print(f"Updated to PUBLIC {bundle.get('id')} for {ws['id']}")


def update_user_data(email_set_dict_tuple_list):
    for email, set_dict in email_set_dict_tuple_list:
        print("Setting: ", email, "===>", set_dict)
        sandbox_db.db["user"].update_one({"email_id": email}, {"$set": set_dict})


def add_new_indices():
    col_names = sandbox_db.db.list_collection_names()
    for col in INDICES_LIST:
        if col["col_name"] in col_names:
            for index in col["indices"]:
                sandbox_db.db[col["col_name"]].create_index(index)


def add_settings_to_workspaces():
    for ws in sandbox_db.db["workspace"].find(
        {"active": True},
        {"_id": 0, "id": 1, "settings": 1},
    ):
        settings = ws.get("settings", {})
        changed = False
        if not settings.get("domain", None):
            settings["domain"] = "general"
            changed = True
        if not settings.get("search_settings", None):
            settings["search_settings"] = {
                "table_search": False,
            }
            changed = True
        if changed:
            sandbox_db.db["workspace"].update_one(
                {
                    "id": ws["id"],
                },
                {
                    "$set": {
                        "settings": settings,
                    },
                },
            )


def add_statistics_to_workspaces():
    for ws in sandbox_db.db["workspace"].find(
        {"active": True},
        {"_id": 0, "id": 1, "statistics": 1},
    ):
        statistics = ws.get("statistics", {})
        changed = False
        if not statistics:
            doc_query = {
                "is_deleted": False,
                "parent_folder": "root",
                "workspace_id": ws["id"],
            }
            bundle_query = {
                "workspace_id": ws["id"],
                "active": True,
            }
            statistics = {
                "document": {
                    "total": sandbox_db.db["document"].count_documents(doc_query),
                },
                "field_bundle": {
                    "total": sandbox_db.db["field_bundle"].count_documents(
                        bundle_query,
                    ),
                },
                "fields": {
                    "total": sandbox_db.db["field"].count_documents(bundle_query),
                },
            }
            changed = True
        if changed:
            sandbox_db.db["workspace"].update_one(
                {"id": ws["id"]},
                {
                    "$set": {
                        "statistics": statistics,
                    },
                },
            )


def modify_ws_notification_settings():
    for user in sandbox_db.db["user"].find({"active": True}, {"_id": 0}):
        changed = False
        workspace_notification_settings = {}
        user_ws_not_settings = user.get("workspace_notification_settings", None)
        if user_ws_not_settings is not None:
            if isinstance(user_ws_not_settings, list):
                for ws_not in user_ws_not_settings:
                    if ws_not["email_frequency"] not in workspace_notification_settings:
                        workspace_notification_settings[ws_not["email_frequency"]] = []
                    if (
                        ws_not["workspace_id"]
                        not in workspace_notification_settings[
                            ws_not["email_frequency"]
                        ]
                    ):
                        workspace_notification_settings[
                            ws_not["email_frequency"]
                        ].append(ws_not["workspace_id"])
                changed = True
        if changed:
            sandbox_db.db["user"].update_one(
                {"id": user["id"]},
                {
                    "$set": {
                        "workspace_notification_settings": workspace_notification_settings,
                    },
                },
            )


# create_collections()
# add_nlm_settings()
# add_catalog()
# add_subscriptions(SUBSCRIPTIONS)
# update_users_with_plan_and_expiry_time()
# add_default_usage_metrics()
# add_bundle_type()
# add_new_indices()
# add_settings_to_workspaces()
# add_statistics_to_workspaces()
# modify_ws_notification_settings()
