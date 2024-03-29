import base64
import collections
import datetime
import logging
import os
import tempfile
import traceback
from typing import List
from typing import Optional

from bson.objectid import ObjectId
from flask import jsonify
from flask import make_response
from flask import request
from natsort import natsorted
from nlm_utils.storage import file_storage
from nlm_utils.utils.utils import ensure_bool
from pymongo import MongoClient
from pymongo import ReturnDocument
from pymongo import UpdateOne
from pymongo.errors import CollectionInvalid
from pytz import timezone

from server.models import BBox
from server.models import Document
from server.models import DocumentFolder
from server.models import DocumentKeyInfo
from server.models import Field
from server.models import FieldBundle
from server.models import FieldValue
from server.models import IgnoreBlock
from server.models import Notifications
from server.models import Prompt
from server.models import SavedSearchResult
from server.models import SearchCriteriaWorkflow
from server.models import SearchHistory
from server.models import User
from server.models import UserAccessControl
from server.models import UserFeedback
from server.models import WaitList
from server.models import Workspace
from server.models import WorkspaceFilter
from server.models.history import History
from server.models.train_sample import TrainSample
from server.storage.nosql_db import NoSqlDb
from server.utils import bbox_utils
from server.utils import str_utils
from server.utils.dependent_fields_utils import BOOLEAN_MULTI_CAST_FIELD_TYPE
from server.utils.dependent_fields_utils import BOOLEAN_MULTI_CAST_PERMISSIBLE_VALUES
from server.utils.dependent_fields_utils import CAST_FIELD_TYPE
from server.utils.dependent_fields_utils import DEFAULT_CAST_OPTION_KEY
from server.utils.dependent_fields_utils import DEPENDENT_FIELD_ALLOWED_TYPES
from server.utils.dependent_fields_utils import FORMULA_FIELD_TYPE
from server.utils.dependent_fields_utils import NONE_CAST_OPTION_KEY
from server.utils.formula import evaluate_formula
from server.utils.notification_general import WS_SPECIFIC_NOTIFY_ACTIONS

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
COLLECTION_LIST = []
DATE_TIME_YEAR_MONTH = "%Y-%m"
DATE_TIME_YEAR_MONTH_DATE = "%Y-%m-%d"
t_zone = timezone("UTC")
T_ZONE_FORMAT = "%Y-%m-%d %H:%M:%S %Z%z"
MAX_SELF_SORT_LIMIT = 1001
PAYMENT_CONTROLLED_RENEWABLE_RESOURCES = ensure_bool(
    os.getenv("PAYMENT_CONTROLLED_RENEWABLE_RESOURCES", False),
)


class MongoDB(NoSqlDb):
    def __init__(self, host=None, db=None, log_level=logging.INFO):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)
        host = host or os.getenv("MONGO_HOST", "localhost")
        self.db_client = MongoClient(host)
        self.db = self.db_client[db or os.getenv("MONGO_DATABASE", "doc-store-dev")]
        self.index_db = self.db_client[os.getenv("MONGO_INDEX_DATABASE", "nlm-index")]
        if (
            ensure_bool(os.getenv("MONGO_CHECK_COLLECTIONS", True))
            and host != "localhost"
        ):
            self._create_collections(COLLECTION_LIST)

    def _create_collections(self, collection_list):
        """
        Creates collections and the associated indices for each of the collection.
        :param collection_list: List of collections and the indices to create.
        :return: VOID
        """
        col_names = self.db.list_collection_names()
        for col in collection_list:
            if col["col_name"] not in col_names:
                for index in col["indices"]:
                    self.db[col["col_name"]].create_index(index)

    def _create_entity(
        self,
        entity_object,
        entity_type,
        append=False,
        entity_object_dict=None,
    ):
        if not append:
            doc_ref = self.db[entity_type].find_one({"id": entity_object.id})
            if doc_ref:
                self.logger.error(
                    f"{entity_type} with id {entity_object.id} already exists",
                )
                raise Exception(
                    f"{entity_type} with id {entity_object.id} already exists",
                )
        self.db[str(entity_type)].insert_one(
            entity_object_dict if entity_object_dict else entity_object.to_dict(),
        )
        self.logger.info(f"{entity_type} created with id {entity_object.id}")
        return entity_object.id

    def _delete_entity(self, entity_id, entity_type):
        doc_ref = self.db[entity_type].find_one({"id": entity_id})
        if not doc_ref:
            self.logger.error(f"{entity_type} with id {entity_id} does not exists")
            raise Exception(f"{entity_type} with id {entity_id} does not exists")
        self.db[entity_type].delete_one(doc_ref)
        self.logger.info(f"{entity_type} with id {entity_id} deleted")
        return entity_id

    def _update_entity(self, entity_id, entity_object, entity_type):
        """Helper method to update entries
        :param entity_id:
        :param entity_object:
        :param entity_type:
        :return:
        """
        doc_ref = self.db[entity_type].find_one({"id": entity_id})
        if not doc_ref and not doc_ref.get().exists:
            self.logger.error(f"{entity_type} with id {entity_id} does not exists")
            raise Exception(f"{entity_type} with id {entity_id} does not exists")
        self.db[entity_type].replace_one({"id": entity_id}, entity_object.to_dict())
        self.logger.info(f"{entity_type} with id {entity_id} updated")
        return entity_id

    def get_user_by_email(
        self,
        email_id,
        expand=False,
        projection=None,
        include_stripe_conf=False,
    ):
        """Returns the user corresponding to the email id
        :param email_id:
        :param expand: Include all the details from user_profile
        :param projection: What parameters to return or not return.
        :param include_stripe_conf: Include stripe configuration or not.
        :return: user object
        """
        if not projection:
            projection = {
                "_id": 0,
            }

        if not include_stripe_conf:
            projection["stripe_conf.stripe_customer_id"] = 0
            projection["stripe_conf.subscriptions.stripe_product_plan_id"] = 0
            projection["stripe_conf.subscriptions.stripe_subscription_id"] = 0

        if not expand:
            projection["workspace_notification_settings"] = 0

        user_stream = self.db["user"].find_one(
            {"email_id": email_id},
            projection,
        )
        if user_stream:
            users = [User(**user_stream)]
        else:
            raise ValueError(str(email_id) + " not in db")

        if len(users) == 1:
            user = users[0]
            # self.logger.info(f"returning user info with id {email_id}: {user}")
            return user
        elif len(users) < 1:
            self.logger.info(f"user with id {email_id} not found")
            return None
        else:
            self.logger.error(f"{len(users)} multiple users found with id {email_id}")
            raise Exception(f"multiple users found for user with id {email_id}")

    def get_distinct_active_user_profiles(
        self,
        domain_name="",
        return_user_obj=False,
        include_only_emails=False,
    ):
        """
        Retrieve active user profiles which has email id matching the domain listed
        :param domain_name: Domain name
        :param return_user_obj: Return a list of user objects
        :param include_only_emails: Include only emails in the return data.
        :return:
        """
        users = []
        if not domain_name:
            self.logger.error("Require a valid domain name: ")
            return users
        domain_name = f".*{domain_name}$"
        distinct_active_emails = self.db["user"].distinct(
            "email_id",
            {
                "email_id": {"$regex": domain_name},
                "active": True,
            },
        )
        return_fields = {
            "_id": 0,
        }
        if include_only_emails:
            return_fields["email_id"] = 1
        cursor = self.db["user"].find(
            {"email_id": {"$in": distinct_active_emails}},
            return_fields,
        )
        users = [user if not return_user_obj else User(**user) for user in cursor]
        return users

    def create_user(self, user):
        return self._create_entity(user, "user")

    def delete_user(self, user_id):
        """Deletes an existing user with given user_id
        :param user_id: id of user to be deleted
        :return: id of the deleted user, if successful
        """
        return self._delete_entity(user_id, "user")

    def get_users(
        self,
        user_list=None,
        return_user_obj=False,
        expand=False,
    ):
        """Returns the list of all users
        :return: list of user objects
        """
        query = {}
        projection = {
            "_id": 0,
        }

        if user_list:
            query = {
                "id": {"$in": user_list},
            }

        if not expand:
            projection["workspace_notification_settings"] = 0

        user_stream = self.db["user"].find(query, projection)
        user_list = [u if not return_user_obj else User(**u) for u in user_stream]
        self.logger.info(f"{len(user_list)} users returned")
        return user_list

    def update_user(self, profile_json, user_id="", email=""):
        """Update info of an existing user
        :param profile_json: user json structure with parameters that needs to be updated.
        :param user_id: id of the user to update
        :param email:
        :return: return updated Document
        """
        query = {}
        if email:
            query["email_id"] = email
        else:
            query["id"] = user_id
        return self.db["user"].find_one_and_update(
            query,
            {"$set": profile_json},
            {"_id": 0},
            return_document=ReturnDocument.AFTER,
        )

    def create_workspace(self, workspace):
        """Creates a new workspace
        :param workspace:
        :return:
        """
        return self._create_entity(workspace, "workspace")

    def delete_workspace(self, workspace_id, permanent=False):
        """Deletes an existing workspace
        :param workspace_id: id of workspace to delete
        :param permanent: Delete permanently
        :return: id of the deleted workspace
        """
        if not permanent:
            self.db["workspace"].update_one(
                {"id": workspace_id, "active": True},
                {"$set": {"active": False}},
            )
        else:
            self.db["workspace"].delete_one({"id": workspace_id})
        return workspace_id

    def undo_delete_workspace(self, workspace_id):
        self.db["workspace"].update_one(
            {"id": workspace_id},
            {"$set": {"active": True}},
        )
        return workspace_id

    def get_archived_workspaces(
        self,
        user_id,
        remove_private_data=True,
    ):
        filter_param = {
            "_id": 0,
        }
        if remove_private_data:
            filter_param["subscribed_users"] = 0
            filter_param["stripe_conf"] = 0

        ws_stream = self.db["workspace"].find(
            {"user_id": user_id, "active": False},
            filter_param,
        )
        ws_list = [Workspace(**ws) for ws in ws_stream]
        self.logger.info(f"{len(ws_list)} workspaces returned")
        return ws_list

    def update_workspace(self, workspace_id, workspace):
        """Updates an existing workspace
        :param workspace_id:
        :param workspace:
        :return:
        """
        return self._update_entity(workspace_id, workspace, "workspace")

    def workspace_exists(self, workspace_id):
        # check if workspace exists
        if self.db["workspace"].find_one({"id": workspace_id, "active": True}):
            return True
        return False

    def workspace_by_name_exists(self, workspace_name):
        return (
            self.db["workspace"].find_one({"name": workspace_name, "active": True})
            is not None
        )

    def get_all_workspaces(
        self,
        remove_private_data=True,
        user_id=None,
    ):
        filter_param = {
            "_id": 0,
        }
        if remove_private_data:
            filter_param["subscribed_users"] = 0
            filter_param["stripe_conf"] = 0

        query = {
            "active": True,
        }
        if user_id:
            query["user_id"] = user_id
        ws_stream = self.db["workspace"].find(
            query,
            filter_param,
        )
        ws_list = [Workspace(**ws) for ws in ws_stream]
        self.logger.info(f"{len(ws_list)} workspaces returned")
        return ws_list

    def get_workspace_by_id(self, workspace_id, remove_private_data=False):
        filter_param = {
            "_id": 0,
        }

        if remove_private_data:
            filter_param["subscribed_users"] = 0
            filter_param["stripe_conf"] = 0

        workspace_ref = self.db["workspace"].find_one(
            {"id": workspace_id, "active": True},
            filter_param,
        )
        workspaces = []
        if workspace_ref:
            workspaces.append(Workspace(**workspace_ref))

        if len(workspaces) == 1:
            self.logger.info(f"workspace with id {workspace_id} found")
            return workspaces[0]

        elif len(workspaces) > 1:
            raise Exception(f"multiple workspace found with id {workspace_id}")

        else:
            self.logger.warning(f"no workspaces with id {workspace_id} found")
            return None

    def get_workspace_for_document_id(self, doc_id: str):
        workspace = None
        doc_ref = self.db["document"].find_one(
            {"id": doc_id},
            {"_id": 0, "workspace_id": 1},  # exclude the return of _id
        )
        if doc_ref:
            workspace = self.get_workspace_by_id(doc_ref["workspace_id"])
        return workspace

    def get_workspaces_for_user(
        self,
        user,
        user_id,
        user_profile=None,
        remove_private_data=True,
        get_only_private_ws=False,
    ):
        idxs = set()
        workspaces = []
        private_workspaces = []
        public_workspaces = []
        collaborated_workspaces = []
        subscribed_workspaces = []
        restricted_workspaces = []

        filter_param = {
            "_id": 0,
        }

        if remove_private_data:
            filter_param["subscribed_users"] = 0
            filter_param["stripe_conf"] = 0
            filter_param["settings.index_settings"] = 0
            filter_param["settings.search_settings"] = 0
            filter_param["settings.document_settings"] = 0

        # get private workspace
        private_ws = self.db["workspace"].find(
            {"user_id": user_id, "active": True},
            filter_param,
        )
        for w in private_ws:
            private_workspaces.append(Workspace(**w))
            idxs.add(w["id"])
        workspaces.extend(private_workspaces)

        if get_only_private_ws:
            workspaces.sort(key=lambda ws: ws.name if ws.name else "")
            return workspaces

        # get collaborated_workspaces
        user_domain = user.split("@")[1]
        collaborators = self.db["workspace"].aggregate(
            [
                {
                    "$addFields": {
                        "result": {
                            "$filter": {
                                "input": {"$objectToArray": "$collaborators"},
                                "cond": {
                                    "$or": [
                                        {"$eq": ["$$this.k", user]},
                                        {"$eq": ["$$this.k", user_domain]},
                                        # {"$eq": ["$$this.k", "*"]},
                                    ],
                                },
                            },
                        },
                    },
                },
                {"$match": {"active": True}},
                {"$match": {"$expr": {"$ne": ["$result", None]}}},
                {"$match": {"$expr": {"$ne": [{"$size": "$result"}, 0]}}},
                {"$project": {"result": 0}},
            ],
        )
        for w in collaborators:
            del w["_id"]
            if remove_private_data:
                w.pop("subscribed_users", None)
                w.pop("stripe_conf", None)
                w.get("settings", {}).pop("index_settings", None)
                w.get("settings", {}).pop("search_settings", None)
                w.get("settings", {}).pop("document_settings", None)
            # remove duplicate workspace
            if w["id"] in idxs:
                continue

            idxs.add(w["id"])
            collaborated_workspaces.append(Workspace(**w))
        workspaces.extend(collaborated_workspaces)

        # public_workspaces
        public_ws = self.db["workspace"].find(
            {"collaborators.*": {"$exists": True}, "active": True},
            filter_param,
        )
        for w in public_ws:
            # remove duplicate workspace
            if w["id"] in idxs:
                continue

            idxs.add(w["id"])
            public_workspaces.append(Workspace(**w))

        workspaces.extend(public_workspaces)

        # Retrieve subscribed_workspaces.
        if user_profile:
            ws_stream = self.db["workspace"].find(
                {
                    "id": {"$in": user_profile.get("subscribed_workspaces", [])},
                    "active": True,
                },
                filter_param,
            )
            for ws in ws_stream:
                subscribed_workspaces.append(Workspace(**ws))
            workspaces.extend(subscribed_workspaces)

            rest_ws_stream = self.db["workspace"].find(
                {
                    "id": {"$in": user_profile.get("restricted_workspaces", [])},
                    "active": True,
                },
                filter_param,
            )
            for ws in rest_ws_stream:
                restricted_workspaces.append(Workspace(**ws))

        ret_dict = {
            "private_workspaces": private_workspaces,
            "collaborated_workspaces": collaborated_workspaces,
            "subscribed_workspaces": subscribed_workspaces,
            "restricted_workspaces": restricted_workspaces,
            "public_workspaces": public_workspaces,
        }
        workspaces.sort(key=lambda _ws: _ws.name if _ws.name else "")
        return workspaces, ret_dict

    def update_workspace_data(self, workspace_id: str, set_data):
        update_ref = self.db["workspace"].update_one(
            {"id": workspace_id},
            {"$set": set_data},
        )
        if update_ref:
            self.logger.info(
                f"workspace {workspace_id} updated with set_data {set_data}",
            )
        else:
            self.logger.error(
                f"workspace {workspace_id} not found, cannot update set_data {set_data}",
            )

    def get_user_obj(self, user_id="", email=""):
        if not email:
            return self.get_user(user_id)
        elif not user_id:
            return self.get_user_by_email(email)
        else:
            return self.get_user_by_email(email)

    def get_user_permission(self, workspace_id, user_id="", email="", user_json=None):
        if not user_json:
            user_obj = self.get_user_obj(user_id, email)
            if not user_obj:
                return "", None
            else:
                email = user_obj.email_id
                user_id = user_obj.id
        else:
            user_obj = User(**user_json)
            email = user_obj.email_id
            user_id = user_obj.id

        workspace = self.get_workspace_by_id(workspace_id)

        restricted_workspaces = user_obj.restricted_workspaces or []
        if workspace_id in restricted_workspaces:
            self.logger.info(f"User {email} has access restricted to {workspace_id}")
            return "", workspace

        user_domain = email.split("@")[1]
        if user_obj.is_admin:
            return "admin", workspace
        elif user_id == workspace.user_id:
            return "owner", workspace
        elif (
            email in workspace.shared_with
            or user_domain in workspace.shared_with
            or "*" in workspace.shared_with
        ):
            # old implementation support
            return "editor", workspace

        elif email in workspace.collaborators:
            # new implementation
            return workspace.collaborators[email], workspace
        elif user_domain in workspace.collaborators:
            # new implementation
            return (
                workspace.collaborators[user_domain],
                workspace,
            )
        elif "*" in workspace.collaborators:
            # new implementation
            return workspace.collaborators["*"], workspace

        else:
            # user does not have permission in this workspace
            return "", workspace

    def get_default_workspace_for_user_id(self, user_id, remove_private_data=True):
        user_details = self.get_user(user_id)
        user_ws, _ = self.get_workspaces_for_user(
            user_details.email_id,
            user_id,
            get_only_private_ws=True,
            remove_private_data=remove_private_data,
        )
        if len(user_ws) == 1:
            return user_ws[0]
        else:
            # find workspace with name 'default'
            default_ws = [ws for ws in user_ws if ws.name.lower() == "default"]
            if len(default_ws) == 1:
                return default_ws[0]
            else:
                raise ValueError(
                    "none/multiple workspaces found. ambiguous to find a default one to use",
                )

    # Prefered Workspace API
    def create_prefered_workspace(self, prefered_workspace):
        entity_type = "prefered_workspace"
        entity_object = prefered_workspace
        set_data = {
            "user_id": entity_object.user_id,
            "default_workspace_id": entity_object.default_workspace_id,
        }
        self.db[entity_type].update_one(
            {"user_id": entity_object.user_id},
            {"$set": set_data},
            upsert=True,
        )
        logger.info(f"{entity_type} created for {entity_object.user_id}")
        return entity_object.user_id

    def delete_prefered_workspace(self, user_id):
        entity_type = "prefered_workspace"
        doc_ref = self.db[entity_type].find_one({"user_id": user_id})
        if doc_ref is None:
            logger.error(f"{entity_type} for user {user_id} does not exists")
            status, rc, msg = "fail", 404, "prefered workspace does not exists"
            return make_response(jsonify({"status": status, "reason": msg}), rc)
        self.db[entity_type].delete_one(doc_ref)
        logger.info(f"{entity_type} for user {user_id} deleted")
        return user_id

    def update_prefered_workspace(self, user_id, workspace_id):
        """Updates an existing preferred workspace or inserts a new one if none exists for the user.
        :param user_id:
        :param workspace_id:
        :return:
        """
        entity_type = "prefered_workspace"
        set_data = {
            "user_id": user_id,
            "default_workspace_id": workspace_id,
        }
        self.db[entity_type].update_one(
            {"user_id": user_id},
            {"$set": set_data},
            upsert=True,
        )
        logger.info(f"{entity_type} for user {user_id} updated")
        return workspace_id

    def get_prefered_workspace(self, user_id, return_preferred_ws=False):
        """Returns an existing preferred workspace
        :param user_id:
        :param return_preferred_ws:
        :return:
        """
        entity_type = "prefered_workspace"
        doc_ref = self.db[entity_type].find_one(
            {
                "user_id": user_id,
            },
            {
                "_id": 0,
            },
        )

        if doc_ref is None:
            logger.error(f"{entity_type} for user {user_id} does not exists")
            # raise Exception(f"{entity_type} for user {user_id} does not exists")
            return None
        if return_preferred_ws:
            workspace_id = doc_ref.get("default_workspace_id")
            workspace = self.get_workspace_by_id(workspace_id)
            return workspace
        else:
            return True

    # Folder API

    def create_folder(self, folder):
        return self._create_entity(folder, "folder")

    def folder_by_name_exists(self, name, workspace_id, parent_folder):
        if name == "root":
            return True
        folders = list(
            self.db["folder"].find(
                {
                    "workspace_id": workspace_id,
                    "name": name,
                    "parent_folder": parent_folder,
                },
            ),
        )

        logger.info(
            f"{len(folders)} folders found matching name: {name}, workspace_id: {workspace_id}, parent_folder: {parent_folder}",
        )
        return len(folders) > 0

    def get_folder(self, name, workspace_id, parent_folder):
        folders_found = self.db["folder"].find(
            {
                "workspace_id": workspace_id,
                "parent_folder": parent_folder,
                "name": name,
            },
        )
        folders = []
        for folder in folders_found:
            del folder["_id"]
            folders.append(DocumentFolder(**folder))

        if len(folders) == 1:
            return folders[0]
        elif len(folders) < 1:
            return None
        else:
            raise Exception(
                f"multiple folders found with the same name {name} in workspace {workspace_id}, this cannot happen!!",
            )

    def get_folder_by_id(self, workspace_id, folder_id):
        folders_found = self.db["folder"].find(
            {"id": folder_id, "workspace_id": workspace_id},
        )
        folders = []
        for folder in folders_found:
            del folder["_id"]
            folders.append(DocumentFolder(**folder))

        if len(folders) == 1:
            return folders[0]
        elif len(folders) < 1:
            self.logger.error(f"no folder found with id {folder_id}")
            return None
        else:
            raise Exception(
                f"multiple folders found with the same id {folder_id}, this cannot happen!!",
            )

    def get_user(
        self,
        user_id,
        expand=False,
        projection=None,
        include_stripe_conf=False,
    ):
        """Get user info
        :param include_stripe_conf:
        :param user_id:
        :param expand: Return all the parameters
        :param projection: What parameters to return or not return.
        :return: return the user object
        """
        if not projection:
            projection = {
                "_id": 0,
            }
        if not include_stripe_conf:
            projection["stripe_conf.stripe_customer_id"] = 0
            projection["stripe_conf.subscriptions.stripe_product_plan_id"] = 0
            projection["stripe_conf.subscriptions.stripe_subscription_id"] = 0

        if not expand:
            projection["workspace_notification_settings"] = 0

        user_ref = self.db["user"].find_one(
            {
                "id": user_id,
            },
            projection,
        )
        user_list = []
        if user_ref:
            user_list.append(user_ref)
        users = [User(**u) for u in user_list]
        if len(users) == 1:
            user = users[0]
            return user
        elif len(users) < 1:
            self.logger.info(f"user with id {user_id} not found")
            return None
        else:
            self.logger.error(f"{len(users)} multiple users found with id {user_id}")
            raise Exception(f"multiple users found for user with id {user_id}")

    def get_user_by_stripe_customer_id(
        self,
        stripe_customer_id,
        expand=False,
        projection=None,
        include_stripe_conf=False,
    ):
        """Get user info
        :param include_stripe_conf:
        :param stripe_customer_id:
        :param expand: Return all the parameters
        :param projection: What parameters to return or not return.
        :return: return the user object
        """
        if not projection:
            projection = {
                "_id": 0,
            }
        if not include_stripe_conf:
            projection["stripe_conf.stripe_customer_id"] = 0
            projection["stripe_conf.subscriptions.stripe_product_plan_id"] = 0
            projection["stripe_conf.subscriptions.stripe_subscription_id"] = 0

        if not expand:
            projection["workspace_notification_settings"] = 0

        user_ref = self.db["user"].find_one(
            {
                "stripe_conf.stripe_customer_id": stripe_customer_id,
            },
            projection,
        )
        user_list = []
        if user_ref:
            user_list.append(user_ref)
        users = [User(**u) for u in user_list]
        if len(users) == 1:
            user = users[0]
            return user
        elif len(users) < 1:
            self.logger.info(
                f"user with stripe customer id {stripe_customer_id} not found",
            )
            return None
        else:
            self.logger.error(
                f"{len(users)} multiple users found with stripe customer id {stripe_customer_id}",
            )
            raise Exception(
                f"multiple users found for user with stripe customer id {stripe_customer_id}",
            )

    def is_user_email_matches_id(self, email_id, user_id):
        """Check is the provided email_id and user_id belong to the same user.
        :param email_id:`
        :param user_id:
        :return: True, if email_id and user_id belong to the same user, else False
        """
        user_obj = self.get_user(user_id)
        # logging.info("result from getting user:::::: " + str(user_obj) + " emailid:" + str(email_id))
        return user_obj and user_obj.email_id == email_id

    def get_folder_contents(
        self,
        workspace_id,
        folder_id="root",
        docs_per_page=10000,
        offset=0,
        projection=None,
        do_sort=True,
        name_contains="",
        name_startswith="",
        sort_method="name",
        reverse_sort=False,
        filter_struct=None,
    ):
        opt_query_params = False
        if folder_id != "root":
            raise ValueError(
                f"folder_id {folder_id} is not supported. Only root is allowed",
            )
        else:
            folder_info = DocumentFolder(
                id="root",
                name="root",
                workspace_id=workspace_id,
            )
        query = {
            "is_deleted": False,
            "parent_folder": folder_id,
            "workspace_id": workspace_id,
        }
        if filter_struct:
            if "status" in filter_struct:
                query["status"] = filter_struct["status"]
            if "filter_date_from" in filter_struct:
                filter_date_from = datetime.datetime.fromtimestamp(
                    filter_struct["filter_date_from"],
                )
                if filter_struct.get("filter_date_to", None):
                    filter_date_to = datetime.datetime.fromtimestamp(
                        filter_struct["filter_date_to"],
                    )
                else:
                    filter_date_to = datetime.datetime.now()
                query["$and"] = [
                    {"meta.pubDate": {"$gte": filter_date_from}},
                    {"meta.pubDate": {"$lte": filter_date_to}},
                ]
        query_params = {"blocks": 0, "_id": 0, "key_info": 0}

        if name_contains:
            # use regex, case insensitive
            # query["name"] = {"$regex": name_contains, "$options": "i"}
            # regex expression will not use any index and hence will take longer. Moving to a text index on name.
            # Partial matches are not supported by text indices
            query["$text"] = {"$search": f'"{name_contains}"', "$language": "none"}
            query_params = {"_id": 0, "id": 1, "name": 1}
            opt_query_params = True
        elif name_startswith:
            query["name"] = {"$regex": f"^{name_startswith}", "$options": "i"}
            query_params = {"_id": 0, "id": 1, "name": 1}
            opt_query_params = True

        if projection:
            query_params = projection
        # sort the documents by name in ascending order
        approx_count = self.db["document"].count_documents(
            query,
            limit=MAX_SELF_SORT_LIMIT,
        )

        if approx_count != MAX_SELF_SORT_LIMIT and not opt_query_params:
            document_stream = self.db["document"].find(query, query_params)
            documents = [Document(**d) for d in document_stream]
            if do_sort:
                if sort_method == "time":
                    documents = sorted(
                        documents,
                        key=lambda d: d["created_on"],
                        reverse=reverse_sort,
                    )
                elif sort_method == "name":
                    documents = natsorted(
                        documents,
                        key=lambda d: d["name"],
                        reverse=reverse_sort,
                    )
                elif sort_method == "size":
                    documents = sorted(
                        documents,
                        key=lambda d: d["file_size"],
                        reverse=reverse_sort,
                    )
                elif sort_method == "title":
                    documents = natsorted(
                        documents,
                        key=lambda d: (not d["inferred_title"], d["inferred_title"]),
                        reverse=reverse_sort,
                    )
                elif sort_method == "pubDate":
                    documents = natsorted(
                        documents,
                        key=lambda d: d["meta"]["pubDate"],
                        reverse=reverse_sort,
                    )
                else:
                    documents = natsorted(
                        documents,
                        key=lambda d: d["name"],
                        reverse=reverse_sort,
                    )

            documents = documents[offset : docs_per_page + offset]
        else:
            if do_sort:
                sort_order = 1 if not reverse_sort else -1
                if sort_method == "time":
                    sort_field = "created_on"
                elif sort_method == "size":
                    sort_field = "file_size"
                elif sort_method == "title":
                    sort_field = "inferred_title"
                elif sort_method == "pubDate":
                    sort_field = "meta.pubDate"
                else:
                    sort_field = "name"
                if not offset:
                    offset = 0
                document_stream = (
                    self.db["document"]
                    .find(query, query_params)
                    .sort(
                        [(sort_field, sort_order)],
                    )
                    .skip(offset)
                    .limit(docs_per_page)
                )
                if opt_query_params:
                    documents = [d for d in document_stream]
                else:
                    documents = [Document(**d) for d in document_stream]
            else:
                if offset is None:
                    offset = 0
                document_stream = (
                    self.db["document"]
                    .find(query, query_params)
                    .skip(offset)
                    .limit(docs_per_page)
                )
                if opt_query_params:
                    documents = [d for d in document_stream]
                else:
                    documents = [Document(**d) for d in document_stream]

        if do_sort:
            total_doc_count = self.db["document"].count_documents(query)
            return {
                **folder_info.to_dict(),
                "totalDocCount": total_doc_count,
                "documents": documents,
            }
        else:
            return {
                "documents": documents,
            }

    def get_docs_in_workspace(
        self,
        workspace_id,
        do_total_count=False,
        folder_id="root",
        docs_per_page=10000,
        offset=0,
        projection=None,
        opt_query_params={},
    ):
        if folder_id != "root":
            raise ValueError(
                f"folder_id {folder_id} is not supported. Only root is allowed",
            )
        query = {
            "is_deleted": False,
            "parent_folder": folder_id,
            "workspace_id": workspace_id,
        }
        if opt_query_params:
            query.update(opt_query_params)

        if not projection:
            projection = {"blocks": 0, "_id": 0, "key_info": 0}

        total_doc_count = None
        if do_total_count:
            total_doc_count = self.db["document"].count_documents(query)

        document_stream = (
            self.db["document"]
            .find(query, projection)
            .skip(offset)
            .limit(docs_per_page)
        )
        documents = [Document(**d) for d in document_stream]
        result = {
            "documents": documents,
        }
        if total_doc_count is not None:
            result["totalDocCount"] = total_doc_count

        return result

    def get_num_docs_in_folder(self, workspace_id, folder_id):
        """
        Return the number of documents in the folder corresponding to the given workspace_id
        :param workspace_id: Workspace ID
        :param folder_id: Folder ID
        :return: The number of documents in the folder.
        """
        query = {
            "is_deleted": False,
            "parent_folder": folder_id,
            "workspace_id": workspace_id,
        }
        return self.db["document"].find(query).count()

    def folder_exists(self, workspace_id, folder_id):
        folder = self.db["folder"].find_one({"id": folder_id})
        return (
            folder
            and "workspace_id" in folder.keys()
            and folder["workspace_id"] == workspace_id
        )

    # Document operations

    def create_document(self, document, document_json=None):
        self.create_file_history(
            document.user_id,
            document.workspace_id,
            document.id,
            "uploaded_document",
        )
        return self._create_entity(
            document,
            "document",
            entity_object_dict=document_json,
        )

    def set_document_info(self, document_id, data_to_set):
        """Updates an existing workspace
        :param document_id:
        :param data_to_set: field values to set
        :return:
        """
        self.db["document"].update_one(
            {"id": document_id},
            {"$set": data_to_set},
        )

    def update_document(self, document_id, doc_info):
        logging.info(f"updating document entry {document_id}")
        logging.info(f"{doc_info}, this is {doc_info.to_dict()}")
        old_id = doc_info.id
        old_doc_ws_id = doc_info.workspace_id
        old_doc_folder_id = doc_info.parent_folder
        if self.db["document"].find_one(
            {
                "id": old_id,
                "workspace_id": old_doc_ws_id,
                "parent_folder": old_doc_folder_id,
                "is_deleted": False,
            },
        ):
            self.db["document"].replace_one(
                {
                    "id": old_id,
                    "workspace_id": old_doc_ws_id,
                    "parent_folder": old_doc_folder_id,
                    "is_deleted": False,
                },
                doc_info.to_dict(),
            )
            logging.info(f"Updated document {old_id}")
            return True
        logging.info("DID NOT FIND DOCUMENT!")
        logging.info(f"Document with id: {old_id}, failed to update")
        return False

    def add_document_attribute(self, document_id, attribute_key, attribute_value):
        if self.get_document_info_by_id(document_id):
            if len(attribute_key) and len(attribute_value):
                self.db["document"].update_one(
                    {"id": document_id},
                    {"$set": {attribute_key: attribute_value}},
                )
                return True
        return False

    def rename_document(self, document_id, newname):
        logging.info(f"updating document name with ID {document_id}")
        # logging.info(f"{doc_info}, this is {doc_info.to_dict()}")
        doc_details = self.db["document"].find_one(
            {"id": document_id, "is_deleted": False},
        )
        doc_old_name = doc_details["name"]

        if self.db["document"].find_one({"id": document_id, "is_deleted": False}):
            self.db["document"].update_one(
                {"id": document_id, "is_deleted": False},
                {"$set": {"name": newname}},
            )
            logging.info(f"Updated document name from {doc_old_name} to {newname}")
            return newname
        logging.info("DID NOT FIND DOCUMENT!")
        logging.info(f"Document with name: {document_id}, failed to update")
        return False

    def document_exists(self, workspace_id, document_id):
        doc = self.db["document"].find_one(
            {"id": document_id, "workspace_id": workspace_id, "is_deleted": False},
        )
        return doc is not None

    def document_by_name_exists(self, name, workspace_id, folder_id):
        return (
            self.db["document"].find_one(
                {
                    "name": name,
                    "workspace_id": workspace_id,
                    "parent_folder": folder_id,
                    "is_deleted": False,
                },
            )
            is not None
        )

    def get_documents_by_name(self, name, workspace_id, folder_id, projection=None):
        query = {
            "is_deleted": False,
            "parent_folder": folder_id,
            "workspace_id": workspace_id,
            "name": name,
        }
        projection = projection or {}

        document_stream = self.db["document"].find(query, projection)
        documents = []
        if document_stream:
            documents = [Document(**d) for d in document_stream]
        return documents

    def get_document(self, workspace_id, document_id, filter_params=None):
        if not filter_params:
            filter_params = {
                "blocks": 0,
                "_id": 0,
                "key_info": 0,
            }
        db_doc = self.db["document"].find_one(
            {"id": document_id, "is_deleted": False},
            filter_params,
        )
        if db_doc is None:
            raise Exception(f"Document {document_id} not found")
        doc = Document(**db_doc)
        # user_id = db_doc["user_id"]
        # self.create_file_history(user_id, workspace_id, document_id, "opened_document")
        return doc

    def delete_document(self, document_id, permanent=False):
        # check auth, for now find and delete
        # self.db["document"].delete_one({"id": document_id})
        if not permanent:
            self.db["document"].update_one(
                {"id": document_id, "is_deleted": False},
                {"$set": {"is_deleted": True}},
            )
        else:
            self.db["document"].delete_one({"id": document_id})

        return Document(document_id)

    def get_document_info_by_id(
        self,
        document_id,
        return_dict=False,
        check_is_deleted=True,
    ):
        query = {
            "id": document_id,
        }
        if check_is_deleted:
            query["is_deleted"] = False
        db_doc = self.db["document"].find_one(
            query,
            {"blocks": 0, "_id": 0, "key_info": 0},
        )
        if db_doc is None:
            raise Exception(f"Document {document_id} not found")
        if not return_dict:
            doc = Document(**db_doc)
            return doc
        else:
            return db_doc

    def get_document_infos_by_ids(self, document_ids):
        cursor = self.db["document"].find(
            {"id": {"$in": document_ids}, "is_deleted": False},
            {"blocks": 0, "_id": 0, "key_info": 0},
        )
        docs = []
        for item in cursor:
            doc = Document(**item)
            docs.append(doc)
        return docs

    def get_document_info_by_source_url(self, source_url):
        db_doc = self.db["document"].find_one(
            {"source_url": source_url, "is_deleted": False},
            {"blocks": 0, "_id": 0, "key_info": 0},
        )
        if db_doc is None:
            raise Exception(f"Document {source_url} not found")
        doc = Document(**db_doc)
        return doc

    def get_document_key_info_by_id(self, document_id):
        db_doc = self.db["document"].find_one({"id": document_id}, {"key_info": 1})
        if db_doc is None:
            raise Exception(f"Document {document_id} not found")
        if "key_info" not in db_doc:
            db_doc["key_info"] = dict(section_summary=[], key_value_pairs=[])
        key_info = DocumentKeyInfo(**db_doc["key_info"])
        return key_info

    def get_document_reference_definitions_by_id(self, document_id):
        db_doc = self.db["document"].find_one(
            {"id": document_id},
            {"_id": 0, "key_info.reference_definitions": 1},
        )
        if db_doc is None:
            self.logger.info(f"Document {document_id} not found")
            return None
        return db_doc.get("key_info", {}).get("reference_definitions", {})

    def get_status_of_docs_in(self, workspace_id: str, folder_id: str = "root"):
        pipeline = [
            {
                "$match": {
                    "is_deleted": False,
                    "parent_folder": folder_id,
                    "workspace_id": workspace_id,
                },
            },
            {
                "$facet": {
                    "total": [
                        {"$match": {"status": {"$exists": True}}},
                        {"$count": "total"},
                    ],
                    "ingest_ok": [
                        {"$match": {"status": "ingest_ok"}},
                        {"$count": "ingest_ok"},
                    ],
                    "ingest_failed": [
                        {"$match": {"status": "ingest_failed"}},
                        {"$count": "ingest_failed"},
                    ],
                    "ready_for_ingestion": [
                        {"$match": {"status": "ready_for_ingestion"}},
                        {"$count": "ready_for_ingestion"},
                    ],
                    "ingest_inprogress": [
                        {"$match": {"status": "ingest_inprogress"}},
                        {"$count": "ingest_inprogress"},
                    ],
                },
            },
            {
                "$project": {
                    "total": {"$arrayElemAt": ["$total.total", 0]},
                    "ingest_ok": {"$arrayElemAt": ["$ingest_ok.ingest_ok", 0]},
                    "ingest_failed": {
                        "$arrayElemAt": ["$ingest_failed.ingest_failed", 0],
                    },
                    "ready_for_ingestion": {
                        "$arrayElemAt": ["$ready_for_ingestion.ready_for_ingestion", 0],
                    },
                    "ingest_inprogress": {
                        "$arrayElemAt": ["$ingest_inprogress.ingest_inprogress", 0],
                    },
                },
            },
        ]
        db_data = self.db["document"].aggregate(pipeline)
        output = {}
        if db_data:
            for d in db_data:
                output = d
                break
        return output

    # FieldBundle operations

    def create_field_bundle(self, field_bundle):
        if not field_bundle or not field_bundle.id:
            raise AttributeError("field bundle not initialized")
        entered_name = field_bundle.bundle_name
        workspace_id = field_bundle.workspace_id
        query = self.db["field_bundle"].find_one(
            {
                "workplace_id": {"$exists": True},
                "workspace_id": workspace_id,
                "bundle_name": entered_name,
            },
        )
        if query is not None:
            # raise AttributeError("field bundle not initialized")
            return None
        return self._create_entity(field_bundle, "field_bundle")

    def get_field_bundles_in_workspace(
        self,
        workspace_id,
        user_id=None,
    ):
        bundles = []
        query = {
            "workspace_id": workspace_id,
            "active": True,
        }
        if user_id:
            query = {
                "$or": [
                    {
                        "workspace_id": workspace_id,
                        "active": True,
                        "bundle_type": {"$in": ["PUBLIC", "DEFAULT"]},
                    },
                    {
                        "workspace_id": workspace_id,
                        "active": True,
                        "bundle_type": "PRIVATE",
                        "user_id": user_id,
                    },
                ],
            }
        for bundle in self.db["field_bundle"].find(
            query,
            {"_id": 0},
        ):
            bundles.append(FieldBundle(**bundle))
        self.logger.info(f"bundles {bundles}")
        return bundles

    def get_field_bundle_info(
        self,
        bundle_id,
        projection=None,
        return_dict=False,
    ):
        if not bundle_id:
            return None
        if not projection:
            projection = {"_id": 0}
        bundle_ref = self.db["field_bundle"].find_one(
            {"id": bundle_id},
            projection,
        )
        if bundle_ref:
            return bundle_ref if return_dict else FieldBundle(**bundle_ref)
        else:
            self.logger.error(f"bundles {bundle_id} not found")
            return None

    def get_default_field_bundle_info(
        self,
        workspace_id,
        projection=None,
        return_dict=False,
    ):
        if not workspace_id:
            return None
        if not projection:
            projection = {"_id": 0}
        query = {
            "workspace_id": workspace_id,
            "active": True,
            "bundle_type": "DEFAULT",
        }
        bundle_ref = self.db["field_bundle"].find_one(
            query,
            projection,
        )

        if bundle_ref:
            return bundle_ref if return_dict else FieldBundle(**bundle_ref)
        else:
            self.logger.error(
                f"Default field bundle not found for workspace {workspace_id} not found",
            )
            return None

    def delete_field_bundle(self, field_bundle_id):
        self.db["field_bundle"].delete_one({"id": field_bundle_id})
        return field_bundle_id

    def get_field_bundles_with_tag(self, tag) -> Optional[List[FieldBundle]]:
        # query = self.db.collection(u'field_bundle').where(u'tags', u'array_contains', tag)
        query = self.db["field_bundle"].find({"array_contains": tag})
        bundles = []
        for bundle in query:
            del bundle["_id"]
            bundles.append(FieldBundle(**bundle))
        return bundles

    def bundle_exists(self, bundle_id, workspace_id):
        bundle = self.db["field_bundle"].find_one({"id": bundle_id})
        if bundle:
            return (
                bundle_id
                and "workspace_id" in bundle.keys()
                and bundle["workspace_id"] == workspace_id
            )
        return False

    # Field operations

    def add_field_to_bundle(self, field_id, bundle_id):
        """Add an existing field to a field bundle
        :param field_id:
        :param bundle_id:
        :return:
        """
        bundle = self.db["field_bundle"].find_one({"id": bundle_id})
        if "field_ids" in bundle:
            field_ids = bundle["field_ids"]
        else:
            field_ids = []
        if field_id not in field_ids:
            field_ids.append(field_id)
        self.db["field_bundle"].update_one(
            {"id": bundle_id},
            {"$set": {"field_ids": field_ids}},
        )

    def add_fields_to_bundle(self, newly_created_field_id, bundle_id):
        """Add an existing field to a field bundle
        :param bundle_id:
        :param newly_created_field_id:
        :return:
        """
        self.db["field_bundle"].update_one(
            {"id": bundle_id},
            {"$push": {"field_ids": newly_created_field_id}},
        )

    def update_fields_in_bundle(self, field_ids, field_bundle_id):
        self.db["field_bundle"].update_one(
            {"id": field_bundle_id},
            {"$set": {"field_ids": field_ids}},
        )

    def update_field_bundle_attr(self, set_data, field_bundle_id):
        self.db["field_bundle"].update_one(
            {"id": field_bundle_id},
            {"$set": set_data},
        )

    def create_field(self, field):
        if not field.id or not field.name:
            raise AttributeError("field object not initialized")
        return self._create_entity(field, "field")

    def update_field_extraction_status(self, field_idx, action, **kwargs):
        if action == "queued":
            self.db["field"].update_one(
                {"id": field_idx},
                {"$set": {"status.progress": "extracting"}},
            )
        elif action == "extracting":
            self.db["field"].update_one(
                {"id": field_idx},
                {"$set": {"status.progress": "extracting"}},
            )
        elif action == "done":
            self.db["field"].update_one(
                {"id": field_idx},
                {"$set": {"status.progress": "done"}},
            )
        elif action == "batch_done":
            self.db["field"].update_one(
                {"id": field_idx},
                [
                    {
                        "$set": {
                            "status": {
                                "done": {
                                    "$add": [
                                        "$status.done",
                                        kwargs.get("doc_per_page", 0),
                                    ],
                                },
                            },
                        },
                    },
                    {
                        "$set": {
                            "status": {
                                "progress": {
                                    "$cond": {
                                        "if": {
                                            "$gte": ["$status.done", "$status.total"],
                                        },
                                        "then": "done",
                                        "else": "extracting",
                                    },
                                },
                            },
                        },
                    },
                ],
            )

    def get_field_status(
        self,
        field_id,
    ):
        field_ref = self.db["field"].find_one(
            {"id": field_id},
            {"_id": 0, "status": 1},
        )
        if field_ref:
            field_ref = self.unescape_mongo_data(field_ref)
        return field_ref

    def delete_field_by_field_id(
        self,
        field_id,
        update_bundle=True,
        field_details=None,
    ):
        if update_bundle:
            if not field_details:
                field_details = self.get_field_by_id(field_id)
            field_bundle_details = self.get_field_bundle_info(
                field_details.parent_bundle_id,
            )
            if field_bundle_details:
                parent_bundle_id = field_bundle_details.id
                # Deleting field Id in field bundle
                field_ids = field_bundle_details.field_ids
                field_ids.remove(field_id)
                if field_ids is None:
                    field_ids = []
                self.db["field_bundle"].update_one(
                    {"id": parent_bundle_id},
                    {"$set": {"field_ids": field_ids}},
                )
                logger.info(
                    f"new bundle is {self.get_field_bundle_info(parent_bundle_id)}",
                )

        # Deleting field values
        self.delete_extracted_field({"field_idx": field_id})

        # Deleting field
        self._delete_entity(field_id, "field")
        return field_id

    def get_fields_in_bundle(
        self,
        bundle_id: str,
        projection=None,
        return_dict: bool = False,
    ) -> Optional[List]:
        """Returns the list of the fields in the bundle
        :param bundle_id: field bundle id
        :param projection: projection to be applied.
        :param return_dict: Return dict or not
        :return: list of fields in the bundle
        """
        if not projection:
            projection = {"_id": 0}
        field_stream = self.db["field"].find(
            {"parent_bundle_id": bundle_id},
            projection,
        )
        field_stream = [self.unescape_mongo_data(field) for field in field_stream]
        fields = [field if return_dict else Field(**field) for field in field_stream]
        return fields

    def get_relation_fields_in_workspace(
        self,
        workspace_id: str,
        return_dict: bool = False,
    ) -> Optional[List]:
        """Returns the list of the fields in the workspace
        :param workspace_id: workspace id
        :param return_dict: Return dict or not
        :return: list of fields relation fields in the workspace
        """
        field_stream = self.db["field"].find(
            {
                "workspace_id": workspace_id,
                "search_criteria.search_type": {
                    "$in": ["relation-triple", "relation-node"],
                },
            },
            {"_id": 0},
        )
        fields = [field if return_dict else Field(**field) for field in field_stream]
        return fields

    def get_field_by_ids(self, field_ids, projection=None):
        if not projection:
            projection = {"_id": 0}
        field_stream = self.db["field"].find({"id": {"$in": field_ids}}, projection)
        # Preserve the original order in the list.
        id_dict = {}
        for field in field_stream:
            field = self.unescape_mongo_data(field)
            id_dict[field["id"]] = Field(**field)
        fields = []
        for field_id in field_ids:
            if field_id in id_dict:
                fields.append(id_dict[field_id])
        return fields

    def get_field_by_id(self, field_id):
        field_ref = self.db["field"].find_one(
            {"id": field_id},
            {"_id": 0},
        )
        if field_ref:
            field_ref = self.unescape_mongo_data(field_ref)
            return Field(**field_ref)

    def get_field_by_name(self, field_bundle_id, field_name):
        field_ref = self.db["field"].find_one(
            {"parent_bundle_id": field_bundle_id, "name": field_name},
        )
        if field_ref:
            field_ref = self.unescape_mongo_data(field_ref)
        return field_ref

    def field_exists(self, workspace_id, field_id):
        return self.db["field"].find_one(
            {"workspace_id": workspace_id, "id": field_id},
            {"_id": 0, "id": 1},
        )

    def update_field_by_id(self, field_id, field):
        """Update info of an existing user
        :param field_id: id of the filed to update
        :param field: field object
        :return: return id of the user to update
        """
        field = self.escape_mongo_data(field)
        return self._update_entity(field_id, field, "field")

    def update_field_attr(self, set_data, field_id):
        self.db["field"].update_one(
            {"id": field_id},
            {"$set": set_data},
        )

    def update_fields_status_from_ingestor(self, field_ids):
        self.db["field"].update_many(
            {"id": {"$in": field_ids}},
            [
                {
                    "$set": {
                        "status.total": {
                            "$cond": [
                                {"$not": ["$status.total"]},
                                1,
                                {"$add": ["$status.total", 1]},
                            ],
                        },
                        "status.done": {
                            "$cond": [
                                {"$not": ["$status.done"]},
                                1,
                                {"$add": ["$status.done", 1]},
                            ],
                        },
                        "status.progress": "done",
                    },
                },
            ],
        )

    # Template operations

    def create_template(self, template):
        if not template.id or not template.field_id or not template.text:
            raise AttributeError("template object is not fully initialized")
        return self._create_entity(template, "template")

    def is_template_exists_in_field(self, template_id, field_id):
        template = self.db["template"].find_one({"id": template_id})
        if template:
            return template["field_id"] == field_id
        return False

    def delete_template(self, template_id, field_id):
        self.db["template"].delete_one({"id": template_id})
        return

    def update_template(self, template_id, template):
        return self._update_entity(template_id, template, "template")

    def get_templates_for_field(self, field_id):
        """
        return [Template(**t.to_dict()) for t in
                self.db.collection(u'template').where(u'field_id', '==', field_id).where(u'active', '==', True).stream()]
        """
        self._not_impl()
        pass

    # Parsed blocks

    def get_parsed_blocks_for_document(self, doc_id: str) -> Optional[List]:
        # check if the document has been ingested
        doc_ref = self.db["document"].find_one({"id": doc_id, "is_deleted": False})
        if doc_ref and doc_ref["status"] == "ingest_ok":
            doc_blocks = doc_ref["blocks"]
            block_list = []
            for block in doc_blocks:
                block_list.append(block)
            return block_list
        else:
            raise Exception(f"document with id {doc_id} does not exist")

    # Ignore blocks

    def create_ignore_block(self, ignore_block: str) -> str:
        if not ignore_block.id:
            raise AttributeError("ignore block object not initialized")
        return self._create_entity(ignore_block, "ignore_block")

    def get_ignore_blocks(self, workspace_id: str) -> Optional[List]:
        ignore_blocks = []
        for ignore_block in self.db["ignore_block"].find(
            {"workspace_id": workspace_id},
        ):
            del ignore_block["_id"]
            ignore_block = IgnoreBlock(
                ignore_text=ignore_block["ignore_text"],
                ignore_all_after=ignore_block["ignore_all_after"],
                workspace_id=ignore_block["workspace_id"],
                block_type=ignore_block["block_type"],
            )
            ignore_blocks.append(ignore_block)
        return ignore_blocks
        # ignore_block_stream = self.db[u"ignore_block"].find({"workspace_id": workspace_id})
        # ignore_blocks = []
        # for i in ignore_block_stream:
        #     ignore_blocks.append(IgnoreBlock(**i.to_dict()))

        # return ignore_blocks

    def update_search_history(
        self,
        uniq_id,
        user_id,
        timestamp,
        workspace_id=None,
        doc_id=None,
        search_criteria=None,
    ):
        """
        Saves raw user search to database.
        Args:
            uniq_id: unique id for this search
            user_id (str): User id for the search.
            workspace_id (str): Workspace id for the search.
            doc_id (str): Doc id for the search.
            timestamp (List or str of one pattern): timestamp @ which the search was executed.
            search_criteria: search_criteria to save in database.
        Returns:
            None
        """

        info = {
            "id": uniq_id,
            "user_id": user_id,
            "doc_id": doc_id,
            "workspace_id": workspace_id,
            "timestamp": timestamp,
            "search_criteria": search_criteria,
        }

        search_history = SearchHistory(**info)

        return self._create_entity(search_history, "search_history", append=True)

    def get_search_history_by_days(
        self,
        user_id,
        id_type,
        days,
        workspace_id=None,
    ):
        """
        Get search history by days. Return in descending order
        Args:
            user_id (str): User id.
            id_type (str): "question" or "pattern".
            days (int): By last days
        :param workspace_id:
        Returns:
            history (List[(str, datetime)]): List of (history, timestamp) tuples
        Resources
        https://firebase.googleblog.com/2018/08/better-arrays-in-cloud-firestore.html
        https://stackoverflow.com/questions/46568142/google-firestore-query-on-substring-of-a-property-value-text-search
        """

        # Get time interval
        tod = datetime.datetime.now()
        d = datetime.timedelta(days=days)
        created_on = tod - d

        filter_query = {
            "user_id": user_id,
            "timestamp": {
                "$gte": created_on,
            },
        }
        if workspace_id:
            filter_query["workspace_id"] = workspace_id
        history = []
        for s in self.db["search_history"].find(filter_query, {"_id": 0}):
            history.append(SearchHistory(**s))
        history.sort(key=lambda x: x.timestamp, reverse=True)

        return history

    def get_audit_field_value(
        self,
        workspace_id,
        start_date_time,
        end_date_time,
    ):
        """
        Get document, field and field_value for a workspace
        """
        # Fetch all active documents in a workspace
        doc_stream = self.db["document"].find(
            {"workspace_id": workspace_id, "is_deleted": False},
            {"blocks": 0},
        )
        doc_ref = []
        for doc in doc_stream:
            del doc["_id"]
            doc_ref.append(Document(**doc))

        # Fetch all fields in a workspace
        field_stream = self.db["field"].find({"workspace_id": workspace_id}, {"_id": 0})
        field_ref = []
        for field in field_stream:
            field = self.unescape_mongo_data(field)
            field_ref.append(Field(**field))
        # Fetch all field values in a workspace for specific date range
        field_value_stream = self.db["field_value"].find(
            {
                "workspace_idx": workspace_id,
                "$and": [
                    {"last_modified": {"$gte": start_date_time}},
                    {"last_modified": {"$lte": end_date_time}},
                ],
            },
            {"_id": 0},
        )
        field_value_ref = []
        for field_value in field_value_stream:
            field_value = self.unescape_mongo_data(field_value)
            field_value["field_bundle_id"] = field_value["field_bundle_idx"]
            del field_value["field_bundle_idx"]
            field_value_ref.append(FieldValue(**field_value))
        return doc_ref, field_ref, field_value_ref

    def delete_document_blocks(self, doc_id):
        doc_ref = self.db["document"].find_one({"id": doc_id, "is_deleted": False})
        if doc_ref and "blocks" in doc_ref:
            blocks_ref = doc_ref["blocks"]
            blocks = [b for b in blocks_ref]
            self.logger.info(f"deleting {len(blocks)} for document id {doc_id}")
            self.db["document"].find_one_and_update(
                {"id": doc_id, "is_deleted": False},
                {"$set": {"blocks": []}},
            )
            return blocks

    def get_document_blocks(self, doc_id):
        doc_ref = self.db["document"].find_one(
            {"id": ObjectId(doc_id), "is_deleted": False},
        )
        if doc_ref.get().exists:
            blocks_ref = doc_ref["blocks"]
            blocks = [b for b in blocks_ref.stream()]
            self.logger.info(f"Retrieved {len(blocks)} for document id {doc_id}")
            return blocks
        else:
            self.logger.warning(
                f"entry for document id {doc_id} does not exists, no blocks retreived",
            )
            return None

    def save_document_blocks(self, doc_id, blocks):
        doc_ref = self.db["document"].find_one({"id": doc_id, "is_deleted": False})
        if doc_ref:
            self.db["document"].update(
                {"id": doc_id, "is_deleted": False},
                {"$set": {"blocks": blocks}},
            )
        else:
            self.logger.error("document not found")
        return doc_id

    def save_document_key_info(self, doc_id, key_info):
        doc_ref = self.db["document"].find_one({"id": doc_id})
        if doc_ref:
            self.db["document"].update_one(
                {"id": doc_id},
                {"$set": {"key_info": key_info}},
            )
        else:
            self.logger.error("document not found")
        return doc_id

    def set_document_status(
        self,
        doc_id,
        status=None,
        title=None,
        inferred_title=None,
        rendered_file_location=None,
        rendered_json_file_location=None,
        num_pages=None,
    ):
        if (
            not status
            and not title
            and not inferred_title
            and not rendered_file_location
            and not rendered_json_file_location
            and not num_pages
        ):
            return
        set_data = {}
        if status:
            set_data["status"] = status
        if title:
            set_data["title"] = title
        if inferred_title:
            set_data["inferred_title"] = inferred_title
        if rendered_file_location:
            set_data["rendered_file_location"] = rendered_file_location
        if rendered_json_file_location:
            set_data["rendered_json_file_location"] = rendered_json_file_location
        if num_pages is not None:
            set_data["num_pages"] = num_pages
        doc_ref = self.db["document"].find_one_and_update(
            {"id": doc_id, "is_deleted": False},
            {
                "$set": set_data,
            },
        )
        if doc_ref:
            self.logger.info(f"document {doc_id} updated with status {status}")
        else:
            self.logger.error(f"document {doc_id} not found, cannot update status")

    def set_document_url(self, doc_id, url):
        self.db["document"].find_one_and_update(
            {"id": doc_id, "is_deleted": False},
            {
                "$set": {
                    "source_url": url,
                },
            },
        )

    def get_template_by_id(self, template_id):
        return self.db["template"].find_one({"id": template_id})

    def get_template_in_workspace(self, workspace_id):
        cursor = self.db["template"].find({"workspace_id": workspace_id})
        templates = [template for template in cursor]
        return templates

    def create_excel_template(self, template):
        return self._create_entity(template, "template")

    def create_es_entries(self, es_entries, workspace_idx):
        if workspace_idx not in self.index_db.list_collection_names():
            self.index_db[workspace_idx].create_index("file_idx")
        if es_entries:
            result_ids = self.index_db[workspace_idx].insert_many(
                es_entries,
                ordered=False,
            )
            self.logger.info("elastic search result saved")
            return result_ids.inserted_ids

    def get_es_entry(self, es_ids, workspace_idx):
        return self.index_db[workspace_idx].find(
            {"_id": {"$in": es_ids}},
            batch_size=len(es_ids) + 1,
        )

    def get_match_es_entry(
        self,
        workspace_idx,
        file_idx,
        match_id_list,
        header_text,
        projection=None,
    ):
        query = {
            "match_idx": {"$in": match_id_list},
            "file_idx": file_idx,
        }

        if header_text:
            query["header_text"] = header_text

        if not projection:
            projection = {
                "_id": 0,
            }

        return self.index_db[workspace_idx].find(
            query,
            projection,
        )

    def remove_es_entry(self, file_idx, workspace_idx):
        self.index_db[workspace_idx].delete_many({"file_idx": file_idx})
        self.logger.info(f"file {file_idx} removed from elasticsearch result")

    def save_extraction_cache(self, caches, topic_idx, file_idx):
        self.db["cache"].delete_many(
            {"field_idx": topic_idx, "file_idx": {"$in": file_idx}},
        )
        if caches:
            self.db["cache"].insert_many(caches)

    def load_extraction_cache(
        self,
        field_bundle_idx,
        file_idx=None,
        workspace_idx=None,
        topn=None,
    ):
        projection = {}
        if topn and topn > 0:
            projection["matches"] = {"$slice": [0, max(topn, 1)]}

        if file_idx:
            query = {
                "field_bundle_idx": field_bundle_idx,
                "file_idx": file_idx,
            }
        elif workspace_idx:
            query = {
                "field_bundle_idx": field_bundle_idx,
                "workspace_idx": workspace_idx,
            }
        else:
            raise ValueError("need either file_idx or workspace_idx to load cache")

        return self.db["cache"].find(query, projection or None)

    def create_file_history(self, user_id, workspace_id, doc_id, action):
        # make the history record object
        timestamp = str_utils.timestamp_as_str()
        user_history = {
            "user_id": user_id,
            "workspace_id": workspace_id,
            "doc_id": doc_id,
            "timestamp": timestamp,
            # "action": "uploaded_document",
            "action": action,
            "details": {},
        }
        return self.db["history"].insert_one(user_history)

    def get_file_history_in_workspace(self, user_id, workspace_id, action):
        history = []
        for cursor in self.db["history"].find(
            {"user_id": user_id, "workspace_id": workspace_id, "action": action},
        ):
            del cursor["_id"]
            history.append(History(**cursor))
        # sort history by timestamp
        history.sort(key=lambda x: x.timestamp, reverse=True)
        return history

    def delete_file_history_in_workspace(self, user_id, workspace_id, action):
        """
        Delete the history matching the action.
        :param user_id:
        :param workspace_id:
        :param action:
        :return:
        """
        self.db["history"].delete_many(
            {"user_id": user_id, "workspace_id": workspace_id, "action": action},
        )

    def get_saved_graph(self, workspace_id, field_id):
        query = {"workspace_id": workspace_id, "field_id": field_id}
        return self.db["saved_graph"].find_one(query)

    def save_graph(self, workspace_id, field_id, graph_json, json_label="graph_json"):
        # make the history record object
        timestamp = str_utils.timestamp_as_str()

        saved_graph = {
            "workspace_id": workspace_id,
            "field_id": field_id,
            "timestamp": timestamp,
            json_label: graph_json,
        }
        query = {"workspace_id": workspace_id, "field_id": field_id}
        return self.db["saved_graph"].update_one(
            query,
            {"$set": saved_graph},
            upsert=True,
        )

    def create_ingestor_test_case(
        self,
        correct,
        block_html,
        correct_text,
        correct_type,
        block_text,
        block_type,
        document_id,
        workspace_id,
        page_idx,
        user_id,
    ):
        # check if entry in db exists
        # return message if so
        test_case = self.db["ingestor_test_cases"].find_one(
            {
                "correct": correct,
                "block_html": block_html,
                "correct_text": correct_text,
                "correct_type": correct_type,
                "block_text": block_text,
                "block_type": block_type,
                "document_id": document_id,
                "workspace_id": workspace_id,
                "page_idx": page_idx,
            },
        )
        if test_case:
            logging.info("Ingestor Test Case already exists")
        else:
            # otherwise insert
            tz = timezone("UTC")
            fmt = T_ZONE_FORMAT
            time_stamp = datetime.datetime.now(tz).strftime(fmt)
            self.db["ingestor_test_cases"].insert_one(
                {
                    "correct": correct,
                    "block_html": block_html,
                    "correct_text": correct_text,
                    "correct_type": correct_type,
                    "block_text": block_text,
                    "block_type": block_type,
                    "document_id": document_id,
                    "workspace_id": workspace_id,
                    "page_idx": page_idx,
                    "user_id": user_id,
                    "time_stamp": time_stamp,
                },
            )
            logging.info("Ingestor Test Case created")
        return

    def create_ingestor_table_test(self, table):
        test_case = self.db["ingestor_table_test_cases"].find_one(table)
        tz = timezone("UTC")
        fmt = T_ZONE_FORMAT
        time_stamp = datetime.datetime.now(tz).strftime(fmt)
        if test_case:
            logging.info("Ingestor Table Test Case already exists")
        else:
            table["time_stamp"] = time_stamp
            self.db["ingestor_table_test_cases"].insert_one(table)
        return

    def create_ingestor_page_test(self, page):
        test_case = self.db["ingestor_page_test_cases"].find_one(page)
        tz = timezone("UTC")
        fmt = T_ZONE_FORMAT
        time_stamp = datetime.datetime.now(tz).strftime(fmt)
        if test_case:
            logging.info("Ingestor Page Test Case already exists")
        else:
            page["time_stamp"] = time_stamp
            self.db["ingestor_page_test_cases"].insert_one(page)
        return

    def store_flagged_page(self, body):
        flagged_page = self.db["flagged_page"].find_one(body)
        if flagged_page:
            logging.info("Page already flagged")
            return
        else:
            tz = timezone("UTC")
            fmt = T_ZONE_FORMAT
            time_stamp = datetime.datetime.now(tz).strftime(fmt)
            body["time_stamp"] = time_stamp
            self.db["flagged_page"].insert_one(body)
            logging.info("Page flagged")
        return

    def store_flagged_table(self, body):
        flagged_page = self.db["flagged_table"].find_one(body)
        if flagged_page:
            logging.info("Page already flagged")
            return
        else:
            tz = timezone("UTC")
            fmt = T_ZONE_FORMAT
            time_stamp = datetime.datetime.now(tz).strftime(fmt)
            body["time_stamp"] = time_stamp
            self.db["flagged_table"].insert_one(body)
            logging.info("Table flagged")
        return

    def remove_flagged_page(self, body):
        flagged_page = self.db["flagged_page"].find_one(body)
        if flagged_page:
            self.db["flagged_page"].delete_one(body)
            logging.info("Removing Flagged Page")
        return

    def remove_flagged_table(self, body):
        flagged_page = self.db["flagged_table"].find_one(body)
        if flagged_page:
            self.db["flagged_table"].delete_one(body)
            logging.info("Removing Flagged Table")
        return

    def remove_ingestor_test_case(self, body):
        body = {
            "document_id": body["document_id"],
            "block_html": body["block_html"],
            "page_idx": body["page_idx"],
        }
        ingestor_test_case = self.db["ingestor_test_cases"].find_one(body)
        if ingestor_test_case:
            self.db["ingestor_test_cases"].delete_one(body)
        else:
            logging.info("ingestor test case not found")
        pass

    def remove_ingestor_table_test_case(self, body):
        table_test = {key: body[key] for key in ["doc_id", "page_idx"]}
        test_cases = self.db["ingestor_table_test_cases"].find(table_test)
        if test_cases:
            self.db["ingestor_table_test_cases"].delete_many(table_test)
            logging.info("Ingestor Table Test Cases removed on page")
        return

    def delete_saved_search_result(self, doc_id, unique_id):
        self.db["saved_search_results"].delete_one(
            {"doc_id": doc_id, "unique_id": unique_id},
        )

    def get_training_samples(self, status=["created", "retrain"]):
        if not status:
            status = ["created", "retrain"]
        ss_stream = self.db["saved_search_results"].find(
            {
                "action": {
                    "$in": ["correct", "incorrect", "edited", "entered", "failed"],
                },
                "status": {
                    "$in": status,
                },
            },
        )
        samples = []
        for item in ss_stream:
            if (
                "search_criteria" in item
                and item["search_criteria"]
                and len(item["search_criteria"]["criterias"]) > 0
            ):
                question = item["search_criteria"]["criterias"][0]["question"]
                if "criteria_question" in item["search_result"]:
                    question = item["search_result"]["criteria_question"]
                answer = item["search_result"]["answer"]
                phrase = item["search_result"]["phrase"]
                parent_text = (
                    item["search_result"]["parent_text"]
                    if "parent_text" in item["search_result"]
                    else ""
                )
                samples.append(
                    {
                        "id": item["unique_id"],
                        "action": item["action"],
                        "passage": phrase,
                        "question": question,
                        "parent_text": parent_text,
                        "status": item["status"],
                        "headers": item["search_result"]["hierarchy_headers"],
                        "header_text_terms": item["search_result"]["header_text_terms"],
                        "hierarchy_headers_text_terms": item["search_result"][
                            "hierarchy_headers_text_terms"
                        ],
                        "answer": answer,
                    },
                )
        return samples

    def get_unique_training_sample_status(self):
        training_status = self.db["saved_search_results"].aggregate(
            [
                {
                    "$match": {},
                },
                {
                    "$group": {
                        "_id": "$status",
                    },
                },
            ],
        )
        stat_return = []
        if training_status:
            stat_return = [i["_id"] for i in training_status]
        return stat_return

    def get_saved_searches_by_action(self, doc_id, action):
        ss_stream = self.db["saved_search_results"].find(
            {"doc_id": doc_id, "action": action},
        )
        ss_list = []
        for ss in ss_stream:
            del ss["_id"]
            ss_list.append(ss)
        self.logger.info(
            f"{len(ss_list)} saved searches returned for {doc_id} and {action}",
        )
        ss_result = [SavedSearchResult(**s) for s in ss_list]
        ss_result.sort(key=lambda x: x.created_on, reverse=True)
        return ss_result

    def update_saved_search_status(self, ids, status):
        if ids:
            self.db["saved_search_results"].update_many(
                {"unique_id": {"$in": ids}},
                {"$set": {"status": status}},
            )
            logger.info(f"status updated to {status} for {ids}")
            return True
        else:
            logger.error("Error in updating model_to_train: No Valid Params")
            return False

    def save_search_result(self, saved_search_result):
        data_to_save = saved_search_result.to_dict()
        self.db["saved_search_results"].insert_one(data_to_save)

    def store_search_result(self, body):
        # print(body)
        # identify test-cases by these params
        # print()
        search_result = {
            key: body[key]
            for key in ["doc_id", "workspace_id", "header_text", "raw_scores", "tags"]
        }
        search_answer_params = {
            key: body["search_answer"][key] for key in ["answer", "phrase", "page_idx"]
        }
        search_question_params = {
            key: body["search_criteria"][key]
            for key in [
                "template_text",
                "template_question",
                "header_text",
                "post_processors",
            ]
        }

        search_answer_params = convert_sub_dict_to_sub_object(
            search_answer_params,
            "search_answer",
        )
        search_question_params = convert_sub_dict_to_sub_object(
            search_question_params,
            "search_criteria",
        )

        search_result.update(search_answer_params)
        search_result.update(search_question_params)
        # search_result['search_answer'] = search_answer_params
        # search_result['search_criteria'] = search_question_params

        # print("Identify by")
        # print(search_result)
        # test = {"search_answer.file_id": '7dac1958'}

        test_case = self.db["search_result_tests"].find_one(search_result)
        if test_case:
            logging.info("search result already exists")
        else:
            logging.info("storing search result")
            tz = timezone("UTC")
            fmt = T_ZONE_FORMAT
            time_stamp = datetime.datetime.now(tz).strftime(fmt)
            body["time_stamp"] = time_stamp
            self.db["search_result_tests"].insert_one(body)
        return

    def remove_search_result(self, body):
        # print(body)
        # print("removing result")
        search_result = {
            key: body[key] for key in ["doc_id", "workspace_id", "header_text"]
        }
        search_answer_params = {
            key: body["search_answer"][key] for key in ["answer", "phrase"]
        }
        search_question_params = {
            key: body["search_criteria"][key]
            for key in [
                "template_text",
                "template_question",
                "header_text",
                "post_processors",
            ]
        }

        search_answer_params = convert_sub_dict_to_sub_object(
            search_answer_params,
            "search_answer",
        )
        search_question_params = convert_sub_dict_to_sub_object(
            search_question_params,
            "search_criteria",
        )

        search_result.update(search_answer_params)
        search_result.update(search_question_params)
        test_case = self.db["search_result_tests"].find_one(search_result)
        if test_case:
            self.db["search_result_tests"].delete_one(search_result)
            logging.info("Search Result Test Case removed")
        else:
            print(search_result)
            logging.info("Search Result Test Case not found")
        return

    def flag_search_result(self, body, image):
        search_result = {
            key: body[key]
            for key in ["doc_id", "workspace_id", "header_text", "raw_scores", "tags"]
        }
        search_answer_params = {
            key: body["search_answer"][key] for key in ["answer", "phrase", "page_idx"]
        }
        search_question_params = {
            key: body["search_criteria"][key]
            for key in [
                "template_text",
                "template_question",
                "header_text",
                "post_processors",
            ]
        }

        search_answer_params = convert_sub_dict_to_sub_object(
            search_answer_params,
            "search_answer",
        )
        search_question_params = convert_sub_dict_to_sub_object(
            search_question_params,
            "search_criteria",
        )

        search_result.update(search_answer_params)
        search_result.update(search_question_params)
        # search_result['search_answer'] = search_answer_params
        # search_result['search_criteria'] = search_question_params

        # print("Identify by")
        # print(search_result)
        # test = {"search_answer.file_id": '7dac1958'}
        test_case = self.db["flagged_search_result_tests"].find_one(search_result)
        if test_case:
            logging.info("search result already exists")
        else:
            logging.info("flagging search result")
            tz = timezone("UTC")
            fmt = T_ZONE_FORMAT
            time_stamp = datetime.datetime.now(tz).strftime(fmt)
            body["time_stamp"] = time_stamp
            self.db["flagged_search_result_tests"].insert_one(body)
            if image:
                test_case = self.db["flagged_search_result_tests"].find_one(body)
                screen_shot_path = store_screen_shot(image, test_case["_id"])
                self.db["flagged_search_result_tests"].update(
                    {"_id": test_case["_id"]},
                    {"$set": {"screen_shot_path": screen_shot_path}},
                )
        return

    def remove_flag_search_result(self, body):
        # print(body)
        # print("removing result")
        search_result = {
            key: body[key] for key in ["doc_id", "workspace_id", "header_text"]
        }
        search_answer_params = {
            key: body["search_answer"][key] for key in ["answer", "phrase", "page_idx"]
        }
        search_question_params = {
            key: body["search_criteria"][key]
            for key in [
                "template_text",
                "template_question",
                "header_text",
                "post_processors",
            ]
        }

        search_answer_params = convert_sub_dict_to_sub_object(
            search_answer_params,
            "search_answer",
        )
        search_question_params = convert_sub_dict_to_sub_object(
            search_question_params,
            "search_criteria",
        )

        search_result.update(search_answer_params)
        search_result.update(search_question_params)
        test_case = self.db["flagged_search_result_tests"].find_one(search_result)
        if test_case:
            self.db["flagged_search_result_tests"].delete_one(search_result)
            logging.info("Flagged search result removed")
        else:
            logging.info("Flagged search result not found")
        return

    def get_document_search_test_cases(self, ws_id, doc_id):
        if ws_id == "all":
            # get all test cases
            return [test for test in self.db["search_result_tests"].find({})]

        elif doc_id == "all":
            # get all test cases in workspace
            return [
                test
                for test in self.db["search_result_tests"].find({"workspace_id": ws_id})
            ]

        return [
            test
            for test in self.db["search_result_tests"].find(
                {"workspace_id": ws_id, "doc_id": doc_id},
            )
        ]

    def get_document_flagged_search_test_cases(self, ws_id, doc_id):
        if ws_id == "all":
            # get all test cases
            return [test for test in self.db["flagged_search_result_tests"].find({})]

        elif doc_id == "all":
            # get all test cases in workspace
            return [
                test
                for test in self.db["flagged_search_result_tests"].find(
                    {"workspace_id": ws_id},
                )
            ]

        return [
            test
            for test in self.db["flagged_search_result_tests"].find(
                {"workspace_id": ws_id, "doc_id": doc_id},
            )
        ]

    def create_access_log(self, access_log):
        access_log.update(
            {
                "datetime": datetime.datetime.now(),
                "url": request.url,
                "ip": request.headers.get("X-Forwarded-For", None)
                or request.remote_addr,
                "method": request.method,
                "user_agent": request.headers.get("User-Agent", None),
            },
        )
        self.db["access_logs"].insert_one(access_log)

    # API for Active Learning
    def create_training_sample(self, training_sample):
        """
        Creates the training sample.
        By default,
            a) model_to_train is set to None
            b) train_state is set to 'READY'
        :param training_sample:
        :return:
        """
        if not training_sample.id:
            raise AttributeError("training sample is not fully initialized")
        return self._create_entity(training_sample, "training_samples")

    def get_training_samples_for(self, model=None):
        """
        Retrieves the training samples for the specified model which are set to READY state.
        :param model: Model for which we need to retrieve training samples
        :return:
        """
        train_data_stream = self.db["training_samples"].find(
            {"model_to_train": model, "train_state": "READY"},
        )
        train_data = []
        for sample in train_data_stream:
            del sample["_id"]
            train_data.append(sample)
        self.logger.info(f"{len(train_data)} training samples returned for {model}")
        return [TrainSample(**s) for s in train_data]

    def update_training_state(
        self,
        state,
        sample_id_list=None,
        model=None,
        created_on=None,
    ):
        """
        Updates the training state either using the list of sample ids or
        the created_on & model parameter
        :param state: State to which sample has to be moved.
        :param sample_id_list: List of Training Sample IDs to be updated.
        :param model: Model for which the sample state has to be updated.
                    (Used in conjunction with created_on)
        :param created_on: Training Samples before the given time which needs to be updated.
        :return:
        """
        if sample_id_list:
            self.db["training_samples"].update_many(
                {"id": {"$in": sample_id_list}},
                {"$set": {"train_state": state}},
            )
            logger.info(f"Training state updated to {state} for {sample_id_list}")
            return True
        elif created_on and model:
            self.db["training_samples"].update_many(
                {
                    "model_to_train": model,
                    "train_state": "READY",
                    "created_on": {"$lte": created_on},
                },
                {"$set": {"train_state": state}},
            )
            logger.info(
                f"Training state updated to {state} for {model} with creation time lte {created_on}",
            )
            return True
        else:
            logger.error("Error in updating state: No Valid Params")
            return False

    def update_dest_model_in_sample(self, sample_id, model_name):
        """
        Updates the model_to_train parameter of the sample.
        Specifies for which model, this sample belongs to
        :param sample_id: Training Sample ID
        :param model_name: Model Name
        :return:
        """
        if sample_id:
            self.db["training_samples"].update_one(
                {"id": sample_id, "train_state": "READY"},
                {"$set": {"model_to_train": model_name}},
            )
            logger.info(f"Model updated to {model_name} for {sample_id}")
            return True
        else:
            logger.error("Error in updating model_to_train: No Valid Params")
            return False

    def save_bbox_bulk(self, file_idx, bboxes, override=False):
        audited_bboxes = {}

        # preseve audited bbox when override is False
        if not override:
            for bbox in self.db["bboxes"].find({"file_idx": file_idx, "audited": True}):
                if bbox["page_idx"] not in audited_bboxes:
                    audited_bboxes[bbox["page_idx"]] = []
                audited_bboxes[bbox["page_idx"]].append(bbox)

        unaudited_bboxes = []
        # check if bbox overlaps with audited bboxes
        for bbox in bboxes:
            overlaped = False
            # check for audited bbox in the same page
            for audited_bbox in audited_bboxes.get(bbox["page_idx"], []):
                if bbox_utils.check_overlap(bbox, audited_bbox):
                    overlaped = True
                    break
            # no overlap found, insert to db
            if not overlaped:
                unaudited_bboxes.append(bbox)

        # only insert unaudited bboxes to db
        self.db["bboxes"].delete_many({"file_idx": file_idx, "audited": False})
        self.db["bboxes"].insert_many(unaudited_bboxes)

    def save_bbox(self, new_bbox):
        # print(new_bbox)
        new_bbox["bbox"] = bbox_utils.align_bbox_to_features(new_bbox)
        # print(new_bbox)

        # find the existing bboxes
        existing_bboxes = self.db["bboxes"].find(
            {"file_idx": new_bbox.file_idx, "page_idx": new_bbox.page_idx},
        )

        overlap_bboxes = []
        for bbox in existing_bboxes:
            try:
                if bbox_utils.check_overlap(new_bbox, bbox):
                    overlap_bboxes.append(bbox)
            except ValueError as e:
                logger.error(f"Error when resolving overlaped bbox {e}, skipping")
                overlap_bboxes.append(bbox)

        if overlap_bboxes:
            logger.info(f"replacing overlapped bbox {overlap_bboxes}")
            self.db["bboxes"].delete_many(
                {"_id": {"$in": [x["_id"] for x in overlap_bboxes]}},
            )

        return self.db["bboxes"].insert(new_bbox.to_dict())

    def remove_bbox(self, doc_id, page_idx):
        res = self.db["bboxes"].update_many(
            {"file_idx": doc_id, "page_idx": int(page_idx), "audited": True},
            {"$set": {"audited": False}},
        )
        return res.modified_count

    def get_bbox_by_doc_id(self, doc_id, audited_only=False):
        query = {"file_idx": doc_id}
        if audited_only:
            query["audited"] = True
        bboxes = []
        for bbox in self.db["bboxes"].find(query):
            del bbox["_id"]
            bboxes.append(BBox(**bbox))
        return bboxes

    def delete_bbox_by_doc_id(self, doc_id, audited_only=False):
        query = {"file_idx": doc_id}
        if audited_only:
            query["audited"] = True
        return self.db["bboxes"].delete_many(query)

    def insert_task(self, user_id, task_name, task_body):
        task = {
            "body": task_body,
            "user_id": user_id,
            "task_name": task_name,
            "status": "queued",
            "timestamp": str_utils.timestamp_as_str(),
        }
        self.db["task"].insert_one(task)
        task["_id"] = str(task["_id"])
        return task

    def get_task(self, query, offset=0, task_per_page=10000):
        tasks = (
            self.db["task"]
            .find(query)
            .sort("_id", -1)
            .skip(offset)
            .limit(task_per_page)
        )
        cleaned_tasks = []
        for task in tasks:
            task["_id"] = str(task["_id"])
            cleaned_tasks.append(task)
        return cleaned_tasks

    def delete_task(self, task_id):
        self.db["task"].delete_one({"_id": ObjectId(task_id)})

    def create_extracted_field(self, extracted_fields):
        """
        This function update the extracted field to database and try to maintain the user-selected answers when exists.
        top_fact won't be override if topc_fact.type exists and is not null.
        """
        if not extracted_fields:
            return

        query = []

        extracted_fields = self.escape_mongo_data(extracted_fields)
        field_id_list = []
        workspace_id = None

        for field_values in extracted_fields:
            update_query = {
                "field_bundle_idx": field_values.pop("field_bundle_idx"),
                "field_idx": field_values.pop("field_idx"),
                "workspace_idx": field_values.pop("workspace_idx"),
                "file_idx": field_values.pop("file_idx"),
            }
            if update_query["field_idx"] not in field_id_list:
                field_id_list.append(update_query["field_idx"])
            if not workspace_id:
                workspace_id = update_query["workspace_idx"]

            if field_values.get("batch_idx", None):
                update_query["batch_idx"] = field_values.pop("batch_idx")

            query.append(
                UpdateOne(
                    update_query,
                    [
                        {
                            "$set": {
                                "topic_facts": field_values.get("topic_facts", []),
                                "top_fact": {
                                    "$cond": {
                                        "if": {
                                            "$not": [
                                                "$top_fact.type",
                                            ],
                                        },
                                        "then": field_values["topic_facts"][0]
                                        if "topic_facts" in field_values
                                        and field_values["topic_facts"]
                                        else {},
                                        "else": "$top_fact",
                                    },
                                },
                                "file_name": field_values.get("file_name", None),
                                "last_modified": "$$NOW",
                            },
                        },
                    ],
                    upsert=True,
                ),
            )
        ret_val = self.db["field_value"].bulk_write(query)
        for f in field_id_list:
            # Calculate distinct values.
            dist_data_ref = self.db["field_value"].aggregate(
                [
                    {
                        "$match": {
                            "workspace_idx": workspace_id,
                            "field_idx": f,
                        },
                    },
                    {
                        "$group": {
                            "_id": "$top_fact.answer_details.raw_value",
                        },
                    },
                ],
            )
            distinct_values = [d1["_id"] for d1 in dist_data_ref]
            logger.info(
                f"Calculating distinct values for field {f}",
            )
            # Update the distinct_values in field definition.
            self.db["field"].update_one(
                {
                    "id": f,
                },
                {
                    "$set": {
                        "distinct_values": distinct_values,
                    },
                },
            )
        return ret_val

    def bulk_approve_field_value(self, query):
        # only approve field_values that don't have top_fact.type
        query["top_fact.type"] = {"$exists": False}
        res = self.db["field_value"].update_many(
            query,
            {"$set": {"top_fact.type": "approve"}},
        )
        return res.modified_count

    def bulk_disapprove_field_value(self, query):
        # unset disapprove fields with top_fact.type == "approve"
        query["top_fact.type"] = "approve"
        res = self.db["field_value"].update_many(
            query,
            {"$unset": {"top_fact.type": ""}},
        )
        return res.modified_count

    def get_relation_edge_topic_facts(self, field_id, relation_head, relation_tail):
        relation_head = {"$regex": "^" + relation_head, "$options": "i"}
        relation_tail = {"$regex": "^" + relation_tail, "$options": "i"}
        fvs = self.db["field_value"].aggregate(
            [
                {
                    "$match": {
                        "field_idx": field_id,
                        "file_idx": "all_files",
                        "topic_facts.relation_head": relation_head,
                        "topic_facts.relation_tail": relation_tail,
                    },
                },
                {
                    "$unwind": "$topic_facts",
                },
                {
                    "$match": {
                        "topic_facts.relation_head": relation_head,
                        "topic_facts.relation_tail": relation_tail,
                    },
                },
                {
                    "$group": {
                        "_id": "$_id",
                        "topic_facts": {
                            "$push": "$topic_facts",
                        },
                    },
                },
            ],
        )
        topic_facts = []
        for fv in fvs:
            topic_facts.extend(fv["topic_facts"])
        return topic_facts

    def create_field_value(self, field_value: FieldValue):
        field_value.selected_row.update({"type": "override", "is_override": True})
        field_value.selected_row = self.escape_mongo_data(field_value.selected_row)
        field_value.selected_row = correct_legacy_answers(field_value.selected_row)
        logger.info(f"Updating field_value for {field_value.field_id}")
        ret_val = self.db["field_value"].update_one(
            {
                "field_idx": field_value.field_id,
                "file_idx": field_value.doc_id,
                "workspace_idx": field_value.workspace_id,
                "field_bundle_idx": field_value.field_bundle_id,
            },
            {
                "$push": {
                    "field_value_history": {
                        "$each": field_value.history,
                        "$position": 0,
                        # "$slice": 20
                    },
                },
                "$set": {
                    "top_fact": field_value.selected_row,
                    "file_name": field_value.doc_name,
                },
                "$currentDate": {"last_modified": {"$type": "date"}},
            },
            upsert=True,
        )
        # Update the distinct_values in field definition.
        # Don't update for relation extraction.
        if field_value.doc_id != "all_files":
            logger.info(f"Calculating distinct values for field {field_value.field_id}")
            dist_data_ref = self.db["field_value"].aggregate(
                [
                    {
                        "$match": {
                            "workspace_idx": field_value.workspace_id,
                            "field_idx": field_value.field_id,
                        },
                    },
                    {
                        "$group": {
                            "_id": "$top_fact.answer_details.raw_value",
                        },
                    },
                ],
            )
            distinct_values = [d1["_id"] for d1 in dist_data_ref]
            self.db["field"].update_one(
                {
                    "id": field_value.field_id,
                },
                {
                    "$set": {
                        "distinct_values": distinct_values,
                    },
                },
            )
        # UnEscape the data, so that UI can use it to display in the cell.
        field_value.selected_row = self.unescape_mongo_data(field_value.selected_row)
        return ret_val

    def update_file_name_in_field_value(self, workspace_idx, file_idx, file_name):
        query = {
            "workspace_id": workspace_idx,
            "active": True,
        }
        for bundle in self.db["field_bundle"].find(
            query,
            {"_id": 0, "id": 1},
        ):
            field_bundle_idx = bundle["id"]
            field_projection = {
                "_id": 0,
                "id": 1,
            }
            fields = self.get_fields_in_bundle(
                field_bundle_idx,
                projection=field_projection,
                return_dict=True,
            )
            for field in fields:
                self.db["field_value"].update_one(
                    {
                        "field_idx": field["id"],
                        "file_idx": file_idx,
                        "workspace_idx": workspace_idx,
                        "field_bundle_idx": field_bundle_idx,
                    },
                    {
                        "$set": {
                            "file_name": file_name,
                        },
                    },
                )

    def create_workflow_fields_from_doc_meta(
        self,
        workspace_idx,
        field_bundle_idx,
        field_idx,
        doc_meta_param,
        user_name,
        edited_time,
        file_idx=None,
    ):
        doc_projection = {
            "_id": 0,
            "id": 1,
            "name": 1,
            f"meta.{doc_meta_param}": 1,
        }
        doc_query = {
            "is_deleted": False,
            "parent_folder": "root",
            "workspace_id": workspace_idx,
        }
        if not file_idx:
            doc_query["status"] = "ingest_ok"  # Do only for ingested document
        else:
            doc_query["id"] = file_idx
        cnt = 0
        for doc in self.db["document"].find(doc_query, doc_projection):
            cnt += 1
            meta_value = self.escape_mongo_data(doc["meta"].get(doc_meta_param, ""))
            top_fact = {
                "answer": meta_value,
                "formatted_answer": meta_value,
                "answer_details": {
                    "raw_value": meta_value,
                    "formatted_value": meta_value,
                },
                "type": "override",
                "match_idx": "manual",
                "is_override": True,
            }
            history = {
                "username": user_name,
                "edited_time": edited_time,
                "previous": None,
                "modified": top_fact,
            }
            history_list = [history]
            logger.info(
                f"Creating meta dependent workflow field for document {doc['id']} "
                f"for {workspace_idx} - {field_bundle_idx} - {field_idx}",
            )
            self.db["field_value"].update_one(
                {
                    "field_idx": field_idx,
                    "file_idx": doc["id"],
                    "workspace_idx": workspace_idx,
                    "field_bundle_idx": field_bundle_idx,
                },
                {
                    "$push": {
                        "field_value_history": {
                            "$each": history_list,
                            "$position": 0,
                            # "$slice": 20
                        },
                    },
                    "$set": {
                        "top_fact": top_fact,
                        "file_name": doc["name"],
                    },
                    "$currentDate": {"last_modified": {"$type": "date"}},
                },
                upsert=True,
            )

        dist_data_ref = self.db["field_value"].aggregate(
            [
                {
                    "$match": {
                        "workspace_idx": workspace_idx,
                        "field_bundle_idx": field_bundle_idx,
                        "field_idx": field_idx,
                    },
                },
                {
                    "$group": {
                        "_id": "$top_fact.answer_details.raw_value",
                    },
                },
            ],
        )
        distinct_values = [d1["_id"] for d1 in dist_data_ref]
        self.db["field"].update_one(
            {
                "id": field_idx,
            },
            {
                "$set": {
                    "distinct_values": distinct_values,
                },
            },
        )
        return cnt

    def create_cast_workflow_field(
        self,
        workspace_idx,
        field_bundle_idx,
        field_idx,
        field_options,
        user_name,
        edited_time,
        file_idx=None,
    ):
        parent_fields = field_options.get("parent_fields", [])
        query = {
            "workspace_idx": workspace_idx,
            "field_bundle_idx": field_bundle_idx,
            "field_idx": parent_fields[0],
        }
        if file_idx:
            query["file_idx"] = file_idx

        projection = {
            "_id": 0,
            "top_fact": 1,
            "file_idx": 1,
            "file_name": 1,
        }
        cast_options = field_options.get("cast_options", {})
        cnt = 0
        for ingress_field_value in self.db["field_value"].find(query, projection):
            cnt += 1
            egress_history = (None,)
            egress_top_fact = None
            ingress_top_fact = ingress_field_value.get("top_fact", None)
            if not ingress_top_fact or not ingress_top_fact.get("answer_details", {}):
                if (
                    None in cast_options
                    or NONE_CAST_OPTION_KEY in cast_options
                    or DEFAULT_CAST_OPTION_KEY in cast_options
                ):
                    value = cast_options.get(None, None) or cast_options.get(
                        NONE_CAST_OPTION_KEY,
                        None,
                    )
                    if value is None:
                        value = cast_options.get(DEFAULT_CAST_OPTION_KEY, "")
                    (
                        egress_history,
                        egress_top_fact,
                    ) = create_workflow_field_value_params(
                        value,
                        user_name,
                        edited_time,
                    )
            else:
                ingress_answer = ingress_top_fact.get("answer_details", {}).get(
                    "raw_value",
                    "",
                )
                if ingress_answer is None and NONE_CAST_OPTION_KEY in cast_options:
                    value = cast_options.get(NONE_CAST_OPTION_KEY, None)
                else:
                    value = cast_options.get(ingress_answer, None) or cast_options.get(
                        DEFAULT_CAST_OPTION_KEY,
                        "",
                    )
                egress_history, egress_top_fact = create_workflow_field_value_params(
                    value,
                    user_name,
                    edited_time,
                )
            if egress_history and egress_top_fact:
                egress_history_list = [egress_history]
                logger.info(
                    f"Creating cast workflow field for document {ingress_field_value['file_idx']} "
                    f"for {workspace_idx} - {field_bundle_idx} - {field_idx}",
                )
                self.upsert_workflow_field_value_entry(
                    workspace_idx,
                    field_bundle_idx,
                    field_idx,
                    ingress_field_value["file_idx"],
                    ingress_field_value["file_name"],
                    egress_history_list,
                    egress_top_fact,
                )
        return cnt

    def create_boolean_multi_cast_workflow_field(
        self,
        workspace_idx,
        field_bundle_idx,
        field_idx,
        field_options,
        user_name,
        edited_time,
        file_idx=None,
    ):
        parent_fields = field_options.get("parent_fields", [])
        field_values_ordered_by_file = self.download_grid_data_from_field_values(
            workspace_idx,
            field_bundle_idx,
            file_idx=file_idx,
            field_ids=parent_fields,
            include_file_idx=True,
        )

        cast_options = field_options.get("cast_options", {})
        cnt = 0

        for field_value in field_values_ordered_by_file:
            cnt += 1
            value_list = []
            for k, v in field_value.items():
                if k in cast_options:
                    if v is not None:
                        if (
                            v.lower() in BOOLEAN_MULTI_CAST_PERMISSIBLE_VALUES
                            or v in BOOLEAN_MULTI_CAST_PERMISSIBLE_VALUES
                        ):
                            value_list.append(cast_options[k])
                    elif (
                        NONE_CAST_OPTION_KEY in cast_options
                        or DEFAULT_CAST_OPTION_KEY in cast_options
                    ):
                        value = cast_options.get(
                            NONE_CAST_OPTION_KEY,
                            None,
                        )
                        if value is None:
                            value = cast_options.get(
                                DEFAULT_CAST_OPTION_KEY,
                                None,
                            )
                        if value:
                            value_list.append(value)

            (egress_history, egress_top_fact) = create_workflow_field_value_params(
                value_list,
                user_name,
                edited_time,
            )
            if egress_history and egress_top_fact:
                egress_history_list = [egress_history]
                logger.info(
                    f"Creating boolean multi cast workflow field for document {field_value['file_idx']} "
                    f"for {workspace_idx} - {field_bundle_idx} - {field_idx}",
                )
                self.upsert_workflow_field_value_entry(
                    workspace_idx,
                    field_bundle_idx,
                    field_idx,
                    field_value["file_idx"],
                    field_value["file_name"],
                    egress_history_list,
                    egress_top_fact,
                )

        return cnt

    def create_formula_workflow_field(
        self,
        workspace_idx,
        field_bundle_idx,
        field_idx,
        field_options,
        user_name,
        edited_time,
        file_idx=None,
    ):
        parent_fields = field_options.get("parent_fields", [])
        field_values_ordered_by_file = self.download_grid_data_from_field_values(
            workspace_idx,
            field_bundle_idx,
            file_idx=file_idx,
            field_ids=parent_fields,
            include_file_idx=True,
        )
        formula_options = field_options.get("formula_options", {})
        formula_str = formula_options.get("formula_str", "")
        formula_field_map = formula_options.get("formula_field_map", {})
        formula_output_cast = formula_options.get("formula_output_cast", {})
        formula_format_output = formula_options.get("formula_format_output", "")
        formula_format_output_options = {
            "text": str,
            "integer": int,
            "boolean": bool,
            "float": float,
        }
        cnt = 0

        for field_value in field_values_ordered_by_file:
            cnt += 1
            value = None
            new_field_value = {}
            for k, v in field_value.items():
                if k in formula_field_map:
                    new_field_value[formula_field_map[k]] = v
                else:
                    new_field_value[k] = v
            try:
                value = evaluate_formula(formula_str, new_field_value)
            except Exception as e:
                logger.error(
                    f"Evaluation failed {formula_str} - {str(e)}, err: {traceback.format_exc()}",
                )

            if value is not None and isinstance(value, bool):
                value = str(value).lower()
            if value is None and NONE_CAST_OPTION_KEY in formula_output_cast:
                value = formula_output_cast.get(NONE_CAST_OPTION_KEY, "")
            else:
                temp_value = formula_output_cast.get(
                    value,
                    None,
                )
                if temp_value is None and formula_output_cast.get(
                    DEFAULT_CAST_OPTION_KEY,
                    None,
                ):
                    temp_value = formula_output_cast.get(DEFAULT_CAST_OPTION_KEY, None)
                if temp_value is not None:
                    value = temp_value
            if (
                value is not None
                and formula_format_output
                and formula_format_output in formula_format_output_options
            ):
                apply = formula_format_output_options[formula_format_output]
                value = apply(value)
            elif value is None:
                value = ""
            egress_history, egress_top_fact = create_workflow_field_value_params(
                value,
                user_name,
                edited_time,
            )
            egress_history_list = [egress_history]
            logger.info(
                f"Creating formula workflow field for document {new_field_value['file_idx']} "
                f"for {workspace_idx} - {field_bundle_idx} - {field_idx}",
            )
            self.upsert_workflow_field_value_entry(
                workspace_idx,
                field_bundle_idx,
                field_idx,
                new_field_value["file_idx"],
                new_field_value["file_name"],
                egress_history_list,
                egress_top_fact,
            )
        return cnt

    def upsert_workflow_field_value_entry(
        self,
        workspace_idx,
        field_bundle_idx,
        field_idx,
        file_idx,
        file_name,
        history_list,
        top_fact,
    ):
        self.db["field_value"].update_one(
            {
                "field_idx": field_idx,
                "file_idx": file_idx,
                "workspace_idx": workspace_idx,
                "field_bundle_idx": field_bundle_idx,
            },
            {
                "$push": {
                    "field_value_history": {
                        "$each": history_list,
                        "$position": 0,
                    },
                },
                "$set": {
                    "top_fact": top_fact,
                    "file_name": file_name,
                },
                "$currentDate": {"last_modified": {"$type": "date"}},
            },
            upsert=True,
        )

    def create_fields_dependent_workflow_field_values(
        self,
        workspace_idx,
        field_bundle_idx,
        field_idx,
        field_options,
        user_name,
        edited_time,
        file_idx=None,
    ):
        if (
            not field_options
            or not field_options.get("deduct_from_fields", False)
            or not field_options.get("parent_fields", [])
            or field_options.get("type", "") not in DEPENDENT_FIELD_ALLOWED_TYPES
        ):
            logger.info(
                f"Invalid field options {field_options} for {workspace_idx} - {field_bundle_idx} - {field_idx}",
            )
            raise ValueError(
                "Invalid field options while performing create_fields_dependent_workflow_field_values",
            )
        cnt = 0
        dependent_field_type = field_options.get("type", "")
        logger.info(
            f"Creating fields dependent workflow field with type {dependent_field_type} --- "
            f"{field_options} for {workspace_idx} - {field_bundle_idx} - {field_idx}",
        )
        if CAST_FIELD_TYPE == dependent_field_type:
            cnt = self.create_cast_workflow_field(
                workspace_idx,
                field_bundle_idx,
                field_idx,
                field_options,
                user_name,
                edited_time,
                file_idx,
            )
        elif BOOLEAN_MULTI_CAST_FIELD_TYPE == dependent_field_type:
            cnt = self.create_boolean_multi_cast_workflow_field(
                workspace_idx,
                field_bundle_idx,
                field_idx,
                field_options,
                user_name,
                edited_time,
                file_idx,
            )
        elif FORMULA_FIELD_TYPE == dependent_field_type:
            cnt = self.create_formula_workflow_field(
                workspace_idx,
                field_bundle_idx,
                field_idx,
                field_options,
                user_name,
                edited_time,
                file_idx,
            )

        # Create the unique value list.
        dist_data_ref = self.db["field_value"].aggregate(
            [
                {
                    "$match": {
                        "workspace_idx": workspace_idx,
                        "field_bundle_idx": field_bundle_idx,
                        "field_idx": field_idx,
                    },
                },
                {
                    "$group": {
                        "_id": "$top_fact.answer_details.raw_value",
                    },
                },
            ],
        )
        distinct_values = [d1["_id"] for d1 in dist_data_ref]
        self.db["field"].update_one(
            {
                "id": field_idx,
            },
            {
                "$set": {
                    "distinct_values": distinct_values,
                },
            },
        )
        return cnt

    def delete_field_value(
        self,
        field_id,
        doc_id,
        workspace_id,
        permanent=False,
        field_bundle_idx=None,
    ):
        db_query = {
            "field_idx": field_id,
            "file_idx": doc_id,
            "workspace_idx": workspace_id,
        }
        if field_bundle_idx:
            db_query["field_bundle_idx"] = field_bundle_idx

        if permanent:
            self.logger.info(f"Deleting field value for {field_id} from {doc_id}")
            ret_val = self.db["field_value"].delete_one(db_query)
        else:
            existing_field_values = self.read_extracted_field(
                db_query,
                {"_id": 0, "topic_facts": 1},
            )

            try:
                topic_facts = None
                if len(existing_field_values) > 0:
                    topic_facts = existing_field_values[0].get("topic_facts", None)

                if topic_facts:
                    extracted_top_fact = topic_facts[0]
                    extracted_top_fact = correct_legacy_answers(extracted_top_fact)
                else:
                    extracted_top_fact = {}

            except StopIteration:
                raise ValueError(
                    f"Field_value not found for field_idx: {field_id}, file_idx: {doc_id}",
                )

            self.db["field_value"].update_one(
                db_query,
                {
                    "$set": {
                        "top_fact": self.escape_mongo_data(extracted_top_fact),
                    },
                    "$currentDate": {"last_modified": {"$type": "date"}},
                },
            )
            ret_val = self.unescape_mongo_data(extracted_top_fact)

        # Calculate distinct values.
        dist_data_ref = self.db["field_value"].aggregate(
            [
                {
                    "$match": {
                        "workspace_idx": workspace_id,
                        "field_idx": field_id,
                    },
                },
                {
                    "$group": {
                        "_id": "$top_fact.answer_details.raw_value",
                    },
                },
            ],
        )
        distinct_values = [d1["_id"] for d1 in dist_data_ref]
        logger.info(f"Calculating distinct values for field {field_id}")
        # Update the distinct_values in field definition.
        self.db["field"].update_one(
            {
                "id": field_id,
            },
            {
                "$set": {
                    "distinct_values": distinct_values,
                },
            },
        )
        return ret_val

    def add_results_to_extracted_field(self, field_id, new_results, batch_idx=""):
        logger.info(
            f"adding {len(new_results)} items to field with id: {field_id} and batch_id: {batch_idx}",
        )
        query = {
            "field_idx": field_id,
        }
        if batch_idx:
            query["batch_idx"] = batch_idx
        self.db["field_value"].update_one(
            query,
            {"$push": {"topic_facts": {"$each": new_results}}},
        )

    def read_extracted_field(self, condition, projection=None, count_only=False):
        if "field_idx" not in condition and "field_bundle_idx" not in condition:
            raise ValueError(
                "must specify 'field_idx' or 'field_bundle_idx' when reading extracted fields",
            )
        if "file_idx" not in condition and "workspace_idx" not in condition:
            raise ValueError(
                "must specify 'file_idx' or 'workspace_idx' when reading extracted fields",
            )
        if count_only:
            return self.db["field_value"].count_documents(condition, projection)
        else:
            data = self.db["field_value"].find(condition, projection)
            return self.unescape_mongo_data(list(data))

    def delete_extracted_field(self, condition):
        if "field_idx" not in condition and "field_bundle_idx" not in condition:
            raise ValueError(
                "must specify 'field_idx' or 'field_bundle_idx' when deleting extracted fields",
            )

        return self.db["field_value"].delete_many(condition)

    def retrieve_grid_data(
        self,
        workspace_id,
        field_bundle_id,
        file_id=None,
        field_ids=None,
        limit=25,
        skip=0,
        sort_tuple_list=None,
        filter_dict=None,
        group_by_list=None,
        value_aggregate_list=None,
    ):
        """
        workspace_id (Mandatory): Determines the workspace for which the field bundle data needs to be retrieved.
        field_bundle_id (Mandatory): Determines the field bundle id for which the grid data needs to be retrieved.
        file_id (Optional): When provided determines the field values corresponding to which file has to be retrieved.
        field_ids (Optional): When provided the retrieved data will comprise of only those fields
        limit (Optional): Limit the number of documents returned. To be used in pagination. Default to 25
        skip (Optional): Skip the initial number of documents. To be used in pagination. Default to 0
        sort_tuple_list (Optional): By default the returned documents will be sorted by name,
            specify any additional sort that is required.
            e.g.
                sort_tuple_list = [
                    ("cb870a04.answer_details.raw_value", 1)
                ]
        filter_dict (Optional): Filter query to be used.
            Filter and group_by doesn't co-occur on the same action in grid data retrieval.
            e.g.
                filter_dict = {
                    "a41ecdfa.answer_details.raw_value": {"$gt":575000000, "$lt": 2500000000}
                }
        group_by_list (Optional): List of row_groups to be created.
            e.g.
                group_by_list = ["d3c9657d", "fde9d7ce", "28aeb6c1"]  # List of columns on which grouping needs
                to be applied
        value_aggregate_list (Optional): An optional list of aggregation functions to be applied on columns.
        TODO: Need to add code when the fields are not present. Results are wrong because of the literal in
        aggregate projection.
                        {
                  "result": [
                    {
                      "_id": "",
                      "child_total": 1,
                      "docs": [
                        {
                          "_id": "",
                          "child_total": 1,
                          "id": "9844ba0b"
                        }
                      ],
                      "id": "28aeb6c1"
                    }
                  ]
                }
        """
        if not (workspace_id and field_bundle_id):
            logger.info(
                "One or Both of the mandatory fields (Workspace ID or Field Bundle Id) is missing",
            )
            raise ValueError(
                "must specify 'workspace_id' and 'field_bundle_id' when deleting extracted fields",
            )
        logger.info(
            f"Extracting Grid data for workspace {workspace_id} "
            f"and field bundle {field_bundle_id}",
        )
        grid_collection_name = f"field_bundle_grid_{workspace_id}_{field_bundle_id}"
        # Performing a check for the existence of the collection name increases the latency manifold.
        output = {}
        result = []
        db_data = None
        # Construct the query here.
        # Assuming all the answers are in answer_details
        # Sort Dictionary
        sort_tuple_list = sort_tuple_list or []
        if group_by_list:
            pipeline = []
            grp_len = len(group_by_list)
            # consider only filter_dict, group_by_list and value_aggregate_list
            if filter_dict:
                pipeline.append(
                    {
                        "$match": filter_dict,
                    },
                )
            init_pipeline_len = len(pipeline)
            # GroupBy List
            for idx, g in enumerate(group_by_list[::-1]):
                if idx == 0:
                    grp_pipeline = {
                        "$group": {
                            "_id": {
                                g: {"$ifNull": [f"${g}.answer_details.raw_value", ""]}
                                for g in group_by_list
                            }
                            if grp_len - idx > 1
                            else {"$ifNull": [f"${g}.answer_details.raw_value", ""]},
                            "child_total": {"$sum": 1},
                        },
                    }
                else:
                    grp_pipeline = {
                        "$group": {
                            "_id": {
                                g: f"$_id.{g}" for g in group_by_list[: grp_len - idx]
                            }
                            if grp_len - idx > 1
                            else f"$_id.{g}",
                            "docs": {
                                "$push": {
                                    "_id": f"$_id.{group_by_list[grp_len - idx - 1]}",
                                    "child_total": "$child_total",
                                    "docs": "$docs",
                                },
                            },
                            "child_total": {"$sum": "$child_total"},
                        },
                    }
                pipeline.append(grp_pipeline)

            projection = {}
            projection_head = None
            grp_pipeline_len = len(pipeline) - init_pipeline_len
            for i in range(grp_pipeline_len):
                if i == 0:
                    projection = {
                        "$project": {
                            "_id": 1,
                            "id": {"$literal": group_by_list[i]},
                            "child_total": 1,
                        },
                    }
                    if i != grp_pipeline_len - 1:
                        projection["$project"]["docs"] = {}
                        projection_head = projection["$project"]
                else:
                    projection_head["docs"] = {
                        "_id": 1,
                        "id": {"$literal": group_by_list[i]},
                        "child_total": 1,
                    }
                    if i == grp_pipeline_len - 1:
                        projection_head["docs"]["docs"] = 1
                    else:
                        projection_head["docs"]["docs"] = {}
                        projection_head = projection_head["docs"]

            pipeline.append(projection)
            db_data = self.db[f"{grid_collection_name}"].aggregate(pipeline)
        else:
            filter_dict = filter_dict or {}
            if file_id:
                filter_dict["file_idx"] = file_id
            if "file_name" not in next(iter(list(zip(*sort_tuple_list))), []):
                sort_tuple_list.append(("file_name", 1))
            # Return all the attributes of the documents if projections (fieldIds) are not specified.
            projection = {
                "_id": 0,
            }
            if field_ids:
                projection["file_idx"] = 1
                projection["file_name"] = 1
                for f_id in field_ids:
                    projection[f_id] = 1
            # For initial iteration calculate the total number of matching docs.
            logger.info("Inside filter setup")
            if skip == 0:
                output["totalMatchCount"] = (
                    self.db[f"{grid_collection_name}"]
                    .find(filter_dict, projection)
                    .count()
                )
                logger.info(f"totalMatchCount: {output['totalMatchCount']}")
            if skip == 0:
                # When skip == 0, we need to perform the query only when the totalMatchCount is > 0.
                if output["totalMatchCount"]:
                    db_data = (
                        self.db[f"{grid_collection_name}"]
                        .find(filter_dict, projection)
                        .sort(sort_tuple_list)
                        .limit(limit)
                        .skip(skip)
                    )
            else:
                db_data = (
                    self.db[f"{grid_collection_name}"]
                    .find(filter_dict, projection)
                    .sort(sort_tuple_list)
                    .limit(limit)
                    .skip(skip)
                )

        if db_data:
            for d in db_data:
                result.append(d)
        output["result"] = result
        logger.info(f"Grid Data: Returning {len(output['result'])} matched items.")
        return output

    def retrieve_grid_data_from_field_values(
        self,
        workspace_id,
        field_bundle_id,
        file_ids=None,
        field_ids=None,
        limit=25,
        skip=0,
        sort_tuple_list=None,
        filter_dict=None,
        group_by_list=None,
        value_aggregate_list=None,
        review_status_filter_dict=None,
        distinct_field=None,
        return_only_file_ids=False,
        return_top_fact_answer=False,
    ):
        """
        workspace_id (Mandatory): Determines the workspace for which the field bundle data needs to be retrieved.
        field_bundle_id (Mandatory): Determines the field bundle id for which the grid data needs to be retrieved.
        file_ids (Optional): When provided determines the field values corresponding to which files has to be retrieved.
        field_ids (Optional): When provided the retrieved data will comprise of only those fields
        limit (Optional): Limit the number of documents returned. To be used in pagination. Default to 25
        skip (Optional): Skip the initial number of documents. To be used in pagination. Default to 0
        sort_tuple_list (Optional): By default the returned documents will be sorted by name,
            specify any additional sort that is required.
            e.g.
                sort_tuple_list = [
                    ("cb870a04.answer_details.raw_value", 1)
                ]
        filter_dict (Optional): Filter query to be used.
            Filter and group_by doesn't co-occur on the same action in grid data retrieval.
            e.g.
                filter_dict = {
                    "a41ecdfa.answer_details.raw_value": {"$gt":575000000, "$lt": 2500000000}
                }
        group_by_list (Optional): List of row_groups to be created.
            e.g.
                group_by_list = [
                    ("d3c9657d", "", 0), <-- Default group type is boolean
                    ("fde9d7ce", "number", 10),  <-- Default numBins for numeric field is 10
                    ("28aeb6c1", "", 0)
                ]  # List of columns on which grouping needs
                to be applied
        value_aggregate_list (Optional): An optional list of aggregation functions to be applied on columns.
        TODO: Need to add code when the fields are not present. Results are wrong because of the literal in
        aggregate projection.
                        {
                  "result": [
                    {
                      "_id": "",
                      "child_total": 1,
                      "docs": [
                        {
                          "_id": "",
                          "child_total": 1,
                          "id": "9844ba0b"
                        }
                      ],
                      "id": "28aeb6c1"
                    }
                  ]
                }
        review_status_filter_dict (Optional): review filter dictionary that needs to be applied along with filter_dict
        distinct_field (Optional): Field Id for which we have to retrieve the distinct values.
        return_only_file_ids (Optional): Specifies whether to return only file_ids.
        return_top_fact_answer(Optional): Specifies whether we need to return only the top_fact answer details.
        """
        if not (workspace_id and field_bundle_id):
            logger.info(
                "One or Both of the mandatory fields (Workspace ID or Field Bundle Id) is missing",
            )
            raise ValueError(
                "must specify 'workspace_id' and 'field_bundle_id' when retrieving extracted fields",
            )
        logger.info(
            f"Extracting Grid data for workspace {workspace_id} "
            f"and field bundle {field_bundle_id}",
        )
        output = {}
        pipeline = []
        do_distinct_calc = False

        fixed_match_query = {
            "workspace_idx": f"{workspace_id}",
            "field_bundle_idx": f"{field_bundle_id}",
        }
        if field_ids:
            fixed_match_query["field_idx"] = {
                "$in": field_ids,
            }
        if filter_dict:
            field_ids_list = []
            for item in filter_dict.keys():
                field_ids_list.append(item.split(".answer_details")[0])
            if field_ids_list:
                if not fixed_match_query.get("field_idx", {}):
                    fixed_match_query["field_idx"] = {
                        "$in": field_ids_list,
                    }
                else:
                    existing_fields = fixed_match_query["field_idx"]["$in"]
                    for f in field_ids_list:
                        if f not in existing_fields:
                            existing_fields.append(f)
                    fixed_match_query["field_idx"] = {
                        "$in": existing_fields,
                    }

        if file_ids:
            fixed_match_query["file_idx"] = {"$in": file_ids}

        fixed_pipeline_init_proj = {
            "file_idx": 1,
            "file_name": 1,
            "field_idx": 1,
        }
        if return_top_fact_answer:
            fixed_pipeline_init_proj["top_fact.answer"] = 1
            fixed_pipeline_init_proj["top_fact.formatted_answer"] = 1
            fixed_pipeline_init_proj["top_fact.answer_details"] = 1
        else:
            fixed_pipeline_init_proj["top_fact"] = 1

        fixed_pipeline = [
            # Pipeline for Grid style output except for file_name
            {
                "$match": fixed_match_query,
            },
            {
                "$sort": {
                    "file_name": 1,
                },
            },
            {
                "$project": fixed_pipeline_init_proj,
            },
            {
                "$group": {
                    "_id": "$file_idx",
                    "file_name": {"$first": "$file_name"},
                    "cols": {
                        "$push": {
                            "k": "$field_idx",
                            "v": "$top_fact",
                        },
                    },
                },
            },
            {
                "$addFields": {
                    "cols": {
                        "$arrayToObject": "$cols",
                    },
                    "file_idx": "$_id",
                },
            },
            {
                "$replaceRoot": {
                    "newRoot": {
                        "$mergeObjects": [
                            "$cols",
                            "$$ROOT",
                        ],
                    },
                },
            },
            {
                "$project": {
                    "_id": 0,
                    "cols": 0,  # Discard the cols array created using group by.
                },
            },
        ]
        # Add fixed pipeline to pipeline list.
        pipeline.extend(fixed_pipeline)

        variable_pipeline = []
        # Sort Dictionary
        sort_tuple_list = sort_tuple_list or []

        if group_by_list:
            grp_len = len(group_by_list)
            # consider only filter_dict, group_by_list and value_aggregate_list
            if filter_dict:
                variable_pipeline.append(
                    {
                        "$match": filter_dict,
                    },
                )
            if review_status_filter_dict:
                variable_pipeline.append(
                    {
                        "$match": review_status_filter_dict,
                    },
                )
            init_pipeline_len = len(variable_pipeline)
            # GroupBy List
            for idx, g in enumerate(group_by_list[::-1]):
                if idx == 0:
                    grp_pipeline = {}
                    # Check for the type in group_by_list. Default is boolean / ""
                    if not g[1] or g[1] == "boolean":
                        grp_pipeline = {
                            "$group": {
                                "_id": {
                                    g[0]: f"${g[0]}.answer_details.raw_value"
                                    for g in group_by_list
                                }
                                if grp_len - idx > 1
                                else f"${g[0]}.answer_details.raw_value",
                                "child_total": {"$sum": 1},
                            },
                        }
                    elif g[1] == "number":
                        grp_pipeline = {
                            "$bucketAuto": {
                                "groupBy": f"${g[0]}.answer_details.raw_value",
                                "buckets": g[2] if g[2] else 10,
                                "output": {
                                    "child_total": {
                                        "$sum": 1,
                                    },
                                },
                            },
                        }
                    if grp_pipeline and "$group" in grp_pipeline:
                        for (field, agg_val) in value_aggregate_list:
                            grp_pipeline["$group"][field] = agg_val
                else:
                    # Update 11/21/2022 - Don't think the below piece of code is invoked.
                    # TODO: Value aggregation on deep nested grouping.
                    grp_pipeline = {
                        "$group": {
                            "_id": {
                                g[0]: f"$_id.{g[0]}"
                                for g in group_by_list[: grp_len - idx]
                            }
                            if grp_len - idx > 1
                            else f"$_id.{g[0]}",
                            "docs": {
                                "$push": {
                                    "_id": f"$_id.{group_by_list[grp_len - idx - 1][0]}",
                                    "child_total": "$child_total",
                                    "docs": "$docs",
                                },
                            },
                            "child_total": {"$sum": "$child_total"},
                        },
                    }
                if grp_pipeline:
                    variable_pipeline.append(grp_pipeline)

            projection = {}
            projection_head = None
            grp_pipeline_len = len(variable_pipeline) - init_pipeline_len
            for i in range(grp_pipeline_len):
                if i == 0:
                    projection = {
                        "$project": {
                            "_id": 1,
                            "id": {"$literal": group_by_list[i][0]},  # Field Id
                            "child_total": 1,
                        },
                    }
                    for (field, _agg_val) in value_aggregate_list:
                        projection["$project"][field] = 1
                    if i != grp_pipeline_len - 1:
                        projection["$project"]["docs"] = {}
                        projection_head = projection["$project"]
                else:
                    projection_head["docs"] = {
                        "_id": 1,
                        "id": {"$literal": group_by_list[i][0]},
                        "child_total": 1,
                    }
                    for (field, _agg_val) in value_aggregate_list:
                        projection_head["docs"][field] = 1
                    if i == grp_pipeline_len - 1:
                        projection_head["docs"]["docs"] = 1
                    else:
                        projection_head["docs"]["docs"] = {}
                        projection_head = projection_head["docs"]

            variable_pipeline.append(projection)
        else:
            # Add Filter dictionary as a match item
            filter_dict = filter_dict or {}
            if file_ids:
                filter_dict["file_idx"] = {"$in": file_ids}
            if filter_dict:
                variable_pipeline.append(
                    {
                        "$match": filter_dict,
                    },
                )
            if review_status_filter_dict:
                variable_pipeline.append(
                    {
                        "$match": review_status_filter_dict,
                    },
                )
            # Return all the attributes of the documents if projections (fieldIds) are not specified.
            # Commenting out below code for performance.
            # if field_ids:
            #     field_id_projection = {
            #         "file_idx": 1,
            #         "file_name": 1,
            #     }
            #     for f_id in field_ids:
            #         field_id_projection[f_id] = 1
            #     variable_pipeline.append(
            #         {
            #             "$project": field_id_projection,
            #         },
            #     )
            if distinct_field:
                do_distinct_calc = True
            else:
                # Perform sorting.
                if "file_name" not in next(iter(list(zip(*sort_tuple_list))), []):
                    sort_tuple_list.append(("file_name", 1))
                variable_pipeline.append(
                    {
                        "$sort": {k: v for (k, v) in sort_tuple_list},
                    },
                )

        if do_distinct_calc:
            last_pipeline = [
                {
                    "$group": {
                        "_id": f"${distinct_field}.answer_details.raw_value",
                    },
                },
            ]
        else:
            temp_last_pipeline = []
            if not group_by_list:
                temp_last_pipeline.append(
                    {
                        "$skip": skip,
                    },
                )
                temp_last_pipeline.append(
                    {
                        "$limit": limit,
                    },
                )
            temp_last_pipeline.append(
                {
                    "$group": {
                        "_id": None,
                        "totalMatchCount": {"$sum": 1},
                        "results": {
                            "$push": "$$ROOT"
                            if not return_only_file_ids
                            else "$file_idx",
                        },
                    },
                },
            )
            if group_by_list:
                temp_last_pipeline.append(
                    {
                        "$project": {
                            "_id": 0,
                        },
                    },
                )
                last_pipeline = temp_last_pipeline
            else:
                temp_last_pipeline.append(
                    {
                        "$project": {
                            "_id": 0,
                            "results": 1,
                        },
                    },
                )
                last_pipeline = [
                    {
                        "$facet": {
                            "totalMatchCount": [
                                {"$count": "totalMatchCount"},
                            ],
                            "results": temp_last_pipeline,
                        },
                    },
                    {
                        "$addFields": {
                            "totalMatchCount": {
                                "$ifNull": [
                                    {
                                        "$arrayElemAt": [
                                            "$totalMatchCount.totalMatchCount",
                                            0,
                                        ],
                                    },
                                    0,
                                ],
                            },
                            "results": {
                                "$ifNull": [
                                    {"$arrayElemAt": ["$results.results", 0]},
                                    [],
                                ],
                            },
                        },
                    },
                ]
        variable_pipeline.extend(last_pipeline)
        # Add variable pipeline to pipeline list
        pipeline.extend(variable_pipeline)
        # Execute the aggregation pipeline.
        db_data = self.db["field_value"].aggregate(pipeline, allowDiskUse=True)
        if db_data and do_distinct_calc:
            output = [self.unescape_mongo_data(d1["_id"]) for d1 in db_data]
            return output
        if db_data:
            for d in db_data:
                output = self.unescape_mongo_data(d)
                break
        if output and group_by_list:
            new_output = {
                "totalMatchCount": output["totalMatchCount"],
                "results": [],
            }
            for item in output["results"]:
                result_item = {
                    item["id"]: {
                        "answer_details": {
                            "formatted_value": item["_id"],
                            "raw_value": item["_id"],
                        },
                    },
                    "child_total": item["child_total"],
                }
                for (field, agg_val) in value_aggregate_list:
                    result_item[field] = {
                        "answer_details": {
                            "formatted_value": item[field],
                            "raw_value": item[field],
                        },
                    }
                new_output["results"].append(result_item)
            output = new_output
        if not output:
            output = {
                "totalMatchCount": 0,
                "results": [],
            }
        logger.info(
            f"Grid Data: Returning Grid matched items. {len(output.get('results', []))}",
        )

        return output

    def download_grid_data_from_field_values(
        self,
        workspace_id,
        field_bundle_id,
        file_idx=None,
        field_ids=None,
        include_file_idx=False,
    ):
        """
        Retrieve all of the grid data to facilitate download.
        :param workspace_id:
        :param field_bundle_id:
        :param file_idx:
        :param field_ids:
        :param include_file_idx:
        :return:
        """
        if not (workspace_id and field_bundle_id):
            logger.info(
                "One or Both of the mandatory fields (Workspace ID or Field Bundle Id) is missing while downloading",
            )
            raise ValueError(
                "must specify 'workspace_id' and 'field_bundle_id' when downloading extracted fields",
            )
        logger.info(
            f"Downloading Grid data for workspace {workspace_id} "
            f"and field bundle {field_bundle_id}",
        )
        output = []
        pipeline = []
        fixed_match_query = {
            "workspace_idx": f"{workspace_id}",
            "field_bundle_idx": f"{field_bundle_id}",
        }
        if field_ids:
            fixed_match_query["field_idx"] = {
                "$in": field_ids,
            }
        if file_idx:
            fixed_match_query["file_idx"] = file_idx

        final_projection = {
            "_id": 0,
            "cols": 0,  # Discard the cols array created using group by.
        }
        if not include_file_idx:
            final_projection["file_idx"] = 0

        fixed_pipeline = [
            # Pipeline for Grid style output except for file_name
            {
                "$match": fixed_match_query,
            },
            {
                "$sort": {
                    "file_name": 1,
                },
            },
            {
                "$project": {
                    "file_idx": 1,
                    "file_name": 1,
                    "field_idx": 1,
                    "top_fact.answer_details.raw_value": 1,
                    "top_fact.matches.answer_details.raw_value": 1,
                },
            },
            {
                "$group": {
                    "_id": "$file_idx",
                    "file_name": {"$first": "$file_name"},
                    "cols": {
                        "$push": {
                            "k": "$field_idx",
                            "v": {
                                "$cond": {
                                    "if": {
                                        "$eq": [
                                            {
                                                "$ifNull": [
                                                    "$top_fact.answer_details.raw_value",
                                                    "",
                                                ],
                                            },
                                            "",
                                        ],
                                    },
                                    "then": {
                                        "$ifNull": [
                                            {
                                                "$reduce": {
                                                    "input": "$top_fact.matches.answer_details",
                                                    "initialValue": None,
                                                    "in": {
                                                        "$cond": [
                                                            "$$value",
                                                            {
                                                                "$concat": [
                                                                    "$$value",
                                                                    "\n",
                                                                    {
                                                                        "$toString": "$$this.raw_value",
                                                                    },
                                                                ],
                                                            },
                                                            {
                                                                "$toString": "$$this.raw_value",
                                                            },
                                                        ],
                                                    },
                                                },
                                            },
                                            None,
                                        ],
                                    },
                                    "else": "$top_fact.answer_details.raw_value",
                                },
                            },
                        },
                    },
                },
            },
            {
                "$addFields": {
                    "cols": {
                        "$arrayToObject": "$cols",
                    },
                    "file_idx": "$_id",
                },
            },
            {
                "$replaceRoot": {
                    "newRoot": {
                        "$mergeObjects": [
                            "$cols",
                            "$$ROOT",
                        ],
                    },
                },
            },
            {
                "$project": final_projection,
            },
        ]
        # Add fixed pipeline to pipeline list.
        pipeline.extend(fixed_pipeline)
        db_data = self.db["field_value"].aggregate(pipeline)
        if db_data:
            for d in db_data:
                output.append(self.unescape_mongo_data(d))
        return output

    def build_field_value_stats(
        self,
        workspace_id,
        field_bundle_id,
        field_ids,
    ):
        """
        Create the field value statistics table
        :param workspace_id: Workspace ID applicable for the construction of statistics table.
        :param field_bundle_id: Field Bundle Id applicable for the construction of statistics table.
        :param field_ids: Non entered fields applicable for the construction of statistics table.
        :return: Returns the statistics table in the following format.
            {
                "rowStats": [...],
                "colStats": [...],
                "totalFiles": 23,
                "totalEdits": 9,
                "totalApprovals": 34,
                "totalFields": 874,
                "nFieldsPerDocument": 38
            }
        """
        if not (workspace_id and field_bundle_id and field_ids):
            logger.info(
                "One of the mandatory fields (Workspace ID or Field Bundle Id or Field Ids) is missing",
            )
            raise ValueError(
                "must specify 'workspace_id' and 'field_bundle_id' and 'field_ids' when constructing stats table",
            )
        logger.info(
            f"Constructing stats table for workspace {workspace_id} "
            f"and field bundle {field_bundle_id}",
        )
        pipeline = [
            {
                "$match": {
                    "workspace_idx": workspace_id,
                    "field_bundle_idx": field_bundle_id,
                    "field_idx": {"$in": field_ids},
                },
            },
            {"$sort": {"file_name": 1}},
            # Retrieve only what is needed.
            {
                "$project": {
                    "file_idx": 1,
                    "file_name": 1,
                    "field_idx": 1,
                    "top_fact.is_override": 1,
                    "top_fact.type": 1,
                },
            },
            {
                "$facet": {
                    "rowStats": [
                        {
                            "$group": {
                                "_id": "$file_idx",
                                "fileName": {"$first": "$file_name"},
                                "nEdits": {
                                    "$sum": {
                                        "$cond": [
                                            {"$eq": [True, "$top_fact.is_override"]},
                                            1,
                                            0,
                                        ],
                                    },
                                },
                                "nApprovals": {
                                    "$sum": {
                                        "$cond": [
                                            {"$eq": ["approve", "$top_fact.type"]},
                                            1,
                                            0,
                                        ],
                                    },
                                },
                            },
                        },
                    ],
                    "colStats": [
                        {
                            "$group": {
                                "_id": "$field_idx",
                                "nEdits": {
                                    "$sum": {
                                        "$cond": [
                                            {"$eq": [True, "$top_fact.is_override"]},
                                            1,
                                            0,
                                        ],
                                    },
                                },
                                "nApprovals": {
                                    "$sum": {
                                        "$cond": [
                                            {"$eq": ["approve", "$top_fact.type"]},
                                            1,
                                            0,
                                        ],
                                    },
                                },
                            },
                        },
                    ],
                },
            },
            {
                "$project": {
                    "totalEdits": {"$sum": "$rowStats.nEdits"},
                    "totalApprovals": {"$sum": "$rowStats.nApprovals"},
                    "totalFiles": {"$size": "$rowStats"},
                    "totalFields": {
                        "$multiply": [{"$size": "$rowStats"}, {"$size": "$colStats"}],
                    },
                    "rowStats": 1,
                    "colStats": 1,
                },
            },
        ]

        db_data = self.db["field_value"].aggregate(pipeline)
        output = {}
        if db_data:
            for d in db_data:
                output = d
                break
        return output

    def upsert_usage_metrics(self, user_id, usage_data, upsert=False):
        """
        Creates / Updates usage metrics for a user for specific month.
        :param user_id: User ID for which the usage statistics will be created
        :param usage_data: Usage Data.
                {
                    "general_usage": {
                        "no_of_pages": 20,
                    },
                    "dev_api_usage": {
                        "pdf_parser": {
                            "no_of_pages": 10,
                        }
                    }
                }
        :param upsert: Do we need to upsert ? First time creation.
        :return: Void
        """
        if not user_id:
            raise ValueError(
                "must specify 'user_id' while creating usage metrics",
            )
        # Store the date as YYYY-MM
        date = datetime.datetime.now().strftime(DATE_TIME_YEAR_MONTH)
        set_data = {
            "user_id": user_id,
            "reported_on": date,
        }
        flatten_data = flatten_dict(usage_data)
        inc_data = {}
        for k, v in flatten_data.items():
            if isinstance(v, str):
                set_data[k] = v
            else:
                inc_data[k] = v
        update_res = self.db["usage"].update_one(
            {"user_id": user_id, "reported_on": date},
            {"$set": set_data, "$inc": inc_data},
            upsert=upsert,
        )
        if not upsert and update_res.modified_count == 0:
            # Find the latest metric to copy over
            latest_metric = self.retrieve_latest_usage(user_id)
            if latest_metric["reported_on"] != date:
                latest_metric["reported_on"] = date
                catalogs = self.retrieve_catalogs()
                for key in ["general_usage", "dev_api_usage"]:
                    for catalog_key in latest_metric.get(key, {key: {}}):
                        if catalog_key in catalogs:
                            if (
                                catalogs[catalog_key]["renewable"][key]
                                and not PAYMENT_CONTROLLED_RENEWABLE_RESOURCES
                            ):
                                latest_metric[key][catalog_key] = usage_data.get(
                                    key,
                                    {catalog_key: 0},
                                ).get(catalog_key, 0)
                            else:
                                latest_metric[key][catalog_key] += usage_data.get(
                                    key,
                                    {catalog_key: 0},
                                ).get(catalog_key, 0)
                # self.db["usage"].insert_one(latest_metric)
                self.db["usage"].update_one(
                    {"user_id": user_id, "reported_on": date},
                    {"$setOnInsert": latest_metric},
                    upsert=True,
                )

    def reset_renewable_resources(self, user_id, subs_name):
        """
        Resets the renewable resources for the user (possibly upon payment).
        :param user_id: User ID for which the usage statistics will be created
        :param subs_name: Subscription name associated with the user
        :return: Void
        """
        if not user_id:
            raise ValueError(
                "must specify 'user_id' while resetting usage metrics",
            )
        # Store the date as YYYY-MM
        date = datetime.datetime.now().strftime(DATE_TIME_YEAR_MONTH)
        # Find the latest metric to copy over
        latest_metric = self.retrieve_latest_usage(user_id)

        if latest_metric["reported_on"] != date:
            latest_metric["reported_on"] = date
        catalogs = self.retrieve_catalogs()
        plans_data = self.retrieve_subscription_plans(subs_name=subs_name)
        if plans_data and plans_data.get(subs_name, {}):
            plan_data = plans_data[subs_name]
            for key in plan_data["quota_limits"]:
                for catalog_key in latest_metric.get(key, {key: {}}):
                    if (
                        catalog_key in catalogs
                        and catalogs[catalog_key]["renewable"][key]
                    ):
                        latest_metric[key][catalog_key] = 0
            logger.info(
                f"Resetting renewable resources for {user_id} ... {latest_metric}",
            )
            self.db["usage"].update_one(
                {"user_id": user_id, "reported_on": date},
                {"$setOnInsert": latest_metric},
                upsert=True,
            )

    def retrieve_latest_usage(self, user_id):
        """
        Retrieve the latest usage metric for the user.
        :param user_id: User ID.
        :return: Latest usage metric: dict
        """
        # Find the latest usage metrics data.
        find_res = (
            self.db["usage"]
            .find(
                {"user_id": user_id},
                {"_id": 0},
            )
            .sort("reported_on", -1)
        )

        latest_metric: dict = {}
        for res in find_res:
            latest_metric = res
            break
        return latest_metric

    def retrieve_usage_metrics(self, user_id, year=None, month=None):
        """
        Retrieve usage metrics for the given user_id, year and/or month combination.
        :param user_id: User ID
        :param year: Year for which we need the metrics
        :param month: Month for which we need the metrics
        :return: List of metric data
        """
        if not user_id:
            raise ValueError(
                "must specify 'user_id' while querying usage metrics",
            )

        query = {
            "user_id": user_id,
        }
        if year and month:
            query["reported_on"] = {"$eq": f"{year}-{month}"}
        elif year:
            query["reported_on"] = {"$gt": f"{year}"}
        elif month:
            year = datetime.datetime.now().strftime("%Y")
            query["reported_on"] = {"$eq": f"{year}-{month}"}
        else:
            date = datetime.datetime.now().strftime(DATE_TIME_YEAR_MONTH)
            query["reported_on"] = {"$eq": f"{date}"}

        metrics = self.db["usage"].find(
            query,
            {"_id": 0},  # exclude the return of _id
        )
        metric_list = [metric for metric in metrics]
        if not metric_list:
            # Find the latest metric to copy over
            latest_metric = self.retrieve_latest_usage(user_id)
            latest_metric["reported_on"] = datetime.datetime.now().strftime(
                DATE_TIME_YEAR_MONTH,
            )
            catalogs = self.retrieve_catalogs()
            for key in ["general_usage", "dev_api_usage"]:
                for catalog_key in latest_metric.get(key, {key: {}}):
                    if (
                        catalog_key in catalogs
                        and catalogs[catalog_key]["renewable"][key]
                        and not PAYMENT_CONTROLLED_RENEWABLE_RESOURCES
                    ):
                        latest_metric[key][catalog_key] = 0
            self.db["usage"].update_one(
                {"user_id": user_id, "reported_on": latest_metric["reported_on"]},
                {"$setOnInsert": latest_metric},
                upsert=True,
            )
            metric_list = [latest_metric]
        return metric_list

    def retrieve_subscription_plans(self, subs_name=None):
        """
        Returns the dictionary of subscription plans allowed.
        :param subs_name: Subscription plan to retrieve
        :return: Dictionary of subscription plans with subscription name as the Key
        """
        query = {}
        if subs_name:
            query["subs_name"] = subs_name
        plans = self.db["nlm_subscriptions"].find(
            query,
            {"_id": 0},  # exclude the return of _id
        )
        plan_dict = {}
        if plans:
            for plan in plans:
                first_key, *rest_keys = plan
                plan_dict[plan[first_key]] = {key: plan[key] for key in rest_keys}
        return plan_dict

    def get_nlm_settings(self, key):
        """
        Returns the value from nlm_settings for the key
        :param key: Key for which the value has to be retrieved.
        :return: Value
        """
        ret_value = None
        setting = self.db["nlm_settings"].find_one(
            {"id": key},
            {"_id": 0},  # exclude the return of _id
        )
        if setting:
            ret_value = setting["value"]
        return ret_value

    def update_nlm_settings(self, key, value):
        """
        Updates the value corresponding to the key in nlm_settings
        :param key: Key for which the value has to be updated.
        :param value: Value to be updated.
        """
        set_data = {
            "id": key,
            "value": value,
        }
        self.db["nlm_settings"].update_one(
            {"id": key},
            {"$set": set_data},
            upsert=True,
        )

    def retrieve_catalogs(self, catalog_id=None):
        """
        Returns the dictionary of Catalogs or catalog data for the given catalog_id.
        :param catalog_id: Catalog for the id to be retrieved.
        :return: Dictionary of Catalogs
        """
        query = {}
        if catalog_id:
            query["id"] = catalog_id
        catalogs = self.db["nlm_catalog"].find(
            query,
            {"_id": 0},  # exclude the return of _id
        )
        catalog_dict = {}
        if catalogs:
            for catalog in catalogs:
                first_key, *rest_keys = catalog
                catalog_dict[catalog[first_key]] = {
                    key: catalog[key] for key in rest_keys
                }
        return catalog_dict

    # APIs for Notifications
    def create_notification(
        self,
        user_id,
        notify_action,
        notify_params,
        send_email=True,
    ):
        """
        Create notifications for the user with the string message as specified
        :param user_id: User for which the notification has to be generated
        :param notify_action: String message
        :param notify_params: Any Notification specific params
        :param send_email: Send EMAIL Notification
        :return:
        """
        notification = {
            "user_id": user_id,
            "is_read": False,
            "created_on": datetime.datetime.now(t_zone).strftime(T_ZONE_FORMAT),
            "notify_action": notify_action,
            "notify_params": notify_params,
            "send_email": send_email,
        }
        query = {
            "user_id": user_id,
            "is_read": False,
            "notify_action": notify_action,
        }
        notify_dict = self.db["notifications"].find_one(query)
        if not notify_dict:
            # There are no unread notifications,
            self.db["notifications"].insert_one(notification)
            return Notifications(**notification)
        else:
            # There is still an unread notification of the same ACTION.
            # Append to the list
            notify_dict["send_email"] = notify_dict["send_email"] or send_email
            # If there are multiple keys in there.
            for k, v in notify_params.items():
                id_list = [element["id"] for element in notify_dict["notify_params"][k]]
                if (
                    notify_action in WS_SPECIFIC_NOTIFY_ACTIONS
                    and v[0]["id"] not in id_list
                ):
                    # Check only the workspace id
                    notify_dict["notify_params"][k].append(
                        v[0],
                    )  # v is a list structure.
                elif notify_action not in WS_SPECIFIC_NOTIFY_ACTIONS:
                    if v[0]["id"] in id_list:
                        # Workspace in the list of notify actions
                        list_index = id_list.index(
                            v[0]["id"],
                        )  # Matching index for the Workspace ID
                        for ws_key, ws_value in v[0].items():
                            if isinstance(ws_value, list):
                                sub_field_id_list = [
                                    x["id"]
                                    for x in notify_dict["notify_params"][k][
                                        list_index
                                    ][ws_key]
                                ]
                                # Append to the list if not a duplicate
                                if ws_value[0]["id"] not in sub_field_id_list:
                                    notify_dict["notify_params"][k][list_index][
                                        ws_key
                                    ].append(
                                        ws_value[0],
                                    )
                    else:
                        # Not there in the list. Just append the value
                        notify_dict["notify_params"][k].append(v[0])
            self.db["notifications"].replace_one(
                {
                    "_id": notify_dict["_id"],
                },
                notify_dict,
            )
            return Notifications(**notification)

    def update_unread_notifications(self, id_list: list):
        """
        Update the unread notifications to read
        :param id_list: List of notification ids
        :return: True
        """
        id_list = [ObjectId(idx) for idx in id_list]
        self.db["notifications"].update_many(
            {"_id": {"$in": id_list}},
            {"$set": {"is_read": True}},
        )
        logger.info(f"is_read updated to True for {id_list}")
        return True

    def get_notifications(self, user_id, is_read: bool = False):
        """
        Retrieve the notifications for the mentioned user_id,
        :param user_id: User ID
        :param is_read: Return should include already read notifications.
        :return: List of notifications.
        """
        query = {
            "user_id": user_id,
        }
        if not is_read:
            query["is_read"] = is_read
        notify_stream = self.db["notifications"].find(
            query,
        )
        notifications = []
        for n in notify_stream:
            n["id"] = str(n["_id"])
            del n["_id"]
            notifications.append(Notifications(**n))
        if notifications:
            notifications.sort(key=lambda x: x.created_on, reverse=True)
        return notifications

    def create_field_bundle_grid(self, workspace_id, field_bundle_id):
        collection_name = f"field_bundle_grid_{workspace_id}_{field_bundle_id}"
        try:
            self.db.create_collection(collection_name)
            logger.info(f"creating field_bundle_grid {collection_name}")
            return True
        except CollectionInvalid:
            # collection exists
            return False

    def insert_field_bundle_grid_rows(self, workspace_id, field_bundle_id, rows):
        collection_name = f"field_bundle_grid_{workspace_id}_{field_bundle_id}"
        logger.info(f"inserting {len(rows)} rows into {collection_name}")
        self.db[collection_name].insert_many(rows)

    def get_field_bundle_grid_rows(self, workspace_id, field_bundle_id):
        collection_name = f"field_bundle_grid_{workspace_id}_{field_bundle_id}"
        rows = []
        for row in self.db[collection_name].find({}, {"_id": 0}):
            rows.append(row)
        return rows

    def add_subscription_session(self, user_id, session_id, price_id):
        self.db["user"].update_one(
            {"id": user_id},
            {
                "$push": {
                    "subscription_sessions": {
                        "session_id": session_id,
                        "status": "initiated",
                        "price_id": price_id,
                    },
                },
            },
        )

    def set_subscription_session_status(
        self,
        session_id,
        status,
        subscriptions=None,
        subscribed_workspaces=None,
        restricted_workspaces=None,
        subscription_plan=None,
    ):
        if subscriptions:
            set_data = {
                "subscription_sessions.$.status": status,
                "stripe_conf.subscriptions": subscriptions,
                "subscribed_workspaces": subscribed_workspaces,
                "restricted_workspaces": restricted_workspaces,
            }
            if subscription_plan:
                set_data["subscription_plan"] = subscription_plan
            logger.info(f"set_subscription_session_status: {set_data}")
            self.db["user"].update_one(
                {"subscription_sessions.session_id": session_id},
                {
                    "$set": set_data,
                },
            )
        else:
            self.db["user"].update_one(
                {"subscription_sessions.session_id": session_id},
                {"$set": {"subscription_sessions.$.status": status}},
            )

    def escape_mongo_data(self, data):
        if isinstance(data, dict):
            for key, value in data.items():
                data[key] = self.escape_mongo_data(value)
        elif isinstance(data, list):
            for idx, value in enumerate(data):
                data[idx] = self.escape_mongo_data(value)
        elif isinstance(data, str):
            data = data.replace(".", "_dot_").replace("$", "_dollar_")

        return data

    def unescape_mongo_data(self, data):
        if isinstance(data, dict):
            for key, value in data.items():
                data[key] = self.unescape_mongo_data(value)
        elif isinstance(data, list):
            for idx, value in enumerate(data):
                data[idx] = self.unescape_mongo_data(value)
        elif isinstance(data, str):
            data = data.replace("_dot_", ".").replace("_dollar_", "$")

        return data

    def save_inference_doc(self, doc_id, pages):
        self.db["ml_bbox"].delete_many({"file_idx": doc_id})
        self.db["ml_bbox"].insert_many(pages)
        # self.db["ml_bbox"].update_many({"file_idx": doc_id}, pages, upsert=True)

    def get_inference_bbox(self, doc_id, page_idx=-1):
        query = {
            "file_idx": doc_id,
        }
        if page_idx != -1:
            query["page_idx"] = int(page_idx)
        return [p for p in self.db["ml_bbox"].find(query, {"_id": 0})]

    def autocomplete_relation_node(self, workspace_id, field_id, search_text):
        if not workspace_id:
            raise AttributeError("Workspace ID not specified")
        return_data = self.db["saved_graph"].aggregate(
            [
                {
                    "$addFields": {
                        "result": {
                            "$filter": {
                                "input": "$graph_json.nodes.id",
                                "cond": {
                                    "$regexMatch": {
                                        "input": "$$this",
                                        "regex": search_text,
                                        "options": "i",
                                    },
                                },
                            },
                        },
                    },
                },
                {
                    "$match": {
                        "workspace_id": workspace_id,
                        "field_id": field_id,
                    },
                },
                {
                    "$project": {
                        "result": 1,
                    },
                },
            ],
        )
        ret_val = []
        for res in return_data:
            ret_val.extend(res["result"])
        return ret_val

    def get_yolo_samples(self, type="new"):

        # {
        # "7c120dcc": [
        #     {
        #         "page_idx": 8,
        #         "bbox": [[34,219,572,409]],
        #         "block_type": "table",
        #     },
        # ]
        # }
        samples = {}
        length = 0
        # gen_time = datetime.datetime(2021, 11, 6)
        # dummy_id = ObjectId.from_datetime(gen_time)
        files = self.db["bboxes"].distinct(
            "file_idx",
            {"audited": True, "block_type": "table", "split": type},
        )
        for file_idx in files:
            pages = self.db["bboxes"].distinct(
                "page_idx",
                {
                    "file_idx": file_idx,
                    "audited": True,
                    "block_type": "table",
                    "split": type,
                },
            )
            samples[file_idx] = pages
            length += len(pages)

        return samples, length

    def add_workspace_filter(self, workspace_filter):
        if not workspace_filter.id:
            raise AttributeError("field object not initialized")
        return self._create_entity(workspace_filter, "workspace_filter")

    def update_filter_by_id(self, filter_id, workspace_filter):
        return self._update_entity(filter_id, workspace_filter, "workspace_filter")

    def delete_workspace_filter(
        self,
        filter_id,
    ):
        self._delete_entity(filter_id, "workspace_filter")
        return filter_id

    def get_workspace_filter(
        self,
        user_id="",
        workspace_id="",
    ):
        query = {}
        if user_id:
            query["user_id"] = user_id
        if workspace_id:
            query["workspace_id"] = workspace_id

        return [
            WorkspaceFilter(**f)
            for f in self.db["workspace_filter"].find(query, {"_id": 0})
        ]

    def get_workspace_filter_by_id(
        self,
        workspace_filter_id,
    ):
        return self.db["workspace_filter"].find_one({"id": workspace_filter_id})

    def update_workspace_filters_in_bundle(self, workspace_filter_ids, field_bundle_id):
        self.db["field_bundle"].update_one(
            {"id": field_bundle_id},
            {"$set": {"workspace_filter_ids": workspace_filter_ids}},
        )

    def create_user_feedback(self, user_feedback: UserFeedback):
        time_now = datetime.datetime.now()
        set_data = {
            "user_id": user_feedback.user_id,
            "rating_stars": user_feedback.rating_stars,
            "feedback": user_feedback.feedback,
            "timestamp": time_now.strftime("%Y-%m-%d %H:%M:%S"),
        }
        self.db["user_feedback"].update_one(
            {"user_id": user_feedback.user_id},
            {"$set": set_data},
            upsert=True,
        )
        return int(time_now.timestamp())

    def get_application_settings(self, app_name: str):
        # Application settings is a list of settings specific to the application.
        # App Name should be the same as mentioned in the Auth0
        app_settings = self.get_nlm_settings("application_settings") or {}
        return app_settings.get(app_name, {})

    def create_wait_list_entry(self, wait_list: WaitList):
        time_now = datetime.datetime.now()
        set_data = {
            "user_id": wait_list.user_id,
            "app_name": wait_list.app_name,
            "wait_list_type": wait_list.wait_list_type,
            "send_notifications": False,
            "user_action_taken": False,
            "timestamp": time_now.strftime("%Y-%m-%d %H:%M:%S"),
        }
        self.db["wait_list"].update_one(
            {"user_id": wait_list.user_id},
            {"$set": set_data},
            upsert=True,
        )
        return int(time_now.timestamp())

    def add_to_prompt_library(self, prompt: Prompt):
        time_now = datetime.datetime.now()
        prompt.timestamp = (time_now.strftime("%Y-%m-%d %H:%M:%S"),)
        prompt.id = str_utils.get_unique_string(prompt.workspace_id)
        self.db["prompt_library"].insert_one(prompt.to_dict())
        return prompt.id

    def get_prompts(
        self,
        workspace_id,
        doc_id,
        prompt_type,
        query_scope=None,
        user_id=None,
    ):
        query = {
            "workspace_id": workspace_id,
            "doc_id": doc_id,
            "prompt_type": prompt_type,
        }
        if query_scope:
            query["query_scope"] = query_scope

        # We are dealing with PRIVATE prompt_type
        if user_id:
            query["user_id"] = user_id

        projection = {
            "_id": 0,
            "user_id": 0,
            "timestamp": 0,
        }

        prompt_stream = self.db["prompt_library"].find(query, projection)
        return [Prompt(**prompt) for prompt in prompt_stream]

    def add_to_search_criteria_workflow(self, sc_workflow: SearchCriteriaWorkflow):
        sc_workflow.timestamp = datetime.datetime.now()
        prefix_string = f"{sc_workflow.user_id}-{sc_workflow.workspace_id}"
        sc_workflow.id = str_utils.get_unique_string(prefix_string)
        self.db["search_criteria_workflow"].insert_one(sc_workflow.to_dict())
        return sc_workflow.id

    def get_search_criteria_workflows(
        self,
        user_id=None,
        workspace_id=None,
    ):
        if not user_id and not workspace_id:
            logger.info(
                "One of the mandatory fields (User ID or Workspace Id) is missing",
            )
            raise ValueError(
                "must specify 'user_id' or 'workspace_id' to retrieve search criteria workspaces",
            )
        query = {}
        if user_id:
            query["user_id"] = user_id
        if workspace_id:
            query["workspace_id"] = workspace_id

        projection = {
            "_id": 0,
        }

        sc_workflows_stream = self.db["search_criteria_workflow"].find(
            query,
            projection,
        )
        return [
            SearchCriteriaWorkflow(**sc_workflow) for sc_workflow in sc_workflows_stream
        ]

    def update_user_acl(self, user_acl: UserAccessControl):
        time_now = datetime.datetime.now()
        set_data = {
            "user_id": user_acl.user_id,
            "email_id": user_acl.email_id,
            "access_control_list": user_acl.access_control_list,
        }
        self.db["user_acl"].update_one(
            {
                "user_id": user_acl.user_id,
                "email_id": user_acl.email_id,
            },
            {
                "$set": set_data,
            },
            upsert=True,
        )
        return int(time_now.timestamp())

    def update_user_id_for_acl(self, user_id: str, email_id: str):
        if not (user_id and email_id):
            logger.info(
                "User ID and Email ID should be non-empty",
            )
            raise ValueError(
                "must specify 'user_id' and 'email_id'",
            )

        set_data = {
            "user_id": user_id,
        }
        self.db["user_acl"].update_one(
            {
                "email_id": email_id,
            },
            {
                "$set": set_data,
            },
            upsert=False,
        )
        return

    def get_user_acl(
        self,
        user_id=None,
        email_id=None,
    ):
        if not user_id and not email_id:
            logger.info(
                "User ID / Email ID should be non-empty for querying the acl",
            )
            raise ValueError(
                "must specify 'user_id' or 'email_id' to retrieve Access Control List",
            )

        query = {}
        if user_id:
            query["user_id"] = user_id
        else:
            query["email_id"] = email_id

        projection = {
            "_id": 0,
        }

        user_acl_stream = self.db["user_acl"].find(
            query,
            projection,
        )
        user_acl = None
        if user_acl_stream:
            users_acl = [UserAccessControl(**user_acl) for user_acl in user_acl_stream]
            if len(users_acl):
                user_acl = users_acl[0]
        return user_acl

    def save_anonymized_dict(self, uuid, a_dict):
        saved_graph = {
            "uuid": uuid,
            "a_dict": a_dict,
        }
        query = {
            "uuid": uuid,
        }

        return self.db["anonymized_dict"].update_one(
            query,
            {"$set": saved_graph},
            upsert=True,
        )

    def retrieve_anonymized_dict(self, uuid):
        query = {
            "uuid": uuid,
        }
        doc = self.db["anonymized_dict"].find_one(
            query,
            {"_id": 0},
        )
        ret_val = None
        if doc:
            ret_val = doc.get("a_dict", None)
        return ret_val

    def upsert_llmsherpa_parser_usage_metrics(
        self,
        num_pages=0,
        num_tokens=0,
        upsert=False,
    ):
        """
        Creates / Updates usage metrics for a user for specific month.
        :param num_pages: Number of parsed pages.
        :param num_tokens: Number of parsed tokens.
        :param upsert: Do we need to upsert ? First time creation.
        :return: Void
        """
        if not num_pages and not num_tokens:
            raise ValueError(
                "must specify 'num_pages' and/or 'num_pages' while creating parser usage metrics",
            )
        # Store the date as YYYY-MM
        date = datetime.datetime.now().strftime(DATE_TIME_YEAR_MONTH_DATE)
        set_data = {
            "reported_on": date,
        }
        inc_data = {
            "num_requests": 1,
            "num_pages": num_pages,
            "num_tokens": num_tokens,
        }
        self.db["parser_usage_metrics"].update_one(
            {"reported_on": date},
            {"$set": set_data, "$inc": inc_data},
            upsert=upsert,
        )
        return


def convert_sub_dict_to_sub_object(original_dict, key):
    mongo_dict = {}
    for dict_key in original_dict:
        mongo_dict[f"{key}.{dict_key}"] = original_dict[dict_key]
    return mongo_dict


def store_screen_shot(image_base64_string, dest_blob_name):
    screen_shot_path = f"search_screen_shot/{dest_blob_name}.png"
    # create tmp file
    tempfile_handler, tmp_file = tempfile.mkstemp()
    os.close(tempfile_handler)
    with open(tmp_file, "wb") as fp:
        fp.write(base64.b64decode(image_base64_string))
    # upload via minio
    file_storage.upload_blob(tmp_file, screen_shot_path, "image/png")
    return screen_shot_path


def flatten_dict(d, parent_key="", sep="."):
    """
    Flatten the nested dictionary for usage with metrics.
    All the keys will be joined together with the specified separator.
        {
            "general_usage": {
                "no_of_pages": 20,
            },
            "dev_api_usage": {
                "pdf_parser": {
                    "no_of_pages": 10,
                }
            }
        }  ===> {'general_usage.no_of_pages': 20, 'dev_api_usage.pdf_parser.no_of_pages': 10}
    :param d: Dictionary
    :param parent_key:
    :param sep: Delimiter separator
    :return: Dictionary with nested keys joined with separator
    """
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.abc.MutableMapping):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def correct_legacy_answers(top_fact):
    top_fact_answer = top_fact.get("answer", None)
    if top_fact_answer is None and "matches" not in top_fact:
        return top_fact
    # top_fact answer is not None.
    answer_details = {
        "raw_value": top_fact_answer,
        "formatted_value": top_fact_answer,
    }
    if not top_fact.get("answer_details", {}) and answer_details:
        top_fact["answer_details"] = answer_details
    # Check for matches
    if "matches" in top_fact:
        matches = top_fact.get("matches", [])
        new_matches = []
        for match in matches:
            match_answer = match.get("answer", None)
            if match_answer is None:
                new_matches.append(match)
                continue
            match_answer_details = {
                "raw_value": match_answer,
                "formatted_value": match_answer,
            }
            if not match.get("answer_details", {}) and match_answer_details:
                match["answer_details"] = match_answer_details
            new_matches.append(match)

        if new_matches:
            top_fact["matches"] = new_matches
    return top_fact


def create_workflow_field_value_params(value, user_name, edited_time):
    top_fact = {
        "answer": value,
        "formatted_answer": value,
        "answer_details": {
            "raw_value": value,
            "formatted_value": value,
        },
        "type": "override",
        "match_idx": "manual",
        "is_override": True,
    }
    history = {
        "username": user_name,
        "edited_time": edited_time,
        "previous": None,
        "modified": top_fact,
    }
    return history, top_fact
