import logging
import re

import server.config as cfg
from server.email.email_sender import create_and_send_email
from server.email.email_sender import create_and_send_workflow_email
from server.models import Notifications
from server.storage import nosql_db
from server.utils.notification_general import NotifyAction
from server.utils.notification_general import NotifyFreq
from server.utils.notification_general import WS_SPECIFIC_NOTIFY_ACTIONS

logger = logging.getLogger(__name__)
logger.setLevel(cfg.log_level())

EMAIL_REGEX = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b")
EMAIL_IDS_TO_DISCARD = [
    "*",
    "default@nlmatics.com",
    "admin@nlmatics.com",
    "info@nlmatics.com",
]


def retrieve_notifications(
    user_profile: dict,
    is_read: bool = False,
):
    """
    Retrieve notifications for the user
    :param user_profile: User Profile in dictionary
    :param is_read: Retrieve all or unread notifications
    :return: Notifications that matches the query filters
    """
    return nosql_db.get_notifications(
        user_profile["id"],
        is_read,
    )


def retrieve_new_notifications(
    user_profile: dict,
):
    """
    Retrieve notifications if user_profile has new notifications flag enabled.
    :param user_profile: User Profile in dictionary
    :return: New Notifications for the user.
    """
    if user_profile.get("has_notifications", False):
        return retrieve_notifications(user_profile)


def create_notification(
    user_profile,
    notify_action,
    notify_params,
):
    """
    Create Notification for the user and update their profile if there are no new notifications.
    :param user_profile: User profile.
    :param notify_action: String formatted notification
    :param notify_params: Notification specific params for taking any additional action
    :return: VOID
    """
    workspace_id = notify_params["workspaces"][0]["id"]
    workspace_notify = user_profile.get("workspace_notification_settings", {})
    send_email = True
    workspace_id_already_present = False
    for email_frequency, ws_list in workspace_notify:
        if workspace_id in ws_list:
            workspace_id_already_present = True
            if email_frequency == NotifyFreq.FREQ_INSTANT:
                send_email = False
            break
    if notify_action in WS_SPECIFIC_NOTIFY_ACTIONS:
        send_email = False
    # Create notification.
    notify_obj = nosql_db.create_notification(
        user_profile["id"],
        notify_action,
        notify_params,
        send_email,
    )
    if not send_email:
        # Invoke EMAIL API Client here.
        create_and_send_email(
            notify_obj,
            user_profile,
        )
    # Update the notification flag if not already set
    user_update_data = {}
    if not user_profile.get("has_notifications", False):
        user_update_data["has_notifications"] = True
    if (
        notify_action == NotifyAction.WORKSPACE_ACCESS_ADDED
        and not workspace_id_already_present
    ):
        # Update the user profile with workspace information
        # Add the workspace notification.
        if not workspace_notify:
            workspace_notify[NotifyFreq.FREQ_INSTANT] = []
        if workspace_id not in workspace_notify[NotifyFreq.FREQ_INSTANT]:
            workspace_notify[NotifyFreq.FREQ_INSTANT].append(workspace_id)

        user_update_data["workspace_notification_settings"] = workspace_notify
    elif (
        notify_action
        in [
            NotifyAction.WORKSPACE_DELETED,
            NotifyAction.WORKSPACE_ACCESS_REMOVED,
        ]
        and workspace_id_already_present
    ):
        # Update the user profile with workspace information
        # Add the workspace notification.
        if not workspace_notify:
            workspace_notify[NotifyFreq.FREQ_INSTANT] = []
        for email_frequency, ws_list in workspace_notify:
            if workspace_id in ws_list:
                workspace_notify[email_frequency] = [
                    i for i in ws_list if i != workspace_id
                ]
        user_update_data["workspace_notification_settings"] = workspace_notify

    if user_update_data:
        nosql_db.update_user(
            user_update_data,
            user_id=user_profile["id"],
        )


def send_bulk_notifications(
    dst_user_emails,
    notify_action,
    notify_params,
    subscribed_users=None,
    sender_email=None,
):
    """
    Send the notifications to a group of users.
    :param dst_user_emails: Collection of destination user emails
    :param notify_action: Notification string
    :param notify_params: Notification specific params.
    :param subscribed_users: Subscribed user list.
    :param sender_email: Sender email address.
    :return:
    """
    # send notification to all the users
    emails_to_discard = set(EMAIL_IDS_TO_DISCARD + [sender_email])
    for email in dst_user_emails:
        # Process only specific email.
        if email not in emails_to_discard:
            if EMAIL_REGEX.search(email):
                try:
                    dst_user_prof = nosql_db.get_user_by_email(
                        email,
                        expand=True,
                    ).to_dict()
                    create_notification(dst_user_prof, notify_action, notify_params)
                    emails_to_discard.add(email)
                except Exception as e:
                    logger.info(f"Error creating notification: {str(e)}")
            else:
                user_profiles = nosql_db.get_distinct_active_user_profiles(email)
                for profile in user_profiles:
                    # Send notification to users other than himself.
                    if profile["email_id"] not in emails_to_discard:
                        create_notification(profile, notify_action, notify_params)
                        emails_to_discard.add(profile["email_id"])
    # Send notification to subscribed_users
    subscribed_users = subscribed_users or []
    if subscribed_users:
        sub_user_profiles = nosql_db.get_users(user_list=subscribed_users, expand=True)
        emails_to_discard = emails_to_discard.union(set(dst_user_emails))
        for user_profile in sub_user_profiles:
            if user_profile["email_id"] not in emails_to_discard:
                create_notification(user_profile, notify_action, notify_params)


def send_workspace_update_notification(
    user_profile,
    workspace,
    orig_collaborators,
    new_collaborators,
):
    """
    Send workspace permission update notification.
    :param user_profile: Profile of the user sharing the workspace.
    :param workspace: Workspace details.
    :param orig_collaborators: Collaborators originally stored in DB.
    :param new_collaborators: New Collaborators.
    :return:
    """
    notify_params = {
        "workspaces": [
            {
                "id": workspace.id,
                "name": workspace.name,
                "from_user": {
                    "email_id": user_profile["email_id"],
                },
            },
        ],
    }
    send_bulk_notifications(
        set(orig_collaborators) - set(new_collaborators),
        NotifyAction.WORKSPACE_ACCESS_REMOVED,
        notify_params,
        sender_email=user_profile["email_id"],
    )

    send_bulk_notifications(
        set(new_collaborators) - set(orig_collaborators),
        NotifyAction.WORKSPACE_ACCESS_ADDED,
        notify_params,
        sender_email=user_profile["email_id"],
    )


def send_workspace_delete_notification(
    user_profile,
    workspace,
    collaborators,
):
    """
    Send Workspace delete notification to all the collaborators
    :param user_profile: Profile of the user deleting the workspace.
    :param workspace: Workspace details
    :param collaborators: List of collaborators to whom notifications should be sent.
    :return: VOID
    """
    notify_params = {
        "workspaces": [
            {
                "id": workspace.id,
                "name": workspace.name,
                "from_user": {
                    "email_id": user_profile["email_id"],
                },
            },
        ],
    }
    send_bulk_notifications(
        collaborators,
        NotifyAction.WORKSPACE_DELETED,
        notify_params,
        workspace.subscribed_users,
        sender_email=user_profile["email_id"],
    )


def send_document_notification(
    user_profile,
    workspace,
    document,
    action,
):
    """
    Send Document level notifications to the collaborators.
    :param user_profile: Profile of the user performing document action.
    :param workspace: Workspace for the document.
    :param document: Document under consideration
    :param action: Type of Action.
    :return:
    """
    notify_params = {
        "workspaces": [
            {
                "id": workspace.id,
                "name": workspace.name,
                "documents": [
                    {
                        "id": document.id,
                        "name": document.name,
                        "title": document.title,
                        "from_user": {
                            "email_id": user_profile["email_id"],
                        },
                    },
                ],
            },
        ],
    }
    send_bulk_notifications(
        workspace.collaborators,
        action,
        notify_params,
        workspace.subscribed_users,
        sender_email=user_profile["email_id"],
    )


def send_search_criteria_workflow_notification(
    workspace,
    document,
    sc_workflow,
    facts,
    filter_match=False,
):
    """
    Send Search Criteria Workflow notification to the subscribed user.
    :param workspace: Workspace where the criteria match was observed
    :param document: Document where the criteria match was observed.
    :param sc_workflow: Workflow details
    :param facts: Facts matching the search criteria
    :param filter_match: Notification due to filter match
    :return:
    """
    user_profile = nosql_db.get_user(sc_workflow.user_id).to_dict()
    notify_facts_list = []
    for fact in facts:
        for topic_fact in fact.get("topic_facts", []):
            if topic_fact.get("phrase", ""):
                notify_facts_list.append(topic_fact.get("phrase", ""))
    if notify_facts_list or filter_match:
        notify_params = {
            "workspaces": [
                {
                    "id": workspace.id,
                    "name": workspace.name,
                    "documents": [
                        {
                            "id": document.id,
                            "name": document.name,
                            "notify_facts": notify_facts_list
                            if notify_facts_list
                            else ["Matched Filters"],
                        },
                    ],
                },
            ],
        }
        notification = {
            "notify_action": NotifyAction.SEARCH_CRITERIA_WORKFLOW,
            "notify_params": notify_params,
        }
        notify_obj = Notifications(**notification)
        # Invoke EMAIL API Client here.
        create_and_send_workflow_email(
            notify_obj,
            user_profile,
        )
    else:
        logger.info(
            f"Nothing to send search notification for : {sc_workflow.user_id} -- {workspace.name}",
        )
