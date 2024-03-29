import datetime
import logging
import os

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Email
from sendgrid.helpers.mail import Mail
from sendgrid.helpers.mail import Personalization

import server.config as cfg
from server.storage import nosql_db
from server.utils.notification_general import DOC_SPECIFIC_NOTIFY_ACTIONS
from server.utils.notification_general import get_action_str_for_email
from server.utils.notification_general import NotifyAction
from server.utils.notification_general import WS_SPECIFIC_NOTIFY_ACTIONS

FROM_EMAIL_ADDRESS = "support@nlmatics.com"
WORKSPACE_TEMPLATE_ID = "d-f0003bed06b3463696a7d71ed97241fb"
DOCUMENT_TEMPLATE_ID = "d-126a95d65b7348179d34a716d268b5b5"
SEARCH_CRITERIA_WORKFLOW_TEMPLATE_ID = "d-f5061ed4bcc44ba7b96f41dfe621ebb3"

FRONTEND_URL = os.getenv("FRONTEND_URL")

sendgrid_client = SendGridAPIClient(os.environ.get("SENDGRID_API_KEY"))

logger = logging.getLogger(__name__)
logger.setLevel(cfg.log_level())


def get_action_specific_sendgrid_args(
    notify_action,
    workspace_data,
):
    """
    Get Action specific & Send grid specific args which will be used in the template
    :param notify_action: Action we are dealing with.
    :param workspace_data: List of workspace data from the notification
    :return: sendgrid args in dict
    """
    ret_dict = {
        "workspaces": [],
    }
    for ws_data in workspace_data:
        ws_dict = {
            "name": ws_data["name"],
        }
        # Add link details if we are dealing with ACCESS ADDED Action
        if notify_action not in [
            NotifyAction.WORKSPACE_ACCESS_REMOVED,
            NotifyAction.WORKSPACE_DELETED,
        ]:
            ws_dict["link"] = f"{FRONTEND_URL}/workspace/{ws_data['id']}/documents"
            # Add Document data
            ws_dict["documents"] = []
            for doc in ws_data.get("documents", []):
                doc_dict = {
                    "doc_name": doc["name"],
                }
                if notify_action not in [
                    NotifyAction.DOCUMENT_REMOVED_FROM_WORKSPACE,
                    NotifyAction.SEARCH_CRITERIA_WORKFLOW,
                ]:
                    doc_dict[
                        "doc_link"
                    ] = f"{FRONTEND_URL}/workspace/{ws_data['id']}/document/{doc['id']}/overview"
                if notify_action == NotifyAction.SEARCH_CRITERIA_WORKFLOW:
                    doc_dict["facts"] = doc["notify_facts"]
                ws_dict["documents"].append(doc_dict)
        ret_dict["workspaces"].append(ws_dict)

    return ret_dict


def send_email(
    to_address: str = "",
    template_idx: str = "",
    template_data=None,
):
    """
    Send email to the specified address using sendgrid API
    :param to_address:
    :param template_idx: Template ID
    :param template_data: Template Data as dict
    :return:
    """
    if to_address and template_idx and template_data:
        message = Mail(
            from_email=FROM_EMAIL_ADDRESS,
        )
        message.template_id = template_idx
        p = Personalization()
        p.add_to(Email(to_address))
        p.dynamic_template_data = template_data
        message.add_personalization(p)
        try:
            response = sendgrid_client.send(message)
            logger.info(
                f"Sending message (Response code : {response.status_code}) to {to_address}",
            )
        except Exception as e:
            logger.info(f"Error sending mail to {to_address}: {e}")


def get_template_id_and_content_for(
    notification,
    to_user_profile,
):
    """
    Retrieves template id and data from the notification parameters passed
    :param notification: Notification object
    :param to_user_profile: To user profile
    :return: dict representation of template ID and content (as dict)
    """
    template_idx = ""
    user_name = to_user_profile.get("first_name", "") or to_user_profile.get(
        "email_id",
        "",
    )

    if notification.notify_action in WS_SPECIFIC_NOTIFY_ACTIONS:
        template_idx = WORKSPACE_TEMPLATE_ID
    elif notification.notify_action in DOC_SPECIFIC_NOTIFY_ACTIONS:
        template_idx = DOCUMENT_TEMPLATE_ID
    elif notification.notify_action == NotifyAction.SEARCH_CRITERIA_WORKFLOW:
        template_idx = SEARCH_CRITERIA_WORKFLOW_TEMPLATE_ID

    # Get action string
    notification_string = get_action_str_for_email(
        notification.notify_action,
    )

    email_args = get_action_specific_sendgrid_args(
        notification.notify_action,
        notification.notify_params["workspaces"],
    )
    template_data = {
        "user_name": user_name,
        "notification_string": notification_string,
    }

    if email_args:
        template_data.update(email_args)
    return {
        "template_id": template_idx,
        "template_data": template_data,
    }


def create_and_send_email(
    notification,
    to_user_profile=None,
    update_last_email_time=False,
):
    """
    Generates the notification email and send to the destined user.
    :param notification: Notification object
    :param to_user_profile: Destination user profile
    :param update_last_email_time: Do we need to update the last email timestamp.
    :return:
    """
    if not to_user_profile:
        to_user_profile = nosql_db.get_user(notification.user_id).to_dict()
    # Retrieve / Generate the template ID and content string
    template_id_and_content = get_template_id_and_content_for(
        notification,
        to_user_profile,
    )
    # Sent the email
    send_email(
        to_user_profile["email_id"],
        template_id_and_content["template_id"],
        template_id_and_content["template_data"],
    )
    # Do we need to update the last email sent timestamp?
    if update_last_email_time:
        nosql_db.update_user(
            {
                "last_email_timestamp": datetime.datetime.now(nosql_db.t_zone).strftime(
                    nosql_db.T_ZONE_FORMAT,
                ),
            },
            user_id=to_user_profile["id"],
        )


def create_and_send_workflow_email(
    notification,
    to_user_profile,
):
    """
    Generates the notification email and send to the destined user.
    :param notification: Notification object
    :param to_user_profile: Destination user profile
    :return:
    """

    # Retrieve / Generate the template ID and content string
    template_id_and_content = get_template_id_and_content_for(
        notification,
        to_user_profile,
    )
    # Sent the email
    send_email(
        to_user_profile["email_id"],
        template_id_and_content["template_id"],
        template_id_and_content["template_data"],
    )
