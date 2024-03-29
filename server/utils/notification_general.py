from enum import Enum
from enum import unique


@unique
class NotifyAction(str, Enum):
    """Different Action that can be used for generating notifications."""

    # Workspace Related
    WORKSPACE_ACCESS_ADDED = "WORKSPACE_ACCESS_ADDED"  # Access given to a workspace.
    WORKSPACE_ACCESS_REMOVED = (
        "WORKSPACE_ACCESS_REMOVED"  # Access removed from a workspace
    )
    WORKSPACE_DELETED = "WORKSPACE_DELETED"  # Workspace deleted.

    # Document Related
    DOCUMENT_ADDED_TO_WORKSPACE = (
        "DOCUMENT_ADDED_TO_WORKSPACE"  # Document added to workspace.
    )
    DOCUMENT_REMOVED_FROM_WORKSPACE = (
        "DOCUMENT_REMOVED_FROM_WORKSPACE"  # Document removed from workspace.
    )
    DOCUMENT_UPDATED_INPLACE = "DOCUMENT_UPDATED_INPLACE"  # Document updated inplace.

    # Saved Search Criteria Workflow related.
    SEARCH_CRITERIA_WORKFLOW = "SEARCH_CRITERIA_WORKFLOW"


@unique
class NotifyFreq(str, Enum):
    """Notification Frequencies."""

    # Workspace Related
    FREQ_INSTANT = "FREQ_INSTANT"  # Send notification instant.
    FREQ_DAILY = "FREQ_DAILY"  # Collect and Send notification daily
    FREQ_WEEKLY = "FREQ_WEEKLY"  # Collect and Send notification weekly


WS_SPECIFIC_NOTIFY_ACTIONS = [
    NotifyAction.WORKSPACE_ACCESS_ADDED,
    NotifyAction.WORKSPACE_ACCESS_REMOVED,
    NotifyAction.WORKSPACE_DELETED,
]

DOC_SPECIFIC_NOTIFY_ACTIONS = [
    NotifyAction.DOCUMENT_ADDED_TO_WORKSPACE,
    NotifyAction.DOCUMENT_REMOVED_FROM_WORKSPACE,
    NotifyAction.DOCUMENT_UPDATED_INPLACE,
]


def get_action_str_for_email(
    action: NotifyAction,
):
    ret_str = ""
    if action == NotifyAction.WORKSPACE_ACCESS_ADDED:
        ret_str += "You have been given permission to access \n"
    elif action == NotifyAction.WORKSPACE_ACCESS_REMOVED:
        ret_str += "Your permission has been revoked for \n"
    elif action == NotifyAction.WORKSPACE_DELETED:
        ret_str += "Below mentioned workspace(s) are deleted: \n"
    elif action == NotifyAction.DOCUMENT_ADDED_TO_WORKSPACE:
        ret_str += "There are new document(s) added to the following workspace(s): \n"
    elif action == NotifyAction.DOCUMENT_REMOVED_FROM_WORKSPACE:
        ret_str += "Below mentioned document(s) are removed from the following workspace(s): \n"
    elif action == NotifyAction.DOCUMENT_UPDATED_INPLACE:
        ret_str += "Below mentioned document(s) are updated: \n"
    elif action == NotifyAction.SEARCH_CRITERIA_WORKFLOW:
        ret_str += "Search Criteria Subscription Results from \n"
    return ret_str
