from datetime import datetime

from server import util
from server.models.base_model_ import Model


class History(Model):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    """

    def __init__(
        self,
        user_id: str = None,
        doc_id: str = None,
        workspace_id: str = None,
        timestamp: datetime = None,
        action: str = None,
        details: dict = {},
    ):  # noqa: E501
        """History - a model defined in Swagger
        :param doc_id: The doc_id of this History.  # noqa: E501
        :type doc_id: str
        :param workspace_id: The workspace_id of this History.  # noqa: E501
        :type workspace_id: str
        :param timestamp: The timestamp of this History.  # noqa: E501
        :type timestamp: datetime
        :param id: The id of this History.  # noqa: E501
        :type id: str
        """
        self.swagger_types = {
            "user_id": str,
            "doc_id": str,
            "workspace_id": str,
            "timestamp": datetime,
            "action": str,
            "details": dict,
        }

        self.attribute_map = {
            "user_id": "user_id",
            "doc_id": "doc_id",
            "workspace_id": "workspace_id",
            "timestamp": "timestamp",
            "action": "action",
            "details": "details",
        }
        self._doc_id = doc_id
        self._workspace_id = workspace_id
        self._user_id = user_id
        self._timestamp = timestamp
        self._action = action
        self._details = details

    @classmethod
    def from_dict(cls, dikt) -> "History":
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The History of this History.  # noqa: E501
        :rtype: History
        """
        return util.deserialize_model(dikt, cls)

    @property
    def user_id(self) -> str:
        """Gets the user_id of this History.


        :return: The user_id of this History.
        :rtype: str
        """
        return self._user_id

    @user_id.setter
    def user_id(self, user_id: str):
        """Sets the user_id of this History.


        :param user_id: The user_id of this History.
        :type user_id: str
        """

        self._user_id = user_id

    @property
    def doc_id(self) -> str:
        """Gets the doc_id of this History.


        :return: The doc_id of this History.
        :rtype: str
        """
        return self._doc_id

    @doc_id.setter
    def doc_id(self, doc_id: str):
        """Sets the doc_id of this History.


        :param doc_id: The doc_id of this History.
        :type doc_id: str
        """

        self._doc_id = doc_id

    @property
    def workspace_id(self) -> str:
        """Gets the workspace_id of this History.


        :return: The workspace_id of this History.
        :rtype: str
        """
        return self._workspace_id

    @workspace_id.setter
    def workspace_id(self, workspace_id: str):
        """Sets the workspace_id of this History.


        :param workspace_id: The workspace_id of this History.
        :type workspace_id: str
        """

        self._workspace_id = workspace_id

    @property
    def timestamp(self) -> datetime:
        """Gets the timestamp of this History.


        :return: The timestamp of this History.
        :rtype: datetime
        """
        return self._timestamp

    @timestamp.setter
    def timestamp(self, timestamp: datetime):
        """Sets the timestamp of this History.


        :param timestamp: The timestamp of this History.
        :type timestamp: datetime
        """

        self._timestamp = timestamp

    @property
    def action(self) -> str:
        """Gets the action of this History.


        :return: The action of this History.
        :rtype: action
        """
        return self._action

    @action.setter
    def action(self, action: str):
        """Sets the action of this History.


        :param action: The action of this History.
        :type action: datetime
        """

        self._action = action

    @property
    def details(self) -> dict:
        """Gets the details of this History.


        :return: The details of this History.
        :rtype: datetime
        """
        return self._details

    @details.setter
    def details(self, details: datetime):
        """Sets the details of this History.


        :param details: The details of this History.
        :type details: datetime
        """

        self._details = details
