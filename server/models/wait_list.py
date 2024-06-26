from server import util
from server.models.base_model_ import Model


class WaitList(Model):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    """

    def __init__(
        self,
        user_id: str = None,
        app_name: str = None,
        wait_list_type: str = None,
        send_notifications: bool = False,
        user_action_taken: bool = False,
        **kwargs,
    ):  # noqa: E501
        """WaitList - a model defined in Swagger

        :param user_id: The user_id of this WaitList.  # noqa: E501
        :type user_id: str
        :param app_name: The app_name of this WaitList.  # noqa: E501
        :type app_name: str
        :param wait_list_type: The wait_list_type of this WaitList.  # noqa: E501
        :type wait_list_type: str
        :param send_notifications: The send_notifications of this WaitList.  # noqa: E501
        :type send_notifications: bool
        :param user_action_taken: The user_action_taken of this WaitList.  # noqa: E501
        :type user_action_taken: bool
        """
        self.swagger_types = {
            "user_id": str,
            "app_name": str,
            "wait_list_type": str,
            "send_notifications": bool,
            "user_action_taken": bool,
        }

        self.attribute_map = {
            "user_id": "userId",
            "app_name": "appName",
            "wait_list_type": "waitListType",
            "send_notifications": "sendNotifications",
            "user_action_taken": "userActionTaken",
        }

        self._user_id = user_id
        self._app_name = app_name
        self._wait_list_type = wait_list_type
        self._send_notifications = send_notifications
        self._user_action_taken = user_action_taken

    @classmethod
    def from_dict(cls, dikt) -> "WaitList":
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The WaitList of this WaitList.  # noqa: E501
        :rtype: WaitList
        """
        return util.deserialize_model(dikt, cls)

    @property
    def user_id(self) -> str:
        """Gets the user_id of this WaitList.


        :return: The user_id of this WaitList.
        :rtype: str
        """
        return self._user_id

    @user_id.setter
    def user_id(self, user_id: str):
        """Sets the user_id of this WaitList.


        :param user_id: The user_id of this WaitList.
        :type user_id: str
        """

        self._user_id = user_id

    @property
    def app_name(self) -> str:
        """Gets the app_name of this WaitList.


        :return: The app_name of this WaitList.
        :rtype: str
        """
        return self._app_name

    @app_name.setter
    def app_name(self, app_name: str):
        """Sets the app_name of this WaitList.


        :param app_name: The app_name of this WaitList.
        :type app_name: str
        """

        self._app_name = app_name

    @property
    def wait_list_type(self) -> str:
        """Gets the wait_list_type of this WaitList.


        :return: The wait_list_type of this WaitList.
        :rtype: str
        """
        return self._wait_list_type

    @wait_list_type.setter
    def wait_list_type(self, wait_list_type: str):
        """Sets the wait_list_type of this WaitList.


        :param wait_list_type: The wait_list_type of this WaitList.
        :type wait_list_type: str
        """

        self._wait_list_type = wait_list_type

    @property
    def send_notifications(self) -> bool:
        """Gets the send_notifications of this WaitList.


        :return: The send_notifications of this WaitList.
        :rtype: bool
        """
        return self._send_notifications

    @send_notifications.setter
    def send_notifications(self, send_notifications: bool):
        """Sets the send_notifications of this WaitList.


        :param send_notifications: The send_notifications of this WaitList.
        :type send_notifications: bool
        """

        self._send_notifications = send_notifications

    @property
    def user_action_taken(self) -> bool:
        """Gets the user_action_taken of this WaitList.


        :return: The user_action_taken of this WaitList.
        :rtype: bool
        """
        return self._user_action_taken

    @user_action_taken.setter
    def user_action_taken(self, user_action_taken: bool):
        """Sets the user_action_taken of this WaitList.


        :param user_action_taken: The user_action_taken of this WaitList.
        :type user_action_taken: bool
        """

        self._user_action_taken = user_action_taken
