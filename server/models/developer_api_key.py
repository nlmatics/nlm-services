from server import util
from server.models.base_model_ import Model


class DeveloperApiKey(Model):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    """

    def __init__(self, api_key: str = None, app_id: str = None):  # noqa: E501
        """DeveloperApiKey - a model defined in Swagger

        :param api_key: The api_key of this DeveloperApiKey.  # noqa: E501
        :type api_key: str
        :param app_id: The app_id of this DeveloperApiKey.  # noqa: E501
        :type app_id: str
        """
        self.swagger_types = {
            "api_key": str,
            "app_id": str,
        }

        self.attribute_map = {
            "api_key": "api_key",
            "app_id": "app_id",
        }
        self._api_key = api_key
        self._app_id = app_id

    @classmethod
    def from_dict(cls, dikt) -> "DeveloperApiKey":
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The DeveloperApiKey of this DeveloperApiKey.  # noqa: E501
        :rtype: DeveloperApiKey
        """
        return util.deserialize_model(dikt, cls)

    @property
    def api_key(self) -> str:
        """Gets the api_key of this DeveloperApiKey.


        :return: The api_key of this DeveloperApiKey.
        :rtype: str
        """
        return self._api_key

    @api_key.setter
    def api_key(self, api_key: str):
        """Sets the api_key of this DeveloperApiKey.


        :param api_key: The api_key of this DeveloperApiKey.
        :type api_key: str
        """

        self._api_key = api_key

    @property
    def app_id(self) -> str:
        """Gets the app_id of this DeveloperApiKey.


        :return: The app_id of this DeveloperApiKey.
        :rtype: str
        """
        return self._app_id

    @app_id.setter
    def app_id(self, app_id: str):
        """Sets the app_id of this DeveloperApiKey.


        :param app_id: The app_id of this DeveloperApiKey.
        :type app_id: str
        """

        self._app_id = app_id
