from server import util
from server.models.base_model_ import Model


class MetricData(Model):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    """

    def __init__(
        self,
        catalog_id: str = None,
        feature: str = None,
        used: str = None,
        quota: str = None,
        percent_used: str = None,
    ):  # noqa: E501
        """MetricData - a model defined in Swagger

        :param catalog_id: The catalog_id of this MetricData.  # noqa: E501
        :type catalog_id: str
        :param feature: The feature of this MetricData.  # noqa: E501
        :type feature: str
        :param used: The used of this MetricData.  # noqa: E501
        :type used: str
        :param quota: The quota of this MetricData.  # noqa: E501
        :type quota: str
        :param percent_used: The percent_used of this MetricData.  # noqa: E501
        :type percent_used: str
        """
        self.swagger_types = {
            "catalog_id": str,
            "feature": str,
            "used": str,
            "quota": str,
            "percent_used": str,
        }

        self.attribute_map = {
            "catalog_id": "catalog_id",
            "feature": "feature",
            "used": "used",
            "quota": "quota",
            "percent_used": "percentUsed",
        }
        self._catalog_id = catalog_id
        self._feature = feature
        self._used = used
        self._quota = quota
        self._percent_used = percent_used

    @classmethod
    def from_dict(cls, dikt) -> "MetricData":
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The MetricData of this MetricData.  # noqa: E501
        :rtype: MetricData
        """
        return util.deserialize_model(dikt, cls)

    @property
    def catalog_id(self) -> str:
        """Gets the catalog_id of this MetricData.


        :return: The catalog_id of this MetricData.
        :rtype: str
        """
        return self._catalog_id

    @catalog_id.setter
    def catalog_id(self, catalog_id: str):
        """Sets the catalog_id of this MetricData.


        :param catalog_id: The catalog_id of this MetricData.
        :type catalog_id: str
        """

        self._catalog_id = catalog_id

    @property
    def feature(self) -> str:
        """Gets the feature of this MetricData.


        :return: The feature of this MetricData.
        :rtype: str
        """
        return self._feature

    @feature.setter
    def feature(self, feature: str):
        """Sets the feature of this MetricData.


        :param feature: The feature of this MetricData.
        :type feature: str
        """

        self._feature = feature

    @property
    def used(self) -> str:
        """Gets the used of this MetricData.


        :return: The used of this MetricData.
        :rtype: str
        """
        return self._used

    @used.setter
    def used(self, used: str):
        """Sets the used of this MetricData.


        :param used: The used of this MetricData.
        :type used: str
        """

        self._used = used

    @property
    def quota(self) -> str:
        """Gets the quota of this MetricData.


        :return: The quota of this MetricData.
        :rtype: str
        """
        return self._quota

    @quota.setter
    def quota(self, quota: str):
        """Sets the quota of this MetricData.


        :param quota: The quota of this MetricData.
        :type quota: str
        """

        self._quota = quota

    @property
    def percent_used(self) -> str:
        """Gets the percent_used of this MetricData.


        :return: The percent_used of this MetricData.
        :rtype: str
        """
        return self._percent_used

    @percent_used.setter
    def percent_used(self, percent_used: str):
        """Sets the percent_used of this MetricData.


        :param percent_used: The percent_used of this MetricData.
        :type percent_used: str
        """

        self._percent_used = percent_used