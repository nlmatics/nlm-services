from server import util
from server.models.base_model_ import Model


class UserFeedback(Model):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    """

    def __init__(
        self,
        user_id: str = None,
        rating_stars: float = None,
        feedback: str = None,
        **kwargs,
    ):  # noqa: E501
        """UserFeedback - a model defined in Swagger

        :param user_id: The user_id of this UserFeedback.  # noqa: E501
        :type user_id: str
        :param rating_stars: The rating_stars of this UserFeedback.  # noqa: E501
        :type rating_stars: float
        :param feedback: The feedback of this UserFeedback.  # noqa: E501
        :type feedback: str
        """
        self.swagger_types = {
            "user_id": str,
            "rating_stars": float,
            "feedback": str,
        }

        self.attribute_map = {
            "user_id": "userId",
            "rating_stars": "ratingStars",
            "feedback": "feedback",
        }

        self._user_id = user_id
        self._rating_stars = rating_stars
        self._feedback = feedback

    @classmethod
    def from_dict(cls, dikt) -> "UserFeedback":
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The UserFeedback of this UserFeedback.  # noqa: E501
        :rtype: UserFeedback
        """
        return util.deserialize_model(dikt, cls)

    @property
    def user_id(self) -> str:
        """Gets the user_id of this UserFeedback.


        :return: The user_id of this UserFeedback.
        :rtype: str
        """
        return self._user_id

    @user_id.setter
    def user_id(self, user_id: str):
        """Sets the user_id of this UserFeedback.


        :param user_id: The user_id of this UserFeedback.
        :type user_id: str
        """

        self._user_id = user_id

    @property
    def rating_stars(self) -> float:
        """Gets the rating_stars of this UserFeedback.


        :return: The rating_stars of this UserFeedback.
        :rtype: float
        """
        return self._rating_stars

    @rating_stars.setter
    def rating_stars(self, rating_stars: float):
        """Sets the rating_stars of this UserFeedback.


        :param rating_stars: The rating_stars of this UserFeedback.
        :type rating_stars: float
        """

        self._rating_stars = rating_stars

    @property
    def feedback(self) -> str:
        """Gets the feedback of this UserFeedback.


        :return: The feedback of this UserFeedback.
        :rtype: str
        """
        return self._feedback

    @feedback.setter
    def feedback(self, feedback: str):
        """Sets the feedback of this UserFeedback.


        :param feedback: The feedback of this UserFeedback.
        :type feedback: str
        """

        self._feedback = feedback