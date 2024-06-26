from typing import List

from server import util
from server.models.base_model_ import Model
from server.models.result_row import ResultRow  # noqa: F401,E501
from server.models.search_criteria import SearchCriteria  # noqa: F401,E501


class SearchResult(Model):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    """

    def __init__(
        self,
        user_id: str = None,
        workspace_id: str = None,
        doc_id: str = None,
        header_text: str = None,
        group_type: str = None,
        user_report: object = None,
        tags: List[str] = None,
        raw_scores: object = None,
        search_answer: ResultRow = None,
        search_criteria: SearchCriteria = None,
    ):  # noqa: E501
        """SearchResult - a model defined in Swagger

        :param user_id: The user_id of this SearchResult.  # noqa: E501
        :type user_id: str
        :param workspace_id: The workspace_id of this SearchResult.  # noqa: E501
        :type workspace_id: str
        :param doc_id: The doc_id of this SearchResult.  # noqa: E501
        :type doc_id: str
        :param header_text: The header_text of this SearchResult.  # noqa: E501
        :type header_text: str
        :param group_type: The group_type of this SearchResult.  # noqa: E501
        :type group_type: str
        :param user_report: The user_report of this SearchResult.  # noqa: E501
        :type user_report: object
        :param tags: The tags of this SearchResult.  # noqa: E501
        :type tags: List[str]
        :param raw_scores: The raw_scores of this SearchResult.  # noqa: E501
        :type raw_scores: object
        :param search_answer: The search_answer of this SearchResult.  # noqa: E501
        :type search_answer: ResultRow
        :param search_criteria: The search_criteria of this SearchResult.  # noqa: E501
        :type search_criteria: SearchCriteria
        """
        self.swagger_types = {
            "user_id": str,
            "workspace_id": str,
            "doc_id": str,
            "header_text": str,
            "group_type": str,
            "user_report": object,
            "tags": List[str],
            "raw_scores": object,
            "search_answer": ResultRow,
            "search_criteria": SearchCriteria,
        }

        self.attribute_map = {
            "user_id": "userId",
            "workspace_id": "workspaceId",
            "doc_id": "docId",
            "header_text": "headerText",
            "group_type": "groupType",
            "user_report": "userReport",
            "tags": "tags",
            "raw_scores": "rawScores",
            "search_answer": "searchAnswer",
            "search_criteria": "searchCriteria",
        }
        self._user_id = user_id
        self._workspace_id = workspace_id
        self._doc_id = doc_id
        self._header_text = header_text
        self._group_type = group_type
        self._user_report = user_report
        self._tags = tags
        self._raw_scores = raw_scores
        self._search_answer = search_answer
        self._search_criteria = search_criteria

    @classmethod
    def from_dict(cls, dikt) -> "SearchResult":
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The SearchResult of this SearchResult.  # noqa: E501
        :rtype: SearchResult
        """
        return util.deserialize_model(dikt, cls)

    @property
    def user_id(self) -> str:
        """Gets the user_id of this SearchResult.


        :return: The user_id of this SearchResult.
        :rtype: str
        """
        return self._user_id

    @user_id.setter
    def user_id(self, user_id: str):
        """Sets the user_id of this SearchResult.


        :param user_id: The user_id of this SearchResult.
        :type user_id: str
        """

        self._user_id = user_id

    @property
    def workspace_id(self) -> str:
        """Gets the workspace_id of this SearchResult.


        :return: The workspace_id of this SearchResult.
        :rtype: str
        """
        return self._workspace_id

    @workspace_id.setter
    def workspace_id(self, workspace_id: str):
        """Sets the workspace_id of this SearchResult.


        :param workspace_id: The workspace_id of this SearchResult.
        :type workspace_id: str
        """

        self._workspace_id = workspace_id

    @property
    def doc_id(self) -> str:
        """Gets the doc_id of this SearchResult.


        :return: The doc_id of this SearchResult.
        :rtype: str
        """
        return self._doc_id

    @doc_id.setter
    def doc_id(self, doc_id: str):
        """Sets the doc_id of this SearchResult.


        :param doc_id: The doc_id of this SearchResult.
        :type doc_id: str
        """

        self._doc_id = doc_id

    @property
    def header_text(self) -> str:
        """Gets the header_text of this SearchResult.


        :return: The header_text of this SearchResult.
        :rtype: str
        """
        return self._header_text

    @header_text.setter
    def header_text(self, header_text: str):
        """Sets the header_text of this SearchResult.


        :param header_text: The header_text of this SearchResult.
        :type header_text: str
        """

        self._header_text = header_text

    @property
    def group_type(self) -> str:
        """Gets the group_type of this SearchResult.


        :return: The group_type of this SearchResult.
        :rtype: str
        """
        return self._group_type

    @group_type.setter
    def group_type(self, group_type: str):
        """Sets the group_type of this SearchResult.


        :param group_type: The group_type of this SearchResult.
        :type group_type: str
        """

        self._group_type = group_type

    @property
    def user_report(self) -> object:
        """Gets the user_report of this SearchResult.


        :return: The user_report of this SearchResult.
        :rtype: object
        """
        return self._user_report

    @user_report.setter
    def user_report(self, user_report: object):
        """Sets the user_report of this SearchResult.


        :param user_report: The user_report of this SearchResult.
        :type user_report: object
        """

        self._user_report = user_report

    @property
    def tags(self) -> List[str]:
        """Gets the tags of this SearchResult.


        :return: The tags of this SearchResult.
        :rtype: List[str]
        """
        return self._tags

    @tags.setter
    def tags(self, tags: List[str]):
        """Sets the tags of this SearchResult.


        :param tags: The tags of this SearchResult.
        :type tags: List[str]
        """

        self._tags = tags

    @property
    def raw_scores(self) -> object:
        """Gets the raw_scores of this SearchResult.


        :return: The raw_scores of this SearchResult.
        :rtype: object
        """
        return self._raw_scores

    @raw_scores.setter
    def raw_scores(self, raw_scores: object):
        """Sets the raw_scores of this SearchResult.


        :param raw_scores: The raw_scores of this SearchResult.
        :type raw_scores: object
        """

        self._raw_scores = raw_scores

    @property
    def search_answer(self) -> ResultRow:
        """Gets the search_answer of this SearchResult.


        :return: The search_answer of this SearchResult.
        :rtype: ResultRow
        """
        return self._search_answer

    @search_answer.setter
    def search_answer(self, search_answer: ResultRow):
        """Sets the search_answer of this SearchResult.


        :param search_answer: The search_answer of this SearchResult.
        :type search_answer: ResultRow
        """

        self._search_answer = search_answer

    @property
    def search_criteria(self) -> SearchCriteria:
        """Gets the search_criteria of this SearchResult.


        :return: The search_criteria of this SearchResult.
        :rtype: SearchCriteria
        """
        if isinstance(self._search_criteria, dict):
            self._search_criteria = SearchCriteria(**self._search_criteria)
        return self._search_criteria

    @search_criteria.setter
    def search_criteria(self, search_criteria: SearchCriteria):
        """Sets the search_criteria of this SearchResult.


        :param search_criteria: The search_criteria of this SearchResult.
        :type search_criteria: SearchCriteria
        """

        self._search_criteria = search_criteria
