from typing import List

from server import util
from server.models.base_model_ import Model
from server.models.criteria import Criteria  # noqa: F401,E501
from server.models.field_filter import FieldFilter


class SearchCriteria(Model):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    """

    def __init__(
        self,
        criterias: List[Criteria] = None,
        extractors: List[object] = None,
        post_processors: List[str] = None,
        aggregate_post_processors: List[str] = None,
        doc_per_page: int = 20,
        offset: int = 0,
        match_per_doc: int = 20,
        debug: bool = True,
        topn: int = 3,
        group_by_file: bool = True,
        search_type: str = "extraction",
        field_filter: FieldFilter = None,
        doc_filters: List[str] = None,
        disable_extraction: bool = False,
        abstractive_processors: List[object] = None,
        **kwargs,
    ):  # noqa: E501
        """SearchCriteria - a model defined in Swagger

        :param criterias: The criterias of this SearchCriteria.  # noqa: E501
        :type criterias: List[str]
        :param extractors: The extractors of this SearchCriteria.  # noqa: E501
        :type extractors: List[object]
        :param template_question: The template_question of this SearchCriteria.  # noqa: E501
        :type template_question: str
        :param header_text: The header_text of this SearchCriteria.  # noqa: E501
        :type header_text: str
        :param post_processors: The post_processors of this SearchCriteria.  # noqa: E501
        :type post_processors: str
        :param aggregate_processors: The aggregate_processors of this SearchCriteria.  # noqa: E501
        :type aggregate_processors: str
        :param topn: The topn of this SearchCriteria.  # noqa: E501
        :type topn: int
        :param doc_filters: The doc_filters of this SearchCriteria.  # noqa: E501
        :type doc_filters: List[str]
        :param disable_extraction: The disable_extraction of this SearchCriteria.
        :type: disable_extraction: bool
        :param abstractive_processors: The abstractive_processors of this SearchCriteria.  # noqa: E501
        :type abstractive_processors: List[object]
        """
        self.swagger_types = {
            "criterias": List[Criteria],
            "extractors": List[str],
            "post_processors": List[str],
            "aggregate_post_processors": List[str],
            "doc_per_page": int,
            "offset": int,
            "match_per_doc": int,
            "debug": bool,
            "topn": int,
            "group_by_file": bool,
            "search_type": str,
            "field_filter": FieldFilter,
            "doc_filters": List[str],
            "disable_extraction": bool,
            "abstractive_processors": List[str],
        }

        self.attribute_map = {
            "criterias": "criterias",
            "extractors": "extractors",
            "post_processors": "postProcessors",
            "aggregate_post_processors": "aggregatePostProcessors",
            "doc_per_page": "docPerPage",
            "offset": "offset",
            "match_per_doc": "matchPerDoc",
            "debug": "debug",
            "topn": "topn",
            "group_by_file": "groupByFile",
            "search_type": "searchType",
            "field_filter": "fieldFilter",
            "doc_filters": "docFilters",
            "disable_extraction": "disableExtraction",
            "abstractive_processors": "abstractiveProcessors",
        }

        self._criterias = criterias or []
        self._extractors = extractors or []
        self._post_processors = post_processors or []
        self._aggregate_post_processors = aggregate_post_processors or []
        self._topn = topn
        self._debug = debug
        self._doc_per_page = doc_per_page
        self._offset = offset
        self._match_per_doc = match_per_doc
        self._group_by_file = group_by_file
        self._search_type = search_type
        self._field_filter = field_filter
        self._doc_filters = doc_filters or []
        self._disable_extraction = disable_extraction
        self._abstractive_processors = abstractive_processors or []

    @classmethod
    def from_dict(cls, dikt) -> "SearchCriteria":
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The SearchCriteria of this SearchCriteria.  # noqa: E501
        :rtype: SearchCriteria
        """
        return util.deserialize_model(dikt, cls)

    @property
    def criterias(self) -> List[Criteria]:
        """Gets the criterias of this SearchCriteria.


        :return: The criterias of this SearchCriteria.
        :rtype: List[Criteria]
        """
        if all([isinstance(x, dict) for x in self._criterias]):
            self._criterias = [Criteria(**x) for x in self._criterias]
        return self._criterias

    @criterias.setter
    def criterias(self, criterias: List[Criteria]):
        """Sets the criterias of this SearchCriteria.


        :param criterias: The criterias of this SearchCriteria.
        :type criterias: List[Criteria]
        """
        self._criterias = criterias

    @property
    def extractors(self) -> List[str]:
        """Gets the extractors of this SearchCriteria.


        :return: The extractors of this SearchCriteria.
        :rtype: List[str]
        """
        return self._extractors

    @extractors.setter
    def extractors(self, extractors: List[str]):
        """Sets the extractors of this SearchCriteria.


        :param extractors: The extractors of this SearchCriteria.
        :type extractors: List[str]
        """

        self._extractors = extractors

    @property
    def post_processors(self) -> List[str]:
        """Gets the post_processors of this SearchCriteria.


        :return: The post_processors of this SearchCriteria.
        :rtype: List[str]
        """
        return self._post_processors

    @post_processors.setter
    def post_processors(self, post_processors: List[str]):
        """Sets the post_processors of this SearchCriteria.


        :param post_processors: The post_processors of this SearchCriteria.
        :type post_processors: List[str]
        """

        self._post_processors = post_processors

    @property
    def aggregate_post_processors(self) -> List[str]:
        """Gets the aggregate_post_processors of this SearchCriteria.


        :return: The aggregate_post_processors of this SearchCriteria.
        :rtype: List[str]
        """
        return self._aggregate_post_processors

    @aggregate_post_processors.setter
    def aggregate_post_processors(self, aggregate_post_processors: List[str]):
        """Sets the aggregate_post_processors of this SearchCriteria.


        :param aggregate_post_processors: The aggregate_post_processors of this SearchCriteria.
        :type aggregate_post_processors: List[str]
        """

        self._aggregate_post_processors = aggregate_post_processors

    @property
    def topn(self) -> int:
        """Gets the topn of this SearchCriteria.


        :return: The topn of this SearchCriteria.
        :rtype: int
        """
        return self._topn

    @topn.setter
    def topn(self, topn: int):
        """Sets the topn of this SearchCriteria.


        :param topn: The topn of this SearchCriteria.
        :type topn: int
        """

        self._topn = topn

    @property
    def debug(self) -> bool:
        """Gets the debug of this SearchCriteria.


        :return: The debug of this SearchCriteria.
        :rtype: bool
        """
        return self._debug

    @debug.setter
    def debug(self, debug: bool):
        """Sets the debug of this SearchCriteria.


        :param debug: The debug of this SearchCriteria.
        :type debug: bool
        """

        self._debug = debug

    @property
    def doc_per_page(self) -> int:
        """Gets the doc_per_page of this SearchCriteria.


        :return: The doc_per_page of this SearchCriteria.
        :rtype: int
        """
        return self._doc_per_page

    @doc_per_page.setter
    def doc_per_page(self, doc_per_page: int):
        """Sets the doc_per_page of this SearchCriteria.


        :param doc_per_page: The doc_per_page of this SearchCriteria.
        :type doc_per_page: int
        """

        self._doc_per_page = doc_per_page

    @property
    def offset(self) -> int:
        """Gets the offset of this SearchCriteria.


        :return: The offset of this SearchCriteria.
        :rtype: int
        """
        return self._offset

    @offset.setter
    def offset(self, offset: int):
        """Sets the offset of this SearchCriteria.


        :param offset: The offset of this SearchCriteria.
        :type offset: int
        """

        self._offset = offset

    @property
    def match_per_doc(self) -> int:
        """Gets the match_per_doc of this SearchCriteria.


        :return: The match_per_doc of this SearchCriteria.
        :rtype: int
        """
        return self._match_per_doc

    @match_per_doc.setter
    def match_per_doc(self, match_per_doc: int):
        """Sets the match_per_doc of this SearchCriteria.


        :param match_per_doc: The match_per_doc of this SearchCriteria.
        :type match_per_doc: int
        """

        self._match_per_doc = match_per_doc

    @property
    def topn(self) -> int:
        """Gets the topn of this Field.


        :return: The topn of this Field.
        :rtype: int
        """
        return self._topn

    @topn.setter
    def topn(self, topn: int):
        """Sets the topn of this Field.


        :param topn: The topn of this Field.
        :type topn: int
        """

        self._topn = topn

    @property
    def group_by_file(self) -> bool:
        """Gets the group_by_file of this Field.


        :return: The group_by_file of this Field.
        :rtype: bool
        """
        return self._group_by_file

    @group_by_file.setter
    def group_by_file(self, group_by_file: bool):
        """Sets the group_by_file of this Field.


        :param group_by_file: The group_by_file of this Field.
        :type group_by_file: bool
        """

        self._group_by_file = group_by_file

    @property
    def search_type(self) -> str:
        """Gets the search_type of this Field.


        :return: The search_type of this Field.
        :rtype: str
        """
        return self._search_type

    @search_type.setter
    def search_type(self, search_type: str):
        """Sets the search_type of this Field.


        :param search_type: The search_type of this Field.
        :type search_type: str
        """

        self._search_type = search_type

    @property
    def field_filter(self) -> FieldFilter:
        """Gets the field_filter of this SearchCriteria.


        :return: The field_filter of this SearchCriteria.
        :rtype: FieldFilter
        """
        return self._field_filter

    @field_filter.setter
    def field_filter(self, field_filter: FieldFilter):
        """Sets the field_filter of this SearchCriteria.


        :param field_filter: The field_filter of this SearchCriteria.
        :type field_filter: FieldFilter
        """
        self._field_filter = field_filter

    @property
    def doc_filters(self) -> List[str]:
        """Gets the doc_filters of this SearchCriteria.


        :return: The doc_filters of this SearchCriteria.
        :rtype: List[str]
        """
        return self._doc_filters

    @doc_filters.setter
    def doc_filters(self, doc_filters: List[str]):
        """Sets the doc_filters of this SearchCriteria.


        :param doc_filters: The doc_filters of this SearchCriteria.
        :type doc_filters: List[str]
        """

        self._doc_filters = doc_filters

    @property
    def disable_extraction(self) -> bool:
        """Gets the disable_extraction of this SearchCriteria.


        :return: The disable_extraction of this SearchCriteria.
        :rtype: bool
        """
        return self._disable_extraction

    @disable_extraction.setter
    def disable_extraction(self, disable_extraction: bool):
        """Sets the disable_extraction of this SearchCriteria.


        :param disable_extraction: The disable_extraction of this SearchCriteria.
        :type disable_extraction: bool
        """

        self._disable_extraction = disable_extraction

    @property
    def abstractive_processors(self) -> List[str]:
        """Gets the abstractive_processors of this SearchCriteria.


        :return: The abstractive_processors of this SearchCriteria.
        :rtype: List[str]
        """
        return self._abstractive_processors

    @abstractive_processors.setter
    def abstractive_processors(self, abstractive_processors: List[str]):
        """Sets the abstractive_processors of this SearchCriteria.


        :param abstractive_processors: The abstractive_processors of this SearchCriteria.
        :type abstractive_processors: List[str]
        """

        self._abstractive_processors = abstractive_processors