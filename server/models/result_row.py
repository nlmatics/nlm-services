# coding: utf-8

from __future__ import absolute_import
from datetime import date, datetime  # noqa: F401

from typing import List, Dict  # noqa: F401

from server.models.base_model_ import Model
from server import util


class ResultRow(Model):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    """
    def __init__(self, file_id: str=None, file_name: str=None, phrase: str=None, match_idx: float=None, page_idx: float=None, level: str=None, answer: str=None, formatted_answer: str=None, match_score: float=None, answer_score: float=None, scaled_score: float=None, question_score: float=None):  # noqa: E501
        """ResultRow - a model defined in Swagger

        :param file_id: The file_id of this ResultRow.  # noqa: E501
        :type file_id: str
        :param file_name: The file_name of this ResultRow.  # noqa: E501
        :type file_name: str
        :param phrase: The phrase of this ResultRow.  # noqa: E501
        :type phrase: str
        :param match_idx: The match_idx of this ResultRow.  # noqa: E501
        :type match_idx: float
        :param page_idx: The page_idx of this ResultRow.  # noqa: E501
        :type page_idx: float
        :param level: The level of this ResultRow.  # noqa: E501
        :type level: str
        :param answer: The answer of this ResultRow.  # noqa: E501
        :type answer: str
        :param formatted_answer: The formatted_answer of this ResultRow.  # noqa: E501
        :type formatted_answer: str
        :param match_score: The match_score of this ResultRow.  # noqa: E501
        :type match_score: float
        :param answer_score: The answer_score of this ResultRow.  # noqa: E501
        :type answer_score: float
        :param scaled_score: The scaled_score of this ResultRow.  # noqa: E501
        :type scaled_score: float
        :param question_score: The question_score of this ResultRow.  # noqa: E501
        :type question_score: float
        """
        self.swagger_types = {
            'file_id': str,
            'file_name': str,
            'phrase': str,
            'match_idx': float,
            'page_idx': float,
            'level': str,
            'answer': str,
            'formatted_answer': str,
            'match_score': float,
            'answer_score': float,
            'scaled_score': float,
            'question_score': float
        }

        self.attribute_map = {
            'file_id': 'fileId',
            'file_name': 'fileName',
            'phrase': 'phrase',
            'match_idx': 'matchIdx',
            'page_idx': 'pageIdx',
            'level': 'level',
            'answer': 'answer',
            'formatted_answer': 'formattedAnswer',
            'match_score': 'matchScore',
            'answer_score': 'answerScore',
            'scaled_score': 'scaledScore',
            'question_score': 'questionScore'
        }
        self._file_id = file_id
        self._file_name = file_name
        self._phrase = phrase
        self._match_idx = match_idx
        self._page_idx = page_idx
        self._level = level
        self._answer = answer
        self._formatted_answer = formatted_answer
        self._match_score = match_score
        self._answer_score = answer_score
        self._scaled_score = scaled_score
        self._question_score = question_score

    @classmethod
    def from_dict(cls, dikt) -> 'ResultRow':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The ResultRow of this ResultRow.  # noqa: E501
        :rtype: ResultRow
        """
        return util.deserialize_model(dikt, cls)

    @property
    def file_id(self) -> str:
        """Gets the file_id of this ResultRow.


        :return: The file_id of this ResultRow.
        :rtype: str
        """
        return self._file_id

    @file_id.setter
    def file_id(self, file_id: str):
        """Sets the file_id of this ResultRow.


        :param file_id: The file_id of this ResultRow.
        :type file_id: str
        """

        self._file_id = file_id

    @property
    def file_name(self) -> str:
        """Gets the file_name of this ResultRow.


        :return: The file_name of this ResultRow.
        :rtype: str
        """
        return self._file_name

    @file_name.setter
    def file_name(self, file_name: str):
        """Sets the file_name of this ResultRow.


        :param file_name: The file_name of this ResultRow.
        :type file_name: str
        """

        self._file_name = file_name

    @property
    def phrase(self) -> str:
        """Gets the phrase of this ResultRow.


        :return: The phrase of this ResultRow.
        :rtype: str
        """
        return self._phrase

    @phrase.setter
    def phrase(self, phrase: str):
        """Sets the phrase of this ResultRow.


        :param phrase: The phrase of this ResultRow.
        :type phrase: str
        """

        self._phrase = phrase

    @property
    def match_idx(self) -> float:
        """Gets the match_idx of this ResultRow.


        :return: The match_idx of this ResultRow.
        :rtype: float
        """
        return self._match_idx

    @match_idx.setter
    def match_idx(self, match_idx: float):
        """Sets the match_idx of this ResultRow.


        :param match_idx: The match_idx of this ResultRow.
        :type match_idx: float
        """

        self._match_idx = match_idx

    @property
    def page_idx(self) -> float:
        """Gets the page_idx of this ResultRow.


        :return: The page_idx of this ResultRow.
        :rtype: float
        """
        return self._page_idx

    @page_idx.setter
    def page_idx(self, page_idx: float):
        """Sets the page_idx of this ResultRow.


        :param page_idx: The page_idx of this ResultRow.
        :type page_idx: float
        """

        self._page_idx = page_idx

    @property
    def level(self) -> str:
        """Gets the level of this ResultRow.


        :return: The level of this ResultRow.
        :rtype: str
        """
        return self._level

    @level.setter
    def level(self, level: str):
        """Sets the level of this ResultRow.


        :param level: The level of this ResultRow.
        :type level: str
        """

        self._level = level

    @property
    def answer(self) -> str:
        """Gets the answer of this ResultRow.


        :return: The answer of this ResultRow.
        :rtype: str
        """
        return self._answer

    @answer.setter
    def answer(self, answer: str):
        """Sets the answer of this ResultRow.


        :param answer: The answer of this ResultRow.
        :type answer: str
        """

        self._answer = answer

    @property
    def formatted_answer(self) -> str:
        """Gets the formatted_answer of this ResultRow.


        :return: The formatted_answer of this ResultRow.
        :rtype: str
        """
        return self._formatted_answer

    @formatted_answer.setter
    def formatted_answer(self, formatted_answer: str):
        """Sets the formatted_answer of this ResultRow.


        :param formatted_answer: The formatted_answer of this ResultRow.
        :type formatted_answer: str
        """

        self._formatted_answer = formatted_answer

    @property
    def match_score(self) -> float:
        """Gets the match_score of this ResultRow.


        :return: The match_score of this ResultRow.
        :rtype: float
        """
        return self._match_score

    @match_score.setter
    def match_score(self, match_score: float):
        """Sets the match_score of this ResultRow.


        :param match_score: The match_score of this ResultRow.
        :type match_score: float
        """

        self._match_score = match_score

    @property
    def answer_score(self) -> float:
        """Gets the answer_score of this ResultRow.


        :return: The answer_score of this ResultRow.
        :rtype: float
        """
        return self._answer_score

    @answer_score.setter
    def answer_score(self, answer_score: float):
        """Sets the answer_score of this ResultRow.


        :param answer_score: The answer_score of this ResultRow.
        :type answer_score: float
        """

        self._answer_score = answer_score

    @property
    def scaled_score(self) -> float:
        """Gets the scaled_score of this ResultRow.


        :return: The scaled_score of this ResultRow.
        :rtype: float
        """
        return self._scaled_score

    @scaled_score.setter
    def scaled_score(self, scaled_score: float):
        """Sets the scaled_score of this ResultRow.


        :param scaled_score: The scaled_score of this ResultRow.
        :type scaled_score: float
        """

        self._scaled_score = scaled_score

    @property
    def question_score(self) -> float:
        """Gets the question_score of this ResultRow.


        :return: The question_score of this ResultRow.
        :rtype: float
        """
        return self._question_score

    @question_score.setter
    def question_score(self, question_score: float):
        """Sets the question_score of this ResultRow.


        :param question_score: The question_score of this ResultRow.
        :type question_score: float
        """

        self._question_score = question_score
