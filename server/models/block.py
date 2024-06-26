# coding: utf-8

from __future__ import absolute_import
from datetime import date, datetime  # noqa: F401

from typing import List, Dict  # noqa: F401

from server.models.base_model_ import Model
from server import util


class Block(Model):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    """
    def __init__(self, block_idx: float=None, page_idx: float=None, block_text: str=None, header_text: str=None):  # noqa: E501
        """Block - a model defined in Swagger

        :param block_idx: The block_idx of this Block.  # noqa: E501
        :type block_idx: float
        :param page_idx: The page_idx of this Block.  # noqa: E501
        :type page_idx: float
        :param block_text: The block_text of this Block.  # noqa: E501
        :type block_text: str
        :param header_text: The header_text of this Block.  # noqa: E501
        :type header_text: str
        """
        self.swagger_types = {
            'block_idx': float,
            'page_idx': float,
            'block_text': str,
            'header_text': str
        }

        self.attribute_map = {
            'block_idx': 'blockIdx',
            'page_idx': 'pageIdx',
            'block_text': 'blockText',
            'header_text': 'headerText'
        }
        self._block_idx = block_idx
        self._page_idx = page_idx
        self._block_text = block_text
        self._header_text = header_text

    @classmethod
    def from_dict(cls, dikt) -> 'Block':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The Block of this Block.  # noqa: E501
        :rtype: Block
        """
        return util.deserialize_model(dikt, cls)

    @property
    def block_idx(self) -> float:
        """Gets the block_idx of this Block.


        :return: The block_idx of this Block.
        :rtype: float
        """
        return self._block_idx

    @block_idx.setter
    def block_idx(self, block_idx: float):
        """Sets the block_idx of this Block.


        :param block_idx: The block_idx of this Block.
        :type block_idx: float
        """

        self._block_idx = block_idx

    @property
    def page_idx(self) -> float:
        """Gets the page_idx of this Block.


        :return: The page_idx of this Block.
        :rtype: float
        """
        return self._page_idx

    @page_idx.setter
    def page_idx(self, page_idx: float):
        """Sets the page_idx of this Block.


        :param page_idx: The page_idx of this Block.
        :type page_idx: float
        """

        self._page_idx = page_idx

    @property
    def block_text(self) -> str:
        """Gets the block_text of this Block.


        :return: The block_text of this Block.
        :rtype: str
        """
        return self._block_text

    @block_text.setter
    def block_text(self, block_text: str):
        """Sets the block_text of this Block.


        :param block_text: The block_text of this Block.
        :type block_text: str
        """

        self._block_text = block_text

    @property
    def header_text(self) -> str:
        """Gets the header_text of this Block.


        :return: The header_text of this Block.
        :rtype: str
        """
        return self._header_text

    @header_text.setter
    def header_text(self, header_text: str):
        """Sets the header_text of this Block.


        :param header_text: The header_text of this Block.
        :type header_text: str
        """

        self._header_text = header_text
