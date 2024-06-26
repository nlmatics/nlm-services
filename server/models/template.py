# coding: utf-8

from __future__ import absolute_import

from server import util
from server.models.base_model_ import Model


class Template(Model):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    """
    def __init__(self, id: str=None, active: bool=True, field_id: str=None, template_type: str=None, text: str=None, index: int=None):  # noqa: E501
        """Template - a model defined in Swagger

        :param id: The id of this Template.  # noqa: E501
        :type id: str
        :param active: The active of this Template.  # noqa: E501
        :type active: bool
        :param field_id: The field_id of this Template.  # noqa: E501
        :type field_id: str
        :param template_type: The template_type of this Template.  # noqa: E501
        :type template_type: str
        :param text: The text of this Template.  # noqa: E501
        :type text: str
        :param index: The index of this Template.  # noqa: E501
        :type index: float
        """
        self.swagger_types = {
            'id': str,
            'active': bool,
            'field_id': str,
            'template_type': str,
            'text': str,
            'index': int
        }

        self.attribute_map = {
            'id': 'id',
            'active': 'active',
            'field_id': 'fieldId',
            'template_type': 'templateType',
            'text': 'text',
            'index': 'index'
        }
        self._id = id
        self._active = active
        self._field_id = field_id
        self._template_type = template_type
        self._text = text
        self._index = index

    @classmethod
    def from_dict(cls, dikt) -> 'Template':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The Template of this Template.  # noqa: E501
        :rtype: Template
        """
        return util.deserialize_model(dikt, cls)

    @property
    def id(self) -> str:
        """Gets the id of this Template.


        :return: The id of this Template.
        :rtype: str
        """
        return self._id

    @id.setter
    def id(self, id: str):
        """Sets the id of this Template.


        :param id: The id of this Template.
        :type id: str
        """

        self._id = id

    @property
    def active(self) -> bool:
        """Gets the active of this Template.


        :return: The active of this Template.
        :rtype: bool
        """
        return self._active

    @active.setter
    def active(self, active: bool):
        """Sets the active of this Template.


        :param active: The active of this Template.
        :type active: bool
        """

        self._active = active

    @property
    def field_id(self) -> str:
        """Gets the field_id of this Template.


        :return: The field_id of this Template.
        :rtype: str
        """
        return self._field_id

    @field_id.setter
    def field_id(self, field_id: str):
        """Sets the field_id of this Template.


        :param field_id: The field_id of this Template.
        :type field_id: str
        """

        self._field_id = field_id

    @property
    def template_type(self) -> str:
        """Gets the template_type of this Template.


        :return: The template_type of this Template.
        :rtype: str
        """
        return self._template_type

    @template_type.setter
    def template_type(self, template_type: str):
        """Sets the template_type of this Template.


        :param template_type: The template_type of this Template.
        :type template_type: str
        """
        allowed_values = ["phrase", "question"]  # noqa: E501
        if template_type not in allowed_values:
            raise ValueError(
                "Invalid value for `template_type` ({0}), must be one of {1}"
                .format(template_type, allowed_values)
            )

        self._template_type = template_type

    @property
    def text(self) -> str:
        """Gets the text of this Template.


        :return: The text of this Template.
        :rtype: str
        """
        return self._text

    @text.setter
    def text(self, text: str):
        """Sets the text of this Template.


        :param text: The text of this Template.
        :type text: str
        """

        self._text = text

    @property
    def index(self) -> int:
        """Gets the index of this Template.


        :return: The index of this Template.
        :rtype: float
        """
        return self._index

    @index.setter
    def index(self, index: int):
        """Sets the index of this Template.


        :param index: The index of this Template.
        :type index: float
        """

        self._index = index
