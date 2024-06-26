# coding: utf-8

from __future__ import absolute_import

from datetime import datetime  # noqa: F401

from server import util
from server.models.base_model_ import Model


class DocumentFolder(Model):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    """

    def __init__(self, id: str = None, name: str = None, workspace_id: str = None, parent_folder: str = None,
                 created_on: datetime = None, is_deleted: bool = None):  # noqa: E501
        """DocumentFolder - a model defined in Swagger

        :param id: The id of this DocumentFolder.  # noqa: E501
        :type id: str
        :param name: The name of this DocumentFolder.  # noqa: E501
        :type name: str
        :param workspace_id: The workspace_id of this DocumentFolder.  # noqa: E501
        :type workspace_id: str
        :param parent_folder: The parent_folder of this DocumentFolder.  # noqa: E501
        :type parent_folder: str
        :param created_on: The created_on of this DocumentFolder.  # noqa: E501
        :type created_on: datetime
        :param is_deleted: The is_deleted of this DocumentFolder.  # noqa: E501
        :type is_deleted: bool
        """
        self.swagger_types = {
            'id': str,
            'name': str,
            'workspace_id': str,
            'parent_folder': str,
            'created_on': datetime,
            'is_deleted': bool
        }

        self.attribute_map = {
            'id': 'id',
            'name': 'name',
            'workspace_id': 'workspaceId',
            'parent_folder': 'parentFolder',
            'created_on': 'createdOn',
            'is_deleted': 'isDeleted'
        }
        self._id = id
        self._name = name
        self._workspace_id = workspace_id
        self._parent_folder = parent_folder
        self._created_on = created_on
        self._is_deleted = is_deleted

    @classmethod
    def from_dict(cls, dikt) -> 'DocumentFolder':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The DocumentFolder of this DocumentFolder.  # noqa: E501
        :rtype: DocumentFolder
        """
        return util.deserialize_model(dikt, cls)

    @property
    def id(self) -> str:
        """Gets the id of this DocumentFolder.


        :return: The id of this DocumentFolder.
        :rtype: str
        """
        return self._id

    @id.setter
    def id(self, id: str):
        """Sets the id of this DocumentFolder.


        :param id: The id of this DocumentFolder.
        :type id: str
        """

        self._id = id

    @property
    def name(self) -> str:
        """Gets the name of this DocumentFolder.


        :return: The name of this DocumentFolder.
        :rtype: str
        """
        return self._name

    @name.setter
    def name(self, name: str):
        """Sets the name of this DocumentFolder.


        :param name: The name of this DocumentFolder.
        :type name: str
        """

        self._name = name

    @property
    def workspace_id(self) -> str:
        """Gets the workspace_id of this DocumentFolder.


        :return: The workspace_id of this DocumentFolder.
        :rtype: str
        """
        return self._workspace_id

    @workspace_id.setter
    def workspace_id(self, workspace_id: str):
        """Sets the workspace_id of this DocumentFolder.


        :param workspace_id: The workspace_id of this DocumentFolder.
        :type workspace_id: str
        """

        self._workspace_id = workspace_id

    @property
    def parent_folder(self) -> str:
        """Gets the parent_folder of this DocumentFolder.


        :return: The parent_folder of this DocumentFolder.
        :rtype: str
        """
        return self._parent_folder

    @parent_folder.setter
    def parent_folder(self, parent_folder: str):
        """Sets the parent_folder of this DocumentFolder.


        :param parent_folder: The parent_folder of this DocumentFolder.
        :type parent_folder: str
        """

        self._parent_folder = parent_folder

    @property
    def created_on(self) -> datetime:
        """Gets the created_on of this DocumentFolder.


        :return: The created_on of this DocumentFolder.
        :rtype: datetime
        """
        return self._created_on

    @created_on.setter
    def created_on(self, created_on: datetime):
        """Sets the created_on of this DocumentFolder.


        :param created_on: The created_on of this DocumentFolder.
        :type created_on: datetime
        """

        self._created_on = created_on

    @property
    def is_deleted(self) -> bool:
        """Gets the is_deleted of this DocumentFolder.


        :return: The is_deleted of this DocumentFolder.
        :rtype: bool
        """
        return self._is_deleted

    @is_deleted.setter
    def is_deleted(self, is_deleted: bool):
        """Sets the is_deleted of this DocumentFolder.


        :param is_deleted: The is_deleted of this DocumentFolder.
        :type is_deleted: bool
        """

        self._is_deleted = is_deleted
