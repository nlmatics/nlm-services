# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from server.models.id_with_message import IdWithMessage  # noqa: E501
from server.models.object import Object  # noqa: E501
from server.test import BaseTestCase


class TestDocumentFoldersController(BaseTestCase):
    """DocumentFoldersController integration test stubs"""

    def test_delete_document_folder(self):
        """Test case for delete_document_folder

        Delete an existing folder
        """
        query_string = [('recursive', true)]
        response = self.client.open(
            '/ksuhail7/service/1.0.0/documentFolder/{folderId}'.format(folder_id='folder_id_example'),
            method='DELETE',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_folder_contents(self):
        """Test case for get_folder_contents

        Returns document folder hierarchy
        """
        query_string = [('expand_all', true)]
        response = self.client.open(
            '/ksuhail7/service/1.0.0/documentFolder/{folderId}'.format(folder_id='folder_id_example'),
            method='GET',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_update_document_folder(self):
        """Test case for update_document_folder

        Updates name of an existing document folder
        """
        query_string = [('new_name', 'new_name_example')]
        response = self.client.open(
            '/ksuhail7/service/1.0.0/documentFolder/{folderId}'.format(folder_id='folder_id_example'),
            method='PUT',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
