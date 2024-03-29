# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from server.models.document import Document  # noqa: E501
from server.models.id_with_message import IdWithMessage  # noqa: E501
from server.test import BaseTestCase


class TestDocumentController(BaseTestCase):
    """DocumentController integration test stubs"""

    def test_delete_document_by_id(self):
        """Test case for delete_document_by_id

        Delete an existing document
        """
        response = self.client.open(
            '/ksuhail7/service/1.0.0/document/{documentId}'.format(document_id='document_id_example'),
            method='DELETE')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_download_document_by_id(self):
        """Test case for download_document_by_id

        Download a document identified by docId
        """
        response = self.client.open(
            '/ksuhail7/service/1.0.0/document/download/{documentId}'.format(document_id='document_id_example'),
            method='GET')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_document_info_by_id(self):
        """Test case for get_document_info_by_id

        Returns document information by id
        """
        response = self.client.open(
            '/ksuhail7/service/1.0.0/document/{documentId}'.format(document_id='document_id_example'),
            method='GET')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_documents_in_workspace(self):
        """Test case for get_documents_in_workspace

        List all documents in the workspace
        """
        response = self.client.open(
            '/ksuhail7/service/1.0.0/document/workspace/{workspaceId}'.format(workspace_id='workspace_id_example'),
            method='GET')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_update_document_by_id(self):
        """Test case for update_document_by_id

        Upload a new file to replace the document with id
        """
        data = dict(file='file_example')
        response = self.client.open(
            '/ksuhail7/service/1.0.0/document/{documentId}'.format(document_id='document_id_example'),
            method='PUT',
            data=data,
            content_type='multipart/form-data')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_upload_document(self):
        """Test case for upload_document

        Uploads a new document to a workspace
        """
        query_string = [('folder_id', 'folder_id_example')]
        data = dict(file='file_example')
        response = self.client.open(
            '/ksuhail7/service/1.0.0/document/workspace/{workspaceId}'.format(workspace_id='workspace_id_example'),
            method='POST',
            data=data,
            content_type='multipart/form-data',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
