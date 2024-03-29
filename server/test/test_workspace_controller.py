# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from server.models.id_with_message import IdWithMessage  # noqa: E501
from server.models.workspace import Workspace  # noqa: E501
from server.test import BaseTestCase


class TestWorkspaceController(BaseTestCase):
    """WorkspaceController integration test stubs"""

    def test_create_new_workspace(self):
        """Test case for create_new_workspace

        Creates a new workspace for a user
        """
        body = Workspace()
        response = self.client.open(
            '/ksuhail7/service/1.0.0/workspace',
            method='POST',
            data=json.dumps(body),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_workspace_by_id(self):
        """Test case for delete_workspace_by_id

        Deletes the workspace for given id
        """
        response = self.client.open(
            '/ksuhail7/service/1.0.0/workspace/{workspaceId}'.format(workspace_id='workspace_id_example'),
            method='DELETE')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_all_workspaces(self):
        """Test case for get_all_workspaces

        Returns list of all workspaces
        """
        response = self.client.open(
            '/ksuhail7/service/1.0.0/workspaces/all',
            method='GET')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_workspace_by_id(self):
        """Test case for get_workspace_by_id

        Return workspace for given workspaceId
        """
        response = self.client.open(
            '/ksuhail7/service/1.0.0/workspace/{workspaceId}'.format(workspace_id='workspace_id_example'),
            method='GET')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_workspaces_by_user_id(self):
        """Test case for get_workspaces_by_user_id

        Return list of workspace for user
        """
        response = self.client.open(
            '/ksuhail7/service/1.0.0/workspace/user/{userId}'.format(user_id='user_id_example'),
            method='GET')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_update_workspace_by_id(self):
        """Test case for update_workspace_by_id

        Updates the workspace for given id
        """
        response = self.client.open(
            '/ksuhail7/service/1.0.0/workspace/{workspaceId}'.format(workspace_id='workspace_id_example'),
            method='PUT')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
