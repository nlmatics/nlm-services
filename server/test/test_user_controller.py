# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from server.models.id_with_message import IdWithMessage  # noqa: E501
from server.models.user import User  # noqa: E501
from server.test import BaseTestCase


class TestUserController(BaseTestCase):
    """UserController integration test stubs"""

    def test_create_user(self):
        """Test case for create_user

        Creates a new user
        """
        body = User()
        response = self.client.open(
            '/ksuhail7/service/1.0.0/user',
            method='POST',
            data=json.dumps(body),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_user_by_id(self):
        """Test case for delete_user_by_id

        Delete user with given id
        """
        response = self.client.open(
            '/ksuhail7/service/1.0.0/user/{userId}'.format(user_id='user_id_example'),
            method='DELETE')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_all_users(self):
        """Test case for get_all_users

        Returns list of all users
        """
        response = self.client.open(
            '/ksuhail7/service/1.0.0/users/all',
            method='GET')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_user_info_by_id(self):
        """Test case for get_user_info_by_id

        Returns information about the user with id
        """
        response = self.client.open(
            '/ksuhail7/service/1.0.0/user/{userId}'.format(user_id='user_id_example'),
            method='GET')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_update_user_by_id(self):
        """Test case for update_user_by_id

        Update an existing user with id
        """
        body = User()
        response = self.client.open(
            '/ksuhail7/service/1.0.0/user/{userId}'.format(user_id='user_id_example'),
            method='PUT',
            data=json.dumps(body),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
