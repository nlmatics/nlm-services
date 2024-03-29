# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from server.models.ignore_block import IgnoreBlock  # noqa: E501
from server.models.id_with_message import IdWithMessage  # noqa: E501
from server.test import BaseTestCase


class TestIgnoreBlock(BaseTestCase):
    """FieldController integration test stubs"""

    def test_create_ignore_block(self):
        """Test case for create_ignore_block

        Add New User defined Ignore Block
        """
        body = IgnoreBlock()
        response = self.client.open(
            '/ksuhail7/service/1.0.0/ignoreBlock',
            method='POST',
            data=json.dumps(body),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
