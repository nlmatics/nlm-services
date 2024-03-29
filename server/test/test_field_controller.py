# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from server.models.field import Field  # noqa: E501
from server.models.id_with_message import IdWithMessage  # noqa: E501
from server.test import BaseTestCase


class TestFieldController(BaseTestCase):
    """FieldController integration test stubs"""

    def test_add_field(self):
        """Test case for add_field

        Add New User defined Field (no bundle)
        """
        body = Field()
        response = self.client.open(
            '/ksuhail7/service/1.0.0/field',
            method='POST',
            data=json.dumps(body),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_field_by_field_id(self):
        """Test case for get_field_by_field_id

        Returns the field by given id
        """
        query_string = [('field_bundle_id', 'field_bundle_id_example')]
        response = self.client.open(
            '/ksuhail7/service/1.0.0/field/{fieldId}'.format(field_id='field_id_example'),
            method='GET',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
