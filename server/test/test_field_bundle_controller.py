# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from server.models.body2 import Body2  # noqa: E501
from server.models.field_bundle import FieldBundle  # noqa: E501
from server.models.id_with_message import IdWithMessage  # noqa: E501
from server.test import BaseTestCase


class TestFieldBundleController(BaseTestCase):
    """FieldBundleController integration test stubs"""

    def test_add_entity_to_field_bundle(self):
        """Test case for add_entity_to_field_bundle

        Adds an existing Field to a FieldBundle (does not create a new field)
        """
        query_string = [('field_bundle_id', 'field_bundle_id_example'),
                        ('entity_id', 'entity_id_example'),
                        ('entity_type', 'entity_type_example')]
        response = self.client.open(
            '/ksuhail7/service/1.0.0/fieldBundle/field',
            method='POST',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_create_new_field_bundle(self):
        """Test case for create_new_field_bundle

        Create a new field bundle
        """
        body = Body2()
        response = self.client.open(
            '/ksuhail7/service/1.0.0/fieldBundle',
            method='POST',
            data=json.dumps(body),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_field_bundle(self):
        """Test case for delete_field_bundle

        Deletes a field bundle
        """
        response = self.client.open(
            '/ksuhail7/service/1.0.0/fieldBundle/{fieldBundleId}'.format(field_bundle_id='field_bundle_id_example'),
            method='DELETE')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_field_bundle_info(self):
        """Test case for get_field_bundle_info

        Returns fieldBundle information
        """
        response = self.client.open(
            '/ksuhail7/service/1.0.0/fieldBundle/{fieldBundleId}'.format(field_bundle_id='field_bundle_id_example'),
            method='GET')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_remove_field_from_field_bundle(self):
        """Test case for remove_field_from_field_bundle

        Remove Field from a FieldBundle (does not delete the field itself)
        """
        query_string = [('field_bundle_id', 'field_bundle_id_example'),
                        ('field_id', 'field_id_example')]
        response = self.client.open(
            '/ksuhail7/service/1.0.0/fieldBundle/field',
            method='DELETE',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_update_field_bundle_info(self):
        """Test case for update_field_bundle_info

        Updates a field bundle
        """
        body = FieldBundle()
        response = self.client.open(
            '/ksuhail7/service/1.0.0/fieldBundle/{fieldBundleId}'.format(field_bundle_id='field_bundle_id_example'),
            method='PUT',
            data=json.dumps(body),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
