# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from server.models.field_value import FieldValue  # noqa: E501
from server.models.id_with_message import IdWithMessage  # noqa: E501
from server.test import BaseTestCase


class TestExtractionController(BaseTestCase):
    """ExtractionController integration test stubs"""

    def test_extract_field_bundle_from_document(self):
        """Test case for extract_field_bundle_from_document

        Extracts values of fields from fieldBundle
        """
        query_string = [('field_bundle_id', 'field_bundle_id_example')]
        response = self.client.open(
            '/ksuhail7/service/1.0.0/extractFieldBundle/doc/{docId}'.format(doc_id='doc_id_example'),
            method='GET',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_extract_from_document(self):
        """Test case for extract_from_document

        Extracts field values from document
        """
        query_string = [('field_id', 'field_id_example')]
        response = self.client.open(
            '/ksuhail7/service/1.0.0/extractField/doc/{docId}'.format(doc_id='doc_id_example'),
            method='GET',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_override_field_value(self):
        """Test case for override_field_value

        Overrides the extracted field value
        """
        body = FieldValue()
        response = self.client.open(
            '/ksuhail7/service/1.0.0/overrideFieldValue/{fieldValueId}'.format(field_value_id='field_value_id_example'),
            method='PUT',
            data=json.dumps(body),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_remove_override_value(self):
        """Test case for remove_override_value

        Removes the overriden value for the given field id
        """
        response = self.client.open(
            '/ksuhail7/service/1.0.0/overrideFieldValue/{fieldValueId}'.format(field_value_id='field_value_id_example'),
            method='DELETE')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
