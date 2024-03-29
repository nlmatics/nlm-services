# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from server.models.id_with_message import IdWithMessage  # noqa: E501
from server.models.template import Template  # noqa: E501
from server.test import BaseTestCase


class TestTemplateController(BaseTestCase):
    """TemplateController integration test stubs"""

    def test_create_new_template_for_field_with_given_id(self):
        """Test case for create_new_template_for_field_with_given_id

        Create a new template for a given field id
        """
        body = Template()
        response = self.client.open(
            '/ksuhail7/service/1.0.0/template',
            method='POST',
            data=json.dumps(body),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_template_by_id(self):
        """Test case for delete_template_by_id

        Delete the template with given id
        """
        query_string = [('template_id', 'template_id_example'),
                        ('field_id', 'field_id_example')]
        response = self.client.open(
            '/ksuhail7/service/1.0.0/template',
            method='DELETE',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_update_template_by_id(self):
        """Test case for update_template_by_id

        update template with given id
        """
        body = Template()
        query_string = [('template_id', 'template_id_example'),
                        ('field_id', 'field_id_example')]
        response = self.client.open(
            '/ksuhail7/service/1.0.0/template',
            method='PUT',
            data=json.dumps(body),
            content_type='application/json',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
