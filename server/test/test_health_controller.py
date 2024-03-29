# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from server.test import BaseTestCase


class TestHealthController(BaseTestCase):
    """HealthController integration test stubs"""

    def test_health_check(self):
        """Test case for health_check

        Health check
        """
        response = self.client.open(
            '/ksuhail7/service/1.0.0/healthz',
            method='GET')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
