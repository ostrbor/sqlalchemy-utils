from unittest import TestCase

from api.utils.common import string2datetime


class TestCommonUtils(TestCase):
    def test_string2date(self):
        date = '2017-01-01 00:00:00'
        try:
            string2datetime(date)
        except Exception:
            self.fail('Error while parsing %s' % date)
