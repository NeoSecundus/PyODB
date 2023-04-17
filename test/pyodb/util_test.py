import re
from unittest import TestCase

from pyodb._util import generate_uid


class UtilTest(TestCase):
    def test_create_uid(self):
        for i in range(1, 100):
            self.assertIsInstance(re.fullmatch(rf"[\w\d]{{{i}}}", generate_uid(i)), re.Match)
