import logging
from unittest import TestCase
import re

from src.pyodb._util import create_logger, generate_uid


class UtilTest(TestCase):
    def test_logger_creation(self):
        logger = create_logger("logs", logging.INFO, True)
        self.assertEqual(logger.level, logging.INFO)
        self.assertEqual(len(logger.handlers), 2)


    def test_create_uid(self):
        for i in range(1, 100):
            self.assertIsInstance(re.fullmatch(rf"[\w\d]{{{i}}}", generate_uid(i)), re.Match)
