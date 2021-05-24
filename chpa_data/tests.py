from django.test import TestCase

# Create your tests here.
import unittest
from .views import delesig
class TestDict(unittest.TestCase):
    def test_getcode(self):
        a=delesig("`abc`")
        self.assertEqual('abc',a)
if __name__ == '__main__':
    unittest.main()