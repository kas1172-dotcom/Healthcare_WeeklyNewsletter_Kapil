import unittest
from pipeline.validate import is_valid_url

class URLValidationTests(unittest.TestCase):
    def test_public_web_urls(self):
        for url in ["https://www.cms.gov/news", "http://example.org/path?q=1"]:
            self.assertTrue(is_valid_url(url))
    def test_malformed_or_unsafe_urls(self):
        for url in [None, 7, "https://", "https:///article", "javascript:alert(1)", "https://a b.example", "https://user:password@example.org", "https://[broken"]:
            with self.subTest(url=url):
                self.assertFalse(is_valid_url(url))
