
import unittest
from bot.schedule_manager import get_full_link
from urllib.parse import urlparse

class TestGetFullLink(unittest.TestCase):
    def test_get_full_link(self):
        # относительная ссылка
        self.assertTrue(get_full_link('/path/to/file.xlsx').startswith('http'))

        # ссылка без схемы
        result = get_full_link('//example.com/file.xlsx')
        parsed = urlparse(result)
        self.assertEqual(parsed.scheme, 'http')

        # полная ссылка
        url = 'https://example.com/file.xlsx'
        self.assertEqual(get_full_link(url), url)

if __name__ == '__main__':
    unittest.main()
