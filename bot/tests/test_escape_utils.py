
import unittest
from bot.schedule_manager import escape_html

class TestEscapeHtml(unittest.TestCase):
    def test_escape_html(self):
        self.assertEqual(escape_html('<div>Test & "quote"</div>'),
                         '&lt;div&gt;Test &amp; &quot;quote&quot;&lt;/div&gt;')
        self.assertEqual(escape_html("Tom's book"), 'Tom&#039;s book')
        self.assertEqual(escape_html('No special chars'), 'No special chars')

if __name__ == '__main__':
    unittest.main()
