
import unittest
from unittest.mock import patch, Mock
from bot.schedule_manager import get_schedule_link, load_xls_file
import pandas as pd
from io import BytesIO

class TestScheduleFetching(unittest.TestCase):

    @patch('bot.schedule_manager.requests.get')
    def test_get_schedule_link_success(self, mock_get):
        html_content = '''
        <html>
            <body>
                <a href="/schedule/file1.xlsx">Расписание на неделю</a>
            </body>
        </html>
        '''
        mock_get.return_value.text = html_content

        with patch('bot.schedule_manager.SCHEDULE_FILENAME_PATTERN', '.*файл.*|.*Расписание.*'):
            link = get_schedule_link()
            self.assertTrue(link.endswith('file1.xlsx'))

    @patch('bot.schedule_manager.requests.get')
    def test_get_schedule_link_failure(self, mock_get):
        mock_get.return_value.text = "<html><body>No matches here</body></html>"

        with patch('bot.schedule_manager.SCHEDULE_FILENAME_PATTERN', '.*файл.*'):
            link = get_schedule_link()
            self.assertIsNone(link)

    @patch('bot.schedule_manager.requests.get')
    def test_load_xls_file(self, mock_get):
        # создаём фейковый Excel-файл с помощью openpyxl
        df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        buffer = BytesIO()
        df.to_excel(buffer, index=False, engine='openpyxl')
        buffer.seek(0)

        mock_get.return_value.content = buffer.getvalue()

        excel_file, _ = load_xls_file("http://fakeurl.com/fake.xlsx")
        result_df = pd.read_excel(excel_file, engine='openpyxl')

        pd.testing.assert_frame_equal(result_df, df)

if __name__ == '__main__':
    unittest.main()
