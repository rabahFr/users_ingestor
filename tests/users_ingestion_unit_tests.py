import re
import unittest


class UsersIngestionUnitTests(unittest.TestCase):
    def test_date_extraction(self):
        # Given
        test_strings = [
            'data-2023-10-01.csv',
            'data-2023-12-31.csv',
            'data-2022-02-15.csv',
        ]

        expected_dates = [
            '2023-10-01',
            '2023-12-31',
            '2022-02-15',
        ]

        extracted_dates = []
        for i, test_string in enumerate(test_strings):
            match = re.match(r'data-(\d{4}-\d{2}-\d{2})\.csv', test_string)
            self.assertIsNotNone(match, f"Regex failed to match: {test_string}")
            extracted_date = match.group(1)
            self.assertTrue(extracted_date, f"Date not extracted from: {test_string}")
            extracted_dates.append(extracted_date)

        self.assertEqual(extracted_dates, expected_dates)


if __name__ == '__main__':
    unittest.main()
