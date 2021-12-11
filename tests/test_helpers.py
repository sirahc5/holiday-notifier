import os
import unittest
import json
import sys
import pandas as pd
sys.path.insert(1, os.environ.get('DIR'))
import helpers
import datetime
import sqlite3
import sqlalchemy
from unittest.mock import patch


class HelpersTestCase(unittest.TestCase):

    def test_get_data(self):
        # Mocking using a context manager:
        with patch('helpers.requests.get') as mock_get:
            # Make the mock return a response with status code 200:
            mock_get.return_value.status_code = 200
            # Set function parameters:
            token = os.environ.get('CALENDARIFICTOKEN')
            country = 'uk'
            year = datetime.date.today().strftime('%Y')
            day = datetime.date.today().strftime('%d')
            month = datetime.date.today().strftime('%m')
            today = datetime.date.today().strftime('%Y-%m-%d')
            # Call the function:
            response = helpers.get_data(token, country, year, month, day)

        # Assert that the request-response cycle was completed successfully:
        self.assertEqual(response.status_code, 200)


    def test_json_to_df(self):
        f = open('./tests/test_json_data.json', 'r')
        test_data = json.load(f)
        test_dict = {
            'name': 'test name',
            'description': 'test description',
            'country': 'test country',
            'date': '0000-00-00',
            'holiday_type': 'test holiday type'
        }
        test_df = pd.DataFrame(test_dict,
                               columns=['name', 'description', 'country',
                                        'date', 'holiday_type'],
                                        index=[0])
        # test_df = pd.DataFrame.from_dict(test_dict)
        holidays_df = helpers.json_to_df(test_data)
        f.close()
        pd.testing.assert_frame_equal(holidays_df, test_df)


    def test_check_data_validity(self):
        # Test whether an empty df will make the function return False:
        df = pd.DataFrame()
        primary_key = 'description'
        self.assertEqual(helpers.check_data_validity(df, primary_key), False)

        # Test whether a non-empty df will not make the function return False:
        df = pd.DataFrame([(1, 'a'), (2, 'b')], columns=['x', 'description'])
        primary_key = 'description'
        self.assertNotEqual(helpers.check_data_validity(df, primary_key), False)


        # Test whether the primary key check works if the p.k. isn't unique:
        df = pd.DataFrame([(1, 'a'), (2, 'a')], columns=['x', 'description'])
        primary_key = 'description'
        self.assertRaises(Exception, helpers.check_data_validity, df,
                          primary_key)

        # Test whether nulls can be found:
        df = pd.DataFrame([(1, 'a'), (None, 'b')], columns=['x', 'description'])
        primary_key = 'description'
        self.assertRaises(Exception, helpers.check_data_validity, df,
                          primary_key)


    def test_send_email(self):
        # TODO: Figure out the best way to test send_email, if needed.  
        pass


if __name__ == '__main__':
    unittest.main()
