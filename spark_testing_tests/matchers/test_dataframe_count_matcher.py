import unittest

from hamcrest.core.assert_that import assert_that
from pyspark.sql.types import Row

from spark_testing.matchers import *
from spark_testing import sql_context
from spark_testing_tests.matchers import assertion_error_message


class TestDataFrameCountMatcher(unittest.TestCase):

    def setUp(self):
        self.df = sql_context.createDataFrame([
            Row(key='value'),
            Row(key='value2')
        ])

    def test_has_the_right_count(self):
        assert_that(self.df, has_count(2))

    def test_has_the_wrong_count(self):
        # Given
        wrong_count = 1

        # When
        with self.assertRaises(AssertionError) as ex:
            assert_that(self.df, has_count(wrong_count))

        # Then
        self.assertEqual(
            str(ex.exception.message),
            assertion_error_message('Given DataFrame has count 1', 'has count 2')
        )

    def test_has_the_wrong_type(self):
        # Given
        not_a_dataframe = "Test"

        # When
        with self.assertRaises(AssertionError) as ex:
            assert_that(not_a_dataframe, has_count(2))

        # Then
        self.assertEqual(
            str(ex.exception.message),
            assertion_error_message('Given DataFrame has count 2', '<type \'str\'> is not a DataFrame')
        )

    def test_expected_has_the_wrong_type(self):
        # Given
        not_a_count = "Test"

        # When
        with self.assertRaises(AssertionError) as ex:
            assert_that(self.df, has_count(not_a_count))

        # Then
        self.assertEqual(
            str(ex.exception.message),
            'Provided count is not an int but <type \'str\'>'
        )