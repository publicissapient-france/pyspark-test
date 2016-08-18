import unittest

from hamcrest.core.assert_that import assert_that
from pyspark.sql.types import Row

from spark_testing.matchers import *
from spark_testing import sql_context
from spark_testing_tests.matchers import assertion_error_message


class TestDataFrameCountMatcher(unittest.TestCase):

    def setUp(self):
        self.df = sql_context.createDataFrame([
            Row(key='value')
        ])

    def test_dataframes_are_equals(self):
        # When / Then
        assert_that(self.df, is_the_same_as(self.df))

    def test_expected_is_not_a_dataframe(self):
        # Given
        not_a_dataframe = 'test'

        # When
        with self.assertRaises(AssertionError) as ex:
            assert_that(self.df, is_the_same_as(not_a_dataframe))

        # Then
        self.assertEqual(
            str(ex.exception.message),
            'Expected DataFrame is not a DataFrame but <type \'str\'>'
        )

    def test_input_is_not_a_dataframe(self):
        # Given
        not_a_dataframe = 'test'

        # When
        with self.assertRaises(AssertionError) as ex:
            assert_that(not_a_dataframe, is_the_same_as(self.df))

        # Then
        self.assertEqual(
            str(ex.exception.message),
            assertion_error_message('Given DataFrames should be the same', 'Provided DataFrame is a <type \'str\'>')
        )

    def test_dataframe_count_differ(self):
        # Given
        df_2_rows = sql_context.createDataFrame([
            Row(key='value'),
            Row(key='value2')
        ])

        # When
        with self.assertRaises(AssertionError) as ex:
            assert_that(df_2_rows, is_the_same_as(self.df))

        # Then
        self.assertEqual(
            str(ex.exception.message),
            assertion_error_message('Given DataFrames should be the same', 'Counts differ : expected 1, actual 2')
        )

    def test_dataframe_schema_differ(self):
        # Given
        df_other_schema = sql_context.createDataFrame([
            Row(key2='value')
        ])

        # When
        with self.assertRaises(AssertionError) as ex:
            assert_that(df_other_schema, is_the_same_as(self.df))

        # Then
        self.assertEqual(
            str(ex.exception.message),
            assertion_error_message('Given DataFrames should be the same', 'Schemas differ : expected '
                                    'StructType(List(StructField(key,StringType,true))), actual '
                                    'StructType(List(StructField(key2,StringType,true)))')
        )

    def test_dataframe_one_value_differ(self):
        # Given
        df_content_differ = sql_context.createDataFrame([
            Row(key='value2')
        ])

        # When
        with self.assertRaises(AssertionError) as ex:
            assert_that(df_content_differ, is_the_same_as(self.df))

        # Then
        self.assertEqual(
            str(ex.exception.message),
            assertion_error_message(
                'Given DataFrames should be the same',
                'Content differ :\nOnly in provided dataframe : [Row(key=u\'value2\')]\nOnly in expected dataframe : [Row(key=u\'value\')]')
        )
