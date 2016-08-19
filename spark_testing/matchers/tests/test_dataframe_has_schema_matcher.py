import unittest

from hamcrest.core.assert_that import assert_that
from pyspark.sql.types import Row, StructType, StructField, IntegerType, StringType

from spark_testing.matchers import *
from spark_testing import sql_context
from spark_testing.matchers.tests import assertion_error_message


class TestDataFrameSchemaMatcher(unittest.TestCase):

    def setUp(self):
        self.schema_1_field = StructType(fields=[
            StructField(name='field1', dataType=IntegerType())
        ])
        self.schema_2_fields = StructType(fields=[
            StructField(name='field1', dataType=IntegerType()),
            StructField(name='field2', dataType=StringType())
        ])

        self.df = sql_context.createDataFrame([
            Row(field1=42, field2='value2')
        ], schema=self.schema_2_fields)

    def test_has_the_right_schema(self):
        assert_that(self.df, has_schema(self.schema_2_fields))

    def test_has_the_wrong_schema(self):
        # When
        with self.assertRaises(AssertionError) as ex:
            assert_that(self.df, has_schema(self.schema_1_field))

        # Then
        self.assertEqual(
            str(ex.exception.message),
            assertion_error_message(
                'Given DataFrame has schema %s' % self.schema_1_field,
                'has schema %s' % self.schema_2_fields
            )
        )

    def test_dataframe_has_the_wrong_type(self):
        # Given
        not_a_dataframe = "Test"

        # When
        with self.assertRaises(AssertionError) as ex:
            assert_that(not_a_dataframe, has_schema(self.schema_2_fields))

        # Then
        self.assertEqual(
            str(ex.exception.message),
            assertion_error_message(
                'Given DataFrame has schema %s' % self.schema_2_fields,
                '<type \'str\'> is not a DataFrame'
            )
        )

    def test_expected_has_the_wrong_type(self):
        # Given
        not_a_schema = "Test"

        # When
        with self.assertRaises(AssertionError) as ex:
            assert_that(self.df, has_schema(not_a_schema))

        # Then
        self.assertEqual(
            str(ex.exception.message),
            'Provided schema is not a StructType but <type \'str\'>'
        )