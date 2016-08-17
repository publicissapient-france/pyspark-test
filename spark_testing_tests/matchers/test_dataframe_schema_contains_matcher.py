import unittest

from hamcrest.core.assert_that import assert_that
from pyspark.sql.types import Row, StructType, StructField, IntegerType, StringType

from spark_testing.matchers import *
from spark_testing import sql_context
from spark_testing_tests.matchers import assertion_error_message


class TestDataFrameSchemaContainsMatcher(unittest.TestCase):

    def setUp(self):
        self.field1 = StructField(name='field1', dataType=IntegerType())
        self.schema = StructType(fields=[self.field1])

        self.df = sql_context.createDataFrame([
            Row(field1=42)
        ], schema=self.schema)

    def test_contains_the_field(self):
        assert_that(self.df, has_schema_containing_field(self.field1))

    def test_does_not_contain_the_field(self):
        # Given
        name = 'field2'
        data_type = StringType()
        not_in_dataframe_schema = StructField(name=name, dataType=data_type)

        # When
        with self.assertRaises(AssertionError) as ex:
            assert_that(self.df, has_schema_containing_field(not_in_dataframe_schema))

        # Then
        self.assertEqual(
            str(ex.exception.message),
            assertion_error_message(
                'Given DataFrame\'s schema contains %s of type %s' % (name, data_type),
                'Schema only contains %s' % self.schema.names
            )
        )

    def test_dataframe_has_the_wrong_type(self):
        # Given
        not_a_dataframe = "Test"

        # When
        with self.assertRaises(AssertionError) as ex:
            assert_that(not_a_dataframe, has_schema_containing_field(self.field1))

        # Then
        self.assertEqual(
            str(ex.exception.message),
            assertion_error_message(
                'Given DataFrame\'s schema contains %s of type %s' % (self.field1.name, self.field1.dataType),
                '<type \'str\'> is not a DataFrame'
            )
        )

    def test_expected_has_the_wrong_type(self):
        # Given
        not_a_field = "Test"

        # When
        with self.assertRaises(AssertionError) as ex:
            assert_that(self.df, has_schema_containing_field(not_a_field))

        # Then
        self.assertEqual(
            str(ex.exception.message),
            'Provided field is not a StructField but <type \'str\'>'
        )
