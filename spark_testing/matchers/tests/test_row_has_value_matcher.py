import unittest

from hamcrest.core.assert_that import assert_that
from pyspark.sql.types import Row

from spark_testing.matchers.row_has_value_matcher import has_value, Field
from spark_testing.matchers.tests import assertion_error_message


class TestDataFrameSchemaContainsMatcher(unittest.TestCase):
    def setUp(self):
        self.row = Row(field1=42)

    def test_row_has_value(self):
        assert_that(Field('field1').of(self.row), has_value(42))

    def test_row_has_wrong_value(self):
        # Given
        wrong_value = 51

        # When
        with self.assertRaises(AssertionError) as ex:
            assert_that(Field('field1').of(self.row), has_value(wrong_value))

        # Then
        self.assertEqual(
            str(ex.exception.message),
            assertion_error_message(
                'Given row should contain \'%s\'' % wrong_value,
                'It contains value \'42\''
            )
        )

    def test_row_does_not_contain_column(self):
        # Given
        wrong_row = Row(field2=42)

        # When
        with self.assertRaises(AssertionError) as ex:
            assert_that(Field('field1').of(wrong_row), has_value(42))

        # Then
        self.assertEqual(
            str(ex.exception.message),
            assertion_error_message(
                'Given row should contain \'42\'',
                'Requested column \'field1\' not found'
            )
        )

    def test_wrong_left_part(self):
        # Given
        not_a_rowfield = "Test"

        # When
        with self.assertRaises(AssertionError) as ex:
            assert_that(not_a_rowfield, has_value(42))

        # Then
        self.assertEqual(
            str(ex.exception.message),
            assertion_error_message(
                'Given row should contain \'42\'',
                'Wrong assertion usage. Should be asertThat(Field(field_name).of(row), has_value(expected_value)'
            )
        )
