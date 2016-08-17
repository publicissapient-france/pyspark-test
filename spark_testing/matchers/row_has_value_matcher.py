from hamcrest.core.base_matcher import BaseMatcher
from pyspark.sql.types import Row


class RowValueMatcher(BaseMatcher):
    """
    Internal use only
    """

    def __init__(self, expected_value):
        self.expected_value = expected_value

    def _matches(self, item):
        if not isinstance(item, RowField):
            return False
        return item.value == self.expected_value

    def describe_to(self, description):
        description.append_text('Given row should contain \'%s\'' % self.expected_value)

    def describe_mismatch(self, item, mismatch_description):
        if not isinstance(item, RowField):
            mismatch_description.append_text('Wrong assertion usage. ')
            mismatch_description.append_text('Should be asertThat(Field(field_name).of(row), has_value(expected_value)')

        elif item.column_found:
            mismatch_description.append_text('It contains value \'%s\'' % item.value)
        else:
            mismatch_description.append_text('Requested column \'%s\' not found' % item.column_name)


class RowField:
    """
    Internal use only
    """
    def __init__(self, column_name, column_found=True, value=None):
        self.column_name = column_name
        self.column_found = column_found
        self.value = value


class Field:
    """
    Left part of the row_has_value_matcher.has_value() hamcrest matcher
    """
    def __init__(self, column_name):
        self.column_name = column_name

    def of(self, row):
        assert isinstance(row, Row)
        has_column=(self.column_name in row.asDict().keys())
        if has_column:
            return RowField(column_name=self.column_name, value=row[self.column_name])
        else:
            return RowField(column_name=self.column_name, column_found=False)


def has_value(expected_value):
    """
    Hamcrest matcher to check a specific value in a RDD's row.

    Usage : assertThat(Field(field_name).of(row), has_value(expected_value))

    :param expected_value: the value that should be in the specific column of the provided row
    :return: the matcher
    """
    return RowValueMatcher(expected_value)
