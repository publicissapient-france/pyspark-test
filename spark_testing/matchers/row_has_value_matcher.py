from hamcrest.core.base_matcher import BaseMatcher
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import Row


class RowValueMatcher(BaseMatcher):

    def __init__(self, expected_value):
        self.expected_value = expected_value

    def _matches(self, item):
        if not isinstance(item, Row):
            return False
        return

    def describe_to(self, description):
        description.append_text('')

    def describe_mismatch(self, item, mismatch_description):
        mismatch_description.append_text('')


class RowField:
    def __init__(self, column, value):
        self.column = column
        self.value = value


class Field:
    def __init__(self, column_name):
        self.column_name = column_name

    def of(self, row):
        return RowField(self.column_name, row[self.column_name])

#assertThat(Field('field1').of(my_row), has_value('value'))

def has_value(expected_value):
    return RowValueMatcher(expected_value)
