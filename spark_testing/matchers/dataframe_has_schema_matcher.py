from hamcrest.core.base_matcher import BaseMatcher
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructField, StructType


class DataFrameSchemaMatcher(BaseMatcher):

    def __init__(self, expected_schema):
        assert isinstance(expected_schema, StructType), 'Provided schema is not a StructType but %s' % type(expected_schema)
        self.expected_schema = expected_schema

    def _matches(self, item):
        if not isinstance(item, DataFrame):
            return False
        return item.schema == self.expected_schema

    def describe_to(self, description):
        description.append_text('Given DataFrame has schema %s' % self.expected_schema)

    def describe_mismatch(self, item, mismatch_description):
        if isinstance(item, DataFrame):
            mismatch_description.append_text('has schema %s' % item.schema)
        else:
            mismatch_description.append_text('%s is not a DataFrame' % type(item))


def has_schema(expected_schema):
    """
    Hamcrest matcher to check DataFrame has the right schema.

    Usage : assertThat(dataframe, has_schema(expected_schema))

    :param expected_schema: the schema the dataframe should have
    :return: the matcher
    """
    return DataFrameSchemaMatcher(expected_schema)