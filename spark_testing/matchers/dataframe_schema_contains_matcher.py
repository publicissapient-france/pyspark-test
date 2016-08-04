from hamcrest.core.base_matcher import BaseMatcher
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructField, StructType


class DataFrameSchemaContainsMatcher(BaseMatcher):

    def __init__(self, expected_field):
        assert isinstance(expected_field, StructField), 'Provided field is not a StructField but %s' % type(expected_field)
        self.expected_field = expected_field

    def _matches(self, item):
        if not isinstance(item, DataFrame):
            return False
        return self.expected_field in item.schema.fields

    def describe_to(self, description):
        description.append_text('Given DataFrame\'s schema contains %s of type %s' % (self.expected_field.name, self.expected_field.dataType))

    def describe_mismatch(self, item, mismatch_description):
        if isinstance(item, DataFrame):
            mismatch_description.append_text('Schema only contains %s' % item.schema.names)
        else:
            mismatch_description.append_text('%s is not a DataFrame' % type(item))


def has_schema_containing_field(expected_field):
    """
    Hamcrest matcher to check DataFrame has the right schema.

    Usage : assertThat(dataframe, has_schema_containing_field(expected_field)

    :param expected_schema: the schema the dataframe should have
    :return: the matcher
    """
    return DataFrameSchemaContainsMatcher(expected_field)
