from hamcrest.core.base_matcher import BaseMatcher
from pyspark.sql.dataframe import DataFrame


class DataFrameCountMatcher(BaseMatcher):

    def __init__(self, expected_count):
        assert isinstance(expected_count, int), 'Provided count is not an int but %s' % type(expected_count)
        self.expected_count = expected_count

    def _matches(self, item):
        if not isinstance(item, DataFrame):
            return False
        return item.count() == self.expected_count

    def describe_to(self, description):
        description.append_text('Given DataFrame has count %d' % self.expected_count)

    def describe_mismatch(self, item, mismatch_description):
        if isinstance(item, DataFrame):
            mismatch_description.append_text('has count %d' % item.count())
        else:
            mismatch_description.append_text('%s is not a DataFrame' % type(item))


def has_count(expected_count):
    """
    Hamcrest matcher to check a DataFrame count.

    Usage : assertThat(dataframe, has_count(expected_count))

    :param expected_count: the count the dataframe should have
    :return: the matcher
    """
    return DataFrameCountMatcher(expected_count)