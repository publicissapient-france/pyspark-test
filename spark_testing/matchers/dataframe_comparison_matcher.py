from hamcrest.core.base_matcher import BaseMatcher
from pyspark.sql.dataframe import DataFrame


def compare_lines(left_dataframe, right_dataframe):
    """
    Compare two dataframe's lines. return the rows that differs as a tuple.
    Input dataframes must have the same schema and line count

    :param left_dataframe: left dataframe
    :param right_dataframe: right dataframe
    :return: (is_the_same, rows_only_in_left, rows_only_in_right)
    """
    only_left = [row for row in left_dataframe.collect()
                 if len(filter(lambda dataframe_row: dataframe_row == row, right_dataframe.collect())) == 0]
    only_right = [row for row in right_dataframe.collect()
                  if len(filter(lambda dataframe_row: dataframe_row == row, left_dataframe.collect())) == 0]
    is_the_same = len(only_left) == 0 and len(only_right) == 0
    return is_the_same, only_left, only_right


class DataFrameComparisonMatcher(BaseMatcher):

    def __init__(self, other_dataframe):
        assert isinstance(other_dataframe, DataFrame), 'Expected DataFrame is not a DataFrame but %s' % type(other_dataframe)
        self.other_dataframe = other_dataframe

    def _matches(self, item):
        if not isinstance(item, DataFrame):
            return False
        elif item.count() != self.other_dataframe.count():
            return False
        elif item.schema != self.other_dataframe.schema:
            return False
        else:
            (is_the_same, left_ko, right_ko) = compare_lines(item, self.other_dataframe)
            return is_the_same

    def describe_to(self, description):
        description.append_text('Given DataFrames should be the same')

    def describe_mismatch(self, item, mismatch_description):
        if not isinstance(item, DataFrame):
            mismatch_description.append_text('Provided DataFrame is a %s' % type(item))
        elif item.count() != self.other_dataframe.count():
            mismatch_description.append_text(
                'Counts differ : expected %i, actual %i' % (self.other_dataframe.count(), item.count())
            )
        elif item.schema != self.other_dataframe.schema:
            mismatch_description.append_text(
                'Schemas differ : expected %s, actual %s' % (self.other_dataframe.schema, item.schema)
            )
        else:
            (is_the_same, only_left, only_right) = compare_lines(item, self.other_dataframe)
            if not is_the_same:
                mismatch_description.append_text('Content differ :')
                if len(only_left) > 0:
                    mismatch_description.append_text('\nOnly in provided dataframe : %s' % only_left)
                if len(only_right) > 0:
                    mismatch_description.append_text('\nOnly in expected dataframe : %s' % only_right)


def is_the_same_as(other_dataframe):
    """
    Hamcrest matcher to check two DataFrames are the same. Two DataFrames are considered the same if they have the exact
    same schema (columns in the same order) and if all their lines are identical even if they are in the wrong order.

    Usage : assertThat(dataframe, is_the_same_as(other_dataframe))

    :param other_dataframe: the expected dataframe
    :return: the matcher
    """
    return DataFrameComparisonMatcher(other_dataframe)
