## pyspark-test

# What is pyspark-test

pyspark-test is a set of tools to simplify Spark Unit testing in python. It includes :

- hamcrest matchers to test  pyspark items. With it, you can easily test stuff on DataFrames, RDDs aso.
- a testing SQLContext to avoid writing one each time you create a pyspark project
 
# Hamcrest matchers

## How do matchers work

First import the matcher package, then declare your test as you would write a simple hamcrest test.

```python
from spark_testing.matchers import *

def test_dataframe_count(self):
  assert_that(my_dataframe, has_count(2))
```

## List of available matchers

- Check a dataframe count with has_count
- Check a dataframe schema with has_schema
- Check a dataframe schema contains a certain field with has_schema_containing_field
- Check a given field in a given row has the right value

# Shared SQLContext

To avoid writing it in each of your projects, simply import it from pyspark-test and you're ready to go

```python
from spark_testing import sql_context

schema = StructType(fields=[
  StructField(name='field1', dataType=IntegerType())
])
sql_context.createDataFrame(
  [Row(field1=42)], 
  schema=schema
)
```