from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext

sc = SparkContext(master='local[*]')
sql_context = SQLContext(sc)