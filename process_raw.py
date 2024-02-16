from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()