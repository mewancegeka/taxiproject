from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

file_path = "/mnt/taxi/yellow.parquet"

df = (spark
      .read
      .parquet(file_path))