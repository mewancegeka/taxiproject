from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

file_path = "/mnt/taxi/yellow.parquet"

df = (spark
      .read
      .parquet(file_path)
      )

df = (df
      .withColumn("pickup_date",to_date("tpep_pickup_datetime"))
      .withColumn("travel_time",col("tpep_dropoff_datetime")-col("tpep_pickup_datetime"))
      .withColumn("taxi_type",lit("yellow"))
      )

output_directory = "/mnt/silver/taxi"
df.write.mode("overwrite").partitionBy("pickup_date","taxi_type").parquet(output_directory)
