from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
appName = "PySpark txn_data Analysis"
master = "local"
spark = SparkSession \
  .builder \
  .master(master) \
  .appName(appName) \
  .getOrCreate()
# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
bucket = "retail_immersion_raw"
spark.conf.set('temporaryGcsBucket', bucket)

txn_df = spark.read.format('bigquery').option('project','retail-immersion').option('table','retail-immersion:bqtest.txn_table ').load()
#df = spark.read.format('bigquery').option('project','retail-immersion').option('table','bqtest.txn_table ').load()
#df = spark.read.format('bigquery').option('project','retail-immersion').option('table','txn_table ').load()

from pyspark.sql.functions import *
txn_df = txn_df.withColumn('date_of_visit',to_date(txn_df.date_of_visit, 'yyyyMMdd'))
txn_df = txn_df.withColumn('day_Num',date_format(col("date_of_visit"), "d"))
txn_df = txn_df.withColumn('month',month(txn_df.date_of_visit))
txn_df = txn_df.withColumn('date_time',to_timestamp(col("date_of_visit"))).show(5)
txn_df = txn_df.withColumn('day',date_format(col("date_of_visit"), "EEEE"))
txn_df.show(5)

txn_df.createOrReplaceTempView("txn_table")


