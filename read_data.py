from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
spark = SparkSession \
  .builder \
  .master("local") \
  .appName("PySpark Reding file") \
  .getOrCreate()
# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
bucket = "retail_immersion_raw"
spark.conf.set('temporaryGcsBucket', bucket)

# Create data frame
file_path = "gs://retail_immersion_raw/"
key_df = spark.read.csv(file_path+"Keywords data.csv",header=True)
chp_df = spark.read.csv(file_path+"Channel Performance.csv",header=True)
txn_df = spark.read.csv(file_path+"TXN Data.csv",header=True)
key_df = key_df.withColumnRenamed("Bounce Rate",'Bounce_Rate')
key_df = key_df.withColumnRenamed('Pages / Session','Pages_per_Session')
chp_df = chp_df.withColumnRenamed('Landing Page','Landing_Page')

# Saving the data to BigQuery
key_df.write.format('bigquery') \
  .option('table', 'retail-immersion:bqtest.Key_table') \
  .save()
chp_df.write.format('bigquery') \
  .option('table', 'retail-immersion:bqtest.Channel_table') \
  .save()
txn_df.write.format('bigquery') \
  .option('table', 'retail-immersion:bqtest.txn_table') \
  .save()
