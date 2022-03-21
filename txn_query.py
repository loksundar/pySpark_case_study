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

df = spark.read.format('bigquery').option('project','retail-immersion').option('table','retail-immersion:bqtest.txn_table ').load()
#df = spark.read.format('bigquery').option('project','retail-immersion').option('table','bqtest.txn_table ').load()
#df = spark.read.format('bigquery').option('project','retail-immersion').option('table','txn_table ').load()

df.show()

df.createOrReplaceTempView("txn_table")
df2 = spark.sql("""CREATE OR REPLACE TABLE  txn_table AS
(SELECT
date_of_visit,
EXTRACT(WEEK FROM date_of_visit) AS weekN,
format_datetime('%A',date_of_visit) AS day, Time_Stamp, user_id, visit_number, channel, txn, revenue, product_id,
qty, product_brand, FORMAT_DATE('%B', date_of_visit) AS month, 

    CAST(DIV(EXTRACT(DAY FROM date_of_visit), 7) + 1 AS STRING) 
   AS week_of_month1, 
   
CONCAT(CAST(DIV(EXTRACT(DAY FROM date_of_visit), 7) + 1 AS STRING),"-",FORMAT_DATE('%B', date_of_visit)) AS week_of_month
FROM (
SELECT  PARSE_DATE('%Y%m%d', CAST(date_of_visit AS STRING)) AS date_of_visit,  
 Time_Stamp, user_id, visit_number, 
channel, txn, revenue, product_id, qty, product_brand
  FROM txn_table
))""")
df2.show(3)

