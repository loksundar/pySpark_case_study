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
print(df.schema)
df.show()
