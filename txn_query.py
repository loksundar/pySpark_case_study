from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from google.cloud import bigquery
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
txn_df = txn_df.withColumn('date_time',to_timestamp(col("date_of_visit")))
txn_df = txn_df.withColumn('day',date_format(col("date_of_visit"), "EEEE"))
txn_df.show(5)
client = bigquery.Client()
txn_df.createOrReplaceTempView("txn")
df = spark.sql("""select day,count(visit_number) as num_of_visits from txn group by day order by num_of_visits desc""")
table_id = 'retail-immersion.bqtest.visits_by_weekday'
client.delete_table(table_id, not_found_ok=True)
df.write.format('bigquery') \
  .option('table', 'retail-immersion:bqtest.visits_by_weekday') \
  .save()
df = spark.sql("""select channel,count(visit_number) num_of_visits from txn group by channel order by num_of_visits desc""")
table_id = 'retail-immersion.bqtest.visits_per_channel'
client.delete_table(table_id, not_found_ok=True)
df.write.format('bigquery') \
  .option('table', 'retail-immersion:bqtest.visits_per_channel') \
  .save()
table_id = 'retail-immersion.bqtest.visits_per_channel_per_day'
client.delete_table(table_id, not_found_ok=True)
df = spark.sql("""select day,channel,count(visit_number) num_of_visits from txn group by day,channel order by num_of_visits desc""")
df.write.format('bigquery') \
  .option('table', 'retail-immersion:bqtest.visits_per_channel_per_day') \
  .save()
table_id = 'retail-immersion.bqtest.trans_on_brands'
client.delete_table(table_id, not_found_ok=True)
df = spark.sql("""select product_brand,count(txn) num_of_txns from txn group by product_brand order by num_of_txns desc""")
df.write.format('bigquery') \
  .option('table', 'retail-immersion:bqtest.trans_on_brands') \
  .save()
table_id = 'retail-immersion.bqtest.quant_shipped_sold_per_brand'
client.delete_table(table_id, not_found_ok=True)
df = spark.sql("""select a.product_brand, a.total_revenue/b.total_qty unit_price from 
((select product_brand,sum(revenue) total_revenue from txn
group by product_brand
order by total_revenue desc) a inner join
(select product_brand,sum(qty) total_qty from txn
group by product_brand
order by total_qty desc) b on a.product_brand = b.product_brand)""")
df.write.format('bigquery') \
  .option('table', 'retail-immersion:bqtest.quant_shipped_sold_per_brand') \
  .save()
table_id = 'retail-immersion.bqtest.rev_per_day'
client.delete_table(table_id, not_found_ok=True)
df = spark.sql("""select day,sum(revenue) total_revenue from txn group by day order by total_revenue desc""")
df.write.format('bigquery') \
  .option('table', 'retail-immersion:bqtest.rev_per_day') \
  .save()
table_id = 'retail-immersion.bqtest.cust_channel_at_firstvisit'
client.delete_table(table_id, not_found_ok=True)
df = spark.sql("""select channel,count(visit_number) num_of_visits from txn where visit_number=1 group by channel order by num_of_visits desc""")
df.write.format('bigquery') \
  .option('table', 'retail-immersion:bqtest.cust_channel_at_firstvisit') \
  .save()
table_id = 'retail-immersion.bqtest.rev_per_channel'
client.delete_table(table_id, not_found_ok=True)
df = spark.sql("""select channel,sum(revenue) total_revenue from txn group by channel order by total_revenue desc;""")
df.write.format('bigquery') \
  .option('table', 'retail-immersion:bqtest.rev_per_channel') \
  .save()



