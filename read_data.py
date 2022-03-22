from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from pyspark.sql.functions import *
from google.cloud import bigquery
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
txn_df = spark.read.csv(file_path+"TXN Data.csv",header=True)
"""
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
  """
txn_df = txn_df.withColumn('date_of_visit',to_date(txn_df.date_of_visit, 'yyyyMMdd'))
txn_df = txn_df.withColumn('day_Num',date_format(col("date_of_visit"), "d"))
txn_df = txn_df.withColumn('month',month(txn_df.date_of_visit))
txn_df = txn_df.withColumn('date_time',to_timestamp(col("date_of_visit")))
txn_df = txn_df.withColumn('day',date_format(col("date_of_visit"), "EEEE"))
client = bigquery.Client()
txn_df.createOrReplaceTempView("txn")

table_id = 'retail-immersion.bqtest.TXN_Table2'
client.delete_table(table_id, not_found_ok=True)
df = spark.sql("""select a.product_brand,a.num_of_txns,a.total_revenue/b.total_qty unit_price from 
((select product_brand,count(txn) num_of_txns ,sum(revenue) total_revenue from txn
group by product_brand
order by total_revenue desc) a inner join
(select product_brand,sum(qty) total_qty from txn
group by product_brand
order by total_qty desc) b on a.product_brand = b.product_brand)""")
df.write.format('bigquery') \
  .option('table', 'retail-immersion:bqtest.TXN_Table2') \
  .save()

table_id = 'retail-immersion.bqtest.cust_channel_at_firstvisit'
client.delete_table(table_id, not_found_ok=True)
df = spark.sql("""select channel,count(visit_number) num_of_visits from txn where visit_number=1 group by channel order by num_of_visits desc""")
df.write.format('bigquery') \
  .option('table', 'retail-immersion:bqtest.cust_channel_at_firstvisit') \
  .save()

table_id = 'retail-immersion.bqtest.TXN_Table1'
client.delete_table(table_id, not_found_ok=True)
df = spark.sql("""select channel , day , count(qty) as total_units , sum(revenue) as total_Revenue,count(visit_number) as num_of_visits , sum(txn) as no_of_txns from txn 
group by grouping sets ((channel,day),(channel),(day),()) order by channel""")
df = df.na.fill("ALL")
df.write.format('bigquery') \
  .option('table', 'retail-immersion:bqtest.TXN_Table1') \
  .save()
#####################################################################################################################################
chp_df = spark.read.csv(file_path+"Channel Performance.csv",header=True)
chp_df = chp_df.withColumnRenamed('Landing Page','Landing_Page')
chp_df = chp_df.na.fill(value="0")
chp_df.createOrReplaceTempView("chn")
table_id = 'retail-immersion.bqtest.CHN_Table1'
client.delete_table(table_id, not_found_ok=True)
df = spark.sql("select channel, sum(bounces) as bounces , sum(txn) as no_of_txn,sum(revenue) as total_revenue, count(Cost) as Total_Cost from chn group by channel")
df.write.format('bigquery') \
  .option('table', 'retail-immersion:bqtest.CHN_Table1') \
  .save()
#####################################################################################################################################
key_df = spark.read.csv(file_path+"Keywords data.csv",header=True)
key_df = key_df.withColumnRenamed("Bounce Rate",'Bounce_Rate')
key_df = key_df.withColumnRenamed('Pages / Session','Pages_per_Session')
key_df.createOrReplaceTempView("key")
table_id = 'retail-immersion.bqtest.KEY_Table1'
client.delete_table(table_id, not_found_ok=True)
df = spark.sql("select Bounce_Rate,Revenue/Cost as ROAS,Cost*1000/Clicks as CPM, Sessions*Pages_per_Session as Page_Impressions, Clicks*100/(Sessions*Pages_per_Session) as CTR_N from key")
df.write.format('bigquery') \
  .option('table', 'retail-immersion:bqtest.KEY_Table1') \
  .save()
