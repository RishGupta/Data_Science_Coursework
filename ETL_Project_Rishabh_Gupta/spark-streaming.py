########################################################
# Script to use Spark Streaming and generating KPI using 
# PySpark and then writing them onto HDFS location
#########################################################


#Importing necessary libraries
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.window import Window

#Functions to calculate the aggregated columns

def get_total_items(items):
	total_items = 0
	for x in items:
		total_items = total_items + x['quantity']
	return total_items

def get_total_cost(type, items):
	total_cost = 0
	if type == "ORDER":
		for x in items:
			total_cost = total_cost + (x['unit_price'] * x['quantity'])
	elif type == "RETURN":
		for x in items:
			total_cost = total_cost - (x['unit_price'] * x['quantity']) 
	return total_cost

def get_is_order(type):
	is_order = 0
	if type == "ORDER":
		is_order = 1
	return is_order

def get_is_return(type):
	is_return = 0
	if type == "RETURN":
		is_return = 1
	return is_return


##Starting Spark Session

spark = SparkSession  \
	.builder  \
	.appName("RetailAnalytics")  \
	.getOrCreate()
spark.sparkContext.setLogLevel('ERROR')	


###Reading Stream rom Kafka location- Server hosted by Upgrad

lines = spark  \
	.readStream  \
	.format("kafka")  \
	.option("kafka.bootstrap.servers","18.211.252.152:9092")  \
	.option("startingOffsets","latest") \
	.option("subscribe","real-time-project")  \
	.load()


#Defining Structure/ Schema of the JSON which needs to be read and converted to DF
schema = StructType() \
	.add("invoice_no",LongType(), True) \
	.add("country", StringType(), True) \
	.add("timestamp", TimestampType(), True) \
	.add("type", StringType(), True) \
	.add("items", ArrayType( StructType([
    StructField("SKU", StringType(), True), 
    StructField("title", StringType(), True), 
    StructField("unit_price", DoubleType(), True), 
    StructField("quantity", IntegerType(), True)
    ])))

#Reading Kafka as string
linesDF = lines.selectExpr("CAST(value AS STRING)")

#Converting to DF
orderStream = linesDF \
	.select( from_json(col("value"),schema).alias("data")) \
	.select("data.*")

#Defining UDFs to be used to aggregate data

add_total_items = udf(get_total_items, IntegerType())
add_total_cost = udf(get_total_cost, DoubleType())
add_is_order = udf(get_is_order, IntegerType())
add_is_return = udf(get_is_return, IntegerType())

#Using UDFs to create additional columns
expandedOrderStream = orderStream \
	.withColumn("total_items", add_total_items(orderStream.items)) \
	.withColumn("total_cost", add_total_cost(orderStream.type, orderStream.items)) \
	.withColumn("is_order", add_is_order(orderStream.type)) \
	.withColumn("is_return", add_is_return(orderStream.type))

#writing required columns from stream to console
query = expandedOrderStream  \
	.select("invoice_no","country","timestamp","total_items","total_cost","is_order","is_return") \
	.writeStream  \
	.outputMode("append")  \
	.format("console")  \
	.option("truncate","false") \
	.trigger(processingTime ="1 minute") \
	.start()
	
#Calculation of Time based KPIs
kpiByTime = expandedOrderStream \
	.withWatermark("timestamp", "1 minute") \
	.groupBy(window("timestamp", "1 minute", "1 minute")) \
	.agg(F.sum("total_cost").alias("total_sale_volume"), \
		F.count("invoice_no").alias("OPM"), \
		(F.sum("total_cost")/F.count("invoice_no")).alias("average_transaction_size"), \
		(F.sum("is_return")/F.count("invoice_no")).alias("rate_of_return") \
		) \
	.select("window","OPM","total_sale_volume","average_transaction_size","rate_of_return")

#Writing Time Based KPIs in HDFS location
queryByTime = kpiByTime \
	.writeStream \
	.format("json") \
	.outputMode("append") \
	.option("truncate","false") \
	.option("path","/tmp/timeonlykpi") \
	.option("checkpointLocation","/tmp/cptime") \
	.trigger(processingTime ="1 minute") \
	.start()

#Calculating Time and Country based KPIs
kpiByTimeCountry = expandedOrderStream \
	.withWatermark("timestamp", "1 minute") \
	.groupBy(window("timestamp", "1 minute", "1 minute"),"country") \
	.agg(F.sum("total_cost").alias("total_sale_volume"), \
		F.count("invoice_no").alias("OPM"), \
		(F.sum("is_return")/F.count("invoice_no")).alias("rate_of_return") \
		) \
	.select("window","country","OPM","total_sale_volume","rate_of_return") 


#Writing Time and Country based KPIs in HDFS Location
queryByTimeCountry = kpiByTimeCountry \
	.writeStream \
	.format("json") \
	.outputMode("append") \
	.option("truncate","false") \
	.option("path","/tmp/countrykpi") \
	.option("checkpointLocation","/tmp/cpcountry") \
	.trigger(processingTime ="1 minute") \
	.start()

#Finally awaiting termination
queryByTimeCountry.awaitTermination()
	
