from pyspark.sql.types import *
from pyspark.sql import SparkSession

def execute(spark):

	schema = StructType([StructField('date',TimestampType(), True), StructField('county',StringType(),True),  
	                     StructField('state', StringType(), True), StructField('state_fips', StringType(), True), 
	                     StructField('cases',IntegerType(), True), StructField('deaths', IntegerType(), True)])

	df = spark.read.csv("s3a://hiro-covid-datalake/tables/nationwide_cases.csv", header=True, inferSchema=False, schema=schema)

	df.printSchema()

	df.createTempView('nation_case')

	df.write.partitionBy('state').parquet('s3a://hiro-covid-datalake/states/', 'overwrite')

	df.select('*').show(10)


if __name__ == "__main__":
	print('processing')
	spark = SparkSession.builder.appName("Spark Test Script").getOrCreate()
	execute(spark)


