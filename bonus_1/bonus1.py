# If you are using pyspark CLI or using Jupyter Notebook as PySpark driver, it is no necessary to initialize findspark and  SparkContext
import findspark
findspark.init()

import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import *

sc = pyspark.SparkContext(appName="bonus_1")
sqlContext = SQLContext(sc)

# Read CSV data
rdd_data = sqlContext.read.csv('Crimes.csv', header = True).rdd
dataframe_data = sqlContext.read.csv('Crimes.csv', header = True)

# 1 ===============
primary_type_sort = rdd_data.map(lambda row: (row[5], 1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], False).collect()

with open('primary_type_sort.txt','w') as r1_1:
    for each in primary_type_sort:
        r1_1.write(str(each)+'\n')

location_description_sort = rdd_data.map(lambda row: (row[7], 1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], False).collect()

with open('location_description_sort.txt','w') as r1_1:
    for each in primary_type_sort:
        r1_1.write(str(each)+'\n')

# 2 ===============
type_location = dataframe_data.groupBy('Location Description','Primary Type').count().orderBy('Location description','count', ascending=False)
type_location.coalesce(1).write.csv('result_2',mode='overwrite',header=True)

# 3 ===============
def extract_street(block):
    return block[8:]
extract_street_udf = udf(extract_street)

street_type = dataframe_data.select('Primary type',col('Block').alias('Street')).withColumn('Street',extract_street_udf('Street')).groupBy('Primary type','Street').count().orderBy('Primary type','count', ascending=False)
street_type.coalesce(1).write.csv('result_3',mode='overwrite',header=True)

# 4 ===============
def extract_hour(date):
    return date[11:13]
extract_hour_udf = udf(extract_hour)

hour_primary = dataframe_data.select('Primary type',col('Date').alias('Hour')).withColumn('Hour',extract_hour_udf('Hour')).groupBy('Primary type','Hour').count().orderBy('Primary type','count','Hour', ascending=False)
hour_primary.coalesce(1).write.csv('result_41',mode='overwrite',header=True)

hour_location = dataframe_data.select('Location description',col('Date').alias('Hour')).withColumn('Hour',extract_hour_udf('Hour')).groupBy('Location description','Hour').count().orderBy('Location description','count','Hour', ascending=False)
hour_location.coalesce(1).write.csv('result_42',mode='overwrite',header=True)