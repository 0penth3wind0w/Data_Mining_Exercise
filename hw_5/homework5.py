# Initialization of spark session might be different.
import findspark
findspark.init()

import re
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import *

sc = pyspark.SparkContext(appName="hw5")

# Read data
data = sc.textFile('web-Google.txt').filter(lambda x: x[0]!='#').map(lambda x: re.split(r'\t+', x))
# Verify the data
data.take(3)

# define the class for hw5
class QueryRDD():
    def __init__(self, rdd):
        self.rdd = rdd
    def list_in_degree(self):
        return self.rdd.map(lambda x: (x[1], 1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x:x[1], False)
    def list_out_degree(self):
        return self.rdd.map(lambda x: (x[0], 1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x:x[1], False)
    def show_out_node(self, node):
        out_node = self.rdd.filter(lambda x: x[0] == node).map(lambda x: x[1])
        return out_node
    def show_in_node(self, node):
        in_node = self.rdd.filter(lambda x: x[1] == node).map(lambda x: x[0])
        return in_node

rdd = QueryRDD(data)

# (1) ==========
rdd.list_out_degree().coalesce(1).saveAsTextFile('out_degree')

# (2) ==========
rdd.list_in_degree().coalesce(1).saveAsTextFile('in_degree')

# (3) ==========
#Sample result: list of nodes that node 506742 point to
rdd.show_out_node('506742').coalesce(1).saveAsTextFile('506742_to')
rdd.show_out_node('506742').take(5)
# Sample result: list of nodes that point to node 506742
rdd.show_in_node('506742').coalesce(1).saveAsTextFile('to_506742')
rdd.show_in_node('506742').take(5)
