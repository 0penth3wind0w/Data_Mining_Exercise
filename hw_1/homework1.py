# This code is used with hadoop yarn
import numpy as np
from pyspark import SparkContext,  SparkConf
from pyspark.mllib.stat import Statistics
from math import sqrt

raw=sc.textFile("hdfs:///user/hadoop/data/household_power_consumption.txt").map(lambda x: x.split(";"))
title = raw.first()
data = raw.filter(lambda x: x != title ).filter( lambda x: x[2] != "?" and x[3] != "?" and x[4] != "?"and x[5] != "?" ).map(lambda col: ( float(col[2]), float(col[3]), float(col[4]), float(col[5])))

summary = Statistics.colStats(data)

# 1 ==========
print('[' + title[2] + ' ' + title[3] + ' ' + title[4] + ' ' + title[5] + ']')
print(summary.min())
print(summary.max())

# 2 ==========
print(summary.mean())
print(sqrt(summary.variance()[0]),sqrt(summary.variance()[1]),sqrt(summary.variance()[2]),sqrt(summary.variance()[3]),)

# 3 ==========
def minMax_normalize(record, min_value, max_value):
    return (record - min_value) / (max_value - min_value)

mins = summary.min()
maxs = summary.max()

stds = data.map(lambda row: [minMax_normalize(row[0],mins[0],maxs[0]), minMax_normalize(row[1],mins[1],maxs[1]), minMax_normalize(row[2],mins[2],maxs[2]), minMax_normalize(row[3],mins[3],maxs[3])])
# Save the result
stds.saveAsTextFile('/result.txt')