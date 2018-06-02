import numpy as np
from pyspark import SparkContext,  SparkConf
from pyspark.mllib.stat import Statistics
from math import sqrt

data=sc.textFile("hdfs:///user/hadoop/data/household_power_consumption.txt").map(\
	lambda x: x.split(";"))
title = data.first()
clean = data.filter(lambda x: x != title ).filter( lambda x: x[2] != "?" and x[3] != "?" and x[4] != "?"\ 	and x[5] != "?" ).map(lambda col: ( float(col[2]), float(col[3]), float(col[4]), float(col[5])))

summary = Statistics.colStats(clean)

print(title[2], title[3], title[4], title[5])
print(summary.min())
print(summary.max())
clean.filter( lambda x: x[0]).count()
clean.filter( lambda x: x[1]).count()
clean.filter( lambda x: x[2]).count()
clean.filter( lambda x: x[3]).count()

min_ga = summary.min()[0]
min_gr = summary.min()[1]
min_v = summary.min()[2]
min_gi = summary.min()[3]

range_ga = summary.max()[0] - min_ga
range_gr = summary.max()[1] - min_gr
range_v = summary.max()[2] - min_v
range_gi = summary.max()[3] - min_gi

print(summary.mean())
print(sqrt(summary.variance()[0]),sqrt(summary.variance()[1]),sqrt(summary.variance()[2]),\
	sqrt(summary.variance()[3]),)

stds = clean.map(lambda col: [(col[0]-min_ga)/range_ga, (col[1]-min_gr)/range_gr,\
	(col[2]-min_v)/range_v ,(col[3]-min_gi)/range_gi])
stds.saveAsTextFile('/usr/lib/spark/data/result.txt')
