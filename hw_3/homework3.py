import re
import random
import numpy as np
from pyspark import SparkContext,  SparkConf
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

data = sc.wholeTextFiles('hdfs:///user/reut2-0*')
articles = data.map(lambda x:x[1]).flatMap(lambda x:x.split('<BODY>' or '<body>')[1:]).map(lambda x:x.split('</BODY>' or '</body>')[0]).map(lambda x: re.sub(' +', ' ', x.replace('\n', ' ')))

# 1===============

def shingling(k):
    shingles = articles.map(lambda x:[x[i:i+k] for i in range(len(x)-k+1)]).flatMap(lambda x: x).distinct()
    return shingles

def art_shingle(sh):
    result = []
    for each in article_arr:
        if sh in each:
            result.append(1)
        else:
            result.append(0)
    return result

sh_3 = shingling(3)
article_arr = articles.collect()
shingle_article = sh_3.map(lambda x: art_shingle(x))

shingle_article.coalesce(1, True).saveAsTextFile("shingle_article")

count_article = articles.count()
count_shingles = sh_3.count()

# 2===============

def primeFinder(input):
    i = input + 1
    for each in range(2,i//2 + 1):
        if i%each == 0:
            i+=1
    else:
        return i

def minhash_init(input):
    result = list()
    for each in input:
        if each == 0:
            each = 9999999
        result.append(each)
    return result

def hash(x, a, b, p, N):
    return ((a*x+b)%p)%N

prime = primeFinder(count_shingles)

def apply_hash(input, a, b):
    result = list()
    for each in input[0]:
        if each == 1:
            each = hash(input[1], a, b, prime, count_shingles)
        result.append(each)
    return result

count_hash = 100
prime = primeFinder(count_shingles)
rand = [random.randrange(1,count_hash*5) for i in range(count_hash*2)]
minHash = list()
for i in range(count_hash):
    hash_article = shingle_article.map(lambda x: minhash_init(x)).zipWithIndex().map(lambda x: apply_hash(x, rand[i*2], rand[i*2+1])).reduce(lambda x,y: [xi if xi<yi else yi for xi, yi in zip(x,y)])
    minHash.append(hash_article)

with open('minHash.txt', 'w') as mh:
    for row in minHash:
        mh.write(str(row) + '\n')

# 3===============

count_band = 10
count_r = int(count_hash/count_band)
count_bucket = count_article
s = 0.8
hash_function = [[random.randint(1,100) for i in range(count_r+1)] for j in range(count_band)]

with open('candidate.txt', 'w') as can:
    for i in range(count_article):
        candidatePairs = list()
        for j in range(count_band):
            band = np.array(minHash[j*count_r:j*count_r+count_r]).T
            band = [(np.array(article).dot(np.array(hash_function[j][:count_r]))+hash_function[j][-1])%count_bucket for article in band]
            for k, article in enumerate(band):
                if k>i and article == band[i]:
                    candidatePairs.append(k)
        candidatePairs = [(article, candidatePairs.count(article)) for article in set(candidatePairs)]
        candidatePairs_t = list()
        for candidatePair in candidatePairs:
            if candidatePair[1]>=count_band:
                candidatePairs_t.append(candidatePair[0])
        can.write('Art.' + str(i) + ': ' + str(candidatePairs_t) + '\n')
