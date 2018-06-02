import numpy as np
from pyspark import SparkContext,  SparkConf
from pyspark.mllib.stat import Statistics
from math import sqrt

#(1)----------------------------------------------------------------------------------------------

newsdata = spark.read.csv("hdfs:///user/hadoop/data/hw2/News_Final.csv", header = True).rdd

def write_rdd_to_file(filename, rddobj):
    f = open(filename,'a')
    for each in rddobj.collect():
        f.write(str(each)+'\n')
    f.close()

#total-------------------------------------------

title = newsdata.map(lambda row: row[1] ).flatMap(lambda line: line.split(' ')).map(lambda word:(word, 1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x:x[1], False)
write_rdd_to_file("freq_title.txt", title)

headline = newsdata.map(lambda row: row[2] ).filter(lambda x: type(x)==str).flatMap(lambda line: line.split(' ')).map(lambda word:(word, 1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x:x[1], False)
write_rdd_to_file("freq_headline.txt", headline)

#daily--------------------------------------------

dtitle = newsdata.filter(lambda data: data[5]!='obama' and data[5]!='economy' and data[5]!='microsoft' and data[5]!='palestine')\
                    .map(lambda row: (row[5], row[1]))\
                    .flatMap(lambda r: [item + " " + str(r[0])[:10] for item in str(r[1]).split()])\
                    .map(lambda wrecord: (wrecord,1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x:x[1], False)
write_rdd_to_file("freq_dtitle.txt", dtitle)


dheadline = newsdata.filter(lambda data: data[5]!='obama' and data[5]!='economy' and data[5]!='microsoft' and data[5]!='palestine')\
                        .map(lambda row: (row[5], row[2]))\
                        .flatMap(lambda r: [item + " " + str(r[0])[:10] for item in str(r[1]).split()])\
                        .map(lambda wrecord: (wrecord,1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x:x[1], False)
write_rdd_to_file("freq_dheadline.txt", dheadline)

#topic-------------------------------------------
#encode error in python2 .encode("utf-8")

ttitle =  newsdata.filter(lambda s: s[4]=='obama' or s[4]=='economy' or s[4]=='microsoft' or s[4]=='palestine')\
                    .map(lambda row: (row[4], row[1]) )\
                    .flatMap(lambda r: [item + " " + str(r[0]) for item in str(r[1]).split()])\
                    .map(lambda wrecord: (wrecord,1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x:x[1], False)
write_rdd_to_file("freq_ttitle.txt", ttitle)

theadline =  newsdata.filter(lambda s: s[4]=='obama' or s[4]=='economy' or s[4]=='microsoft' or s[4]=='palestine')\
                        .map(lambda row: (row[4], row[2]) )\
                        .flatMap(lambda r: [item + " " + str(r[0]) for item in str(r[1]).split()])\
                        .map(lambda wrecord: (wrecord,1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x:x[1], False)
write_rdd_to_file("freq_theadline.txt", theadline)

#(2)----------------------------------------------------------------------------------------------

fb_eco= spark.read.csv("hdfs:///user/hadoop/data/hw2/Facebook_Economy.csv", header = True).rdd
fb_mic= spark.read.csv("hdfs:///user/hadoop/data/hw2/Facebook_Microsoft.csv", header = True).rdd
fb_oba= spark.read.csv("hdfs:///user/hadoop/data/hw2/Facebook_Obama.csv", header = True).rdd
fb_pal= spark.read.csv("hdfs:///user/hadoop/data/hw2/Facebook_Palestine.csv", header = True).rdd
gp_eco= spark.read.csv("hdfs:///user/hadoop/data/hw2/GooglePlus_Economy.csv", header = True).rdd
gp_mic= spark.read.csv("hdfs:///user/hadoop/data/hw2/GooglePlus_Microsoft.csv", header = True).rdd
gp_oba= spark.read.csv("hdfs:///user/hadoop/data/hw2/GooglePlus_Obama.csv", header = True).rdd
gp_pal= spark.read.csv("hdfs:///user/hadoop/data/hw2/GooglePlus_Palestine.csv", header = True).rdd
li_eco= spark.read.csv("hdfs:///user/hadoop/data/hw2/LinkedIn_Economy.csv", header = True).rdd
li_mic= spark.read.csv("hdfs:///user/hadoop/data/hw2/LinkedIn_Microsoft.csv", header = True).rdd
li_oba= spark.read.csv("hdfs:///user/hadoop/data/hw2/LinkedIn_Obama.csv", header = True).rdd
li_pal= spark.read.csv("hdfs:///user/hadoop/data/hw2/LinkedIn_Palestine.csv", header = True).rdd

def write_four_array(filename, oba, eco, mic, pal, pf_name):
    file = open(filename, 'a')
    file.write(pf_name+"obama\n")
    for each in oba:
        file.write('\t'+str(each)+'\n')
    file.write(pf_name+"economy\n")
    for each in eco:
        file.write('\t'+str(each)+'\n')
    file.write(pf_name+"microsoft\n")
    for each in mic:
        file.write('\t'+str(each)+'\n')
    file.write(pf_name+"palestine\n")
    for each in pal:
        file.write('\t'+str(each)+'\n')
    file.close()

#hourly----------------------------------------

hfbeco = []
hfbmic = []
hfboba = []
hfbpal = []
hgpeco = []
hgpmic = []
hgpoba = []
hgppal = []
hlieco = []
hlimic = []
hlioba = []
hlipal = []

for i in range(48):
    hfeco = fb_eco.map(lambda row: ("h"+str(i+1),int( row[i*3+1])+int(row[i*3+2])+int(row[i*3+3]))).reduceByKey(lambda x,y: x+y).first()
    hfbeco.append(hfeco)
    hfmic = fb_mic.map(lambda row: ("h"+str(i+1),int( row[i*3+1])+int(row[i*3+2])+int(row[i*3+3]))).reduceByKey(lambda x,y: x+y).first()
    hfbmic.append(hfmic)
    hfoba = fb_oba.map(lambda row: ("h"+str(i+1),int( row[i*3+1])+int(row[i*3+2])+int(row[i*3+3]))).reduceByKey(lambda x,y: x+y).first()
    hfboba.append(hfoba)
    hfpal = fb_pal.map(lambda row: ("h"+str(i+1),int( row[i*3+1])+int(row[i*3+2])+int(row[i*3+3]))).reduceByKey(lambda x,y: x+y).first()
    hfbpal.append(hfpal)
    hgeco = gp_eco.map(lambda row: ("h"+str(i+1),int( row[i*3+1])+int(row[i*3+2])+int(row[i*3+3]))).reduceByKey(lambda x,y: x+y).first()
    hgpeco.append(hgeco)
    hgmic = gp_mic.map(lambda row: ("h"+str(i+1),int( row[i*3+1])+int(row[i*3+2])+int(row[i*3+3]))).reduceByKey(lambda x,y: x+y).first()
    hgpmic.append(hgmic)
    hgoba = gp_oba.map(lambda row: ("h"+str(i+1),int( row[i*3+1])+int(row[i*3+2])+int(row[i*3+3]))).reduceByKey(lambda x,y: x+y).first()
    hgpoba.append(hgoba)
    hgpal = gp_pal.map(lambda row: ("h"+str(i+1),int( row[i*3+1])+int(row[i*3+2])+int(row[i*3+3]))).reduceByKey(lambda x,y: x+y).first()
    hgppal.append(hgpal)
    hleco = li_eco.map(lambda row: ("h"+str(i+1),int( row[i*3+1])+int(row[i*3+2])+int(row[i*3+3]))).reduceByKey(lambda x,y: x+y).first()
    hlieco.append(hleco)
    hlmic = li_mic.map(lambda row: ("h"+str(i+1),int( row[i*3+1])+int(row[i*3+2])+int(row[i*3+3]))).reduceByKey(lambda x,y: x+y).first()
    hlimic.append(hlmic)
    hloba = li_oba.map(lambda row: ("h"+str(i+1),int( row[i*3+1])+int(row[i*3+2])+int(row[i*3+3]))).reduceByKey(lambda x,y: x+y).first()
    hlioba.append(hloba)
    hlpal = li_pal.map(lambda row: ("h"+str(i+1),int( row[i*3+1])+int(row[i*3+2])+int(row[i*3+3]))).reduceByKey(lambda x,y: x+y).first()
    hlipal.append(hlpal)

write_four_array("fb_hourly.txt", hfboba, hfbeco, hfbmic, hfbpal, "fb_")
write_four_array("gp_hourly.txt", hgpoba, hgpeco, hgpmic, hgppal, "gp_")
write_four_array("li_hourly.txt", hlioba, hlieco, hlimic, hlipal, "li_")

#daily-------------------------------------------

dfbeco = []
dfbmic = []
dfboba = []
dfbpal = []
dgpeco = []
dgpmic = []
dgpoba = []
dgppal = []
dlieco = []
dlimic = []
dlioba = []
dlipal = []

for i in range(2):
    dfeco = fb_eco.map(lambda row: ("d"+str(i+1),int( row[i*24+1])+int(row[i*24+2])+int(row[i*24+3])+int(row[i*24+4])+int(row[i*24+5])+int(row[i*24+6])\
            +int(row[i*24+7])+int(row[i*24+8])+int(row[i*24+9])+int(row[i*24+10])+int(row[i*24+11])+int(row[i*24+12])\
            +int(row[i*24+13])+int(row[i*24+14])+int(row[i*24+15])+int(row[i*24+16])+int(row[i*24+17])+int(row[i*24+18])\
            +int(row[i*24+19])+int(row[i*24+20])+int(row[i*24+21])+int(row[i*24+22])+int(row[i*24+23])+int(row[i*24+24]))).reduceByKey(lambda x,y: x+y).first()
    dfbeco.append(dfeco)
    dfmic = fb_mic.map(lambda row: ("d"+str(i+1),int( row[i*24+1])+int(row[i*24+2])+int(row[i*24+3])+int(row[i*24+4])+int(row[i*24+5])+int(row[i*24+6])\
            +int(row[i*24+7])+int(row[i*24+8])+int(row[i*24+9])+int(row[i*24+10])+int(row[i*24+11])+int(row[i*24+12])\
            +int(row[i*24+13])+int(row[i*24+14])+int(row[i*24+15])+int(row[i*24+16])+int(row[i*24+17])+int(row[i*24+18])\
            +int(row[i*24+19])+int(row[i*24+20])+int(row[i*24+21])+int(row[i*24+22])+int(row[i*24+23])+int(row[i*24+24]))).reduceByKey(lambda x,y: x+y).first()
    dfbmic.append(dfmic)
    dfoba = fb_oba.map(lambda row: ("d"+str(i+1),int( row[i*24+1])+int(row[i*24+2])+int(row[i*24+3])+int(row[i*24+4])+int(row[i*24+5])+int(row[i*24+6])\
            +int(row[i*24+7])+int(row[i*24+8])+int(row[i*24+9])+int(row[i*24+10])+int(row[i*24+11])+int(row[i*24+12])\
            +int(row[i*24+13])+int(row[i*24+14])+int(row[i*24+15])+int(row[i*24+16])+int(row[i*24+17])+int(row[i*24+18])\
            +int(row[i*24+19])+int(row[i*24+20])+int(row[i*24+21])+int(row[i*24+22])+int(row[i*24+23])+int(row[i*24+24]))).reduceByKey(lambda x,y: x+y).first()
    dfboba.append(dfoba)
    dfpal = fb_pal.map(lambda row: ("d"+str(i+1),int( row[i*24+1])+int(row[i*24+2])+int(row[i*24+3])+int(row[i*24+4])+int(row[i*24+5])+int(row[i*24+6])\
            +int(row[i*24+7])+int(row[i*24+8])+int(row[i*24+9])+int(row[i*24+10])+int(row[i*24+11])+int(row[i*24+12])\
            +int(row[i*24+13])+int(row[i*24+14])+int(row[i*24+15])+int(row[i*24+16])+int(row[i*24+17])+int(row[i*24+18])\
            +int(row[i*24+19])+int(row[i*24+20])+int(row[i*24+21])+int(row[i*24+22])+int(row[i*24+23])+int(row[i*24+24]))).reduceByKey(lambda x,y: x+y).first()
    dfbpal.append(dfpal)
    dgeco = gp_eco.map(lambda row: ("d"+str(i+1),int( row[i*24+1])+int(row[i*24+2])+int(row[i*24+3])+int(row[i*24+4])+int(row[i*24+5])+int(row[i*24+6])\
            +int(row[i*24+7])+int(row[i*24+8])+int(row[i*24+9])+int(row[i*24+10])+int(row[i*24+11])+int(row[i*24+12])\
            +int(row[i*24+13])+int(row[i*24+14])+int(row[i*24+15])+int(row[i*24+16])+int(row[i*24+17])+int(row[i*24+18])\
            +int(row[i*24+19])+int(row[i*24+20])+int(row[i*24+21])+int(row[i*24+22])+int(row[i*24+23])+int(row[i*24+24]))).reduceByKey(lambda x,y: x+y).first()
    dgpeco.append(dgeco)
    dgmic = gp_mic.map(lambda row: ("d"+str(i+1),int( row[i*24+1])+int(row[i*24+2])+int(row[i*24+3])+int(row[i*24+4])+int(row[i*24+5])+int(row[i*24+6])\
            +int(row[i*24+7])+int(row[i*24+8])+int(row[i*24+9])+int(row[i*24+10])+int(row[i*24+11])+int(row[i*24+12])\
            +int(row[i*24+13])+int(row[i*24+14])+int(row[i*24+15])+int(row[i*24+16])+int(row[i*24+17])+int(row[i*24+18])\
            +int(row[i*24+19])+int(row[i*24+20])+int(row[i*24+21])+int(row[i*24+22])+int(row[i*24+23])+int(row[i*24+24]))).reduceByKey(lambda x,y: x+y).first()
    dgpmic.append(dgmic)
    dgoba = gp_oba.map(lambda row: ("d"+str(i+1),int( row[i*24+1])+int(row[i*24+2])+int(row[i*24+3])+int(row[i*24+4])+int(row[i*24+5])+int(row[i*24+6])\
            +int(row[i*24+7])+int(row[i*24+8])+int(row[i*24+9])+int(row[i*24+10])+int(row[i*24+11])+int(row[i*24+12])\
            +int(row[i*24+13])+int(row[i*24+14])+int(row[i*24+15])+int(row[i*24+16])+int(row[i*24+17])+int(row[i*24+18])\
            +int(row[i*24+19])+int(row[i*24+20])+int(row[i*24+21])+int(row[i*24+22])+int(row[i*24+23])+int(row[i*24+24]))).reduceByKey(lambda x,y: x+y).first()
    dgpoba.append(dgoba)
    dgpal = gp_pal.map(lambda row: ("d"+str(i+1),int( row[i*24+1])+int(row[i*24+2])+int(row[i*24+3])+int(row[i*24+4])+int(row[i*24+5])+int(row[i*24+6])\
            +int(row[i*24+7])+int(row[i*24+8])+int(row[i*24+9])+int(row[i*24+10])+int(row[i*24+11])+int(row[i*24+12])\
            +int(row[i*24+13])+int(row[i*24+14])+int(row[i*24+15])+int(row[i*24+16])+int(row[i*24+17])+int(row[i*24+18])\
            +int(row[i*24+19])+int(row[i*24+20])+int(row[i*24+21])+int(row[i*24+22])+int(row[i*24+23])+int(row[i*24+24]))).reduceByKey(lambda x,y: x+y).first()
    dgppal.append(dgpal)
    dleco = li_eco.map(lambda row: ("d"+str(i+1),int( row[i*24+1])+int(row[i*24+2])+int(row[i*24+3])+int(row[i*24+4])+int(row[i*24+5])+int(row[i*24+6])\
            +int(row[i*24+7])+int(row[i*24+8])+int(row[i*24+9])+int(row[i*24+10])+int(row[i*24+11])+int(row[i*24+12])\
            +int(row[i*24+13])+int(row[i*24+14])+int(row[i*24+15])+int(row[i*24+16])+int(row[i*24+17])+int(row[i*24+18])\
            +int(row[i*24+19])+int(row[i*24+20])+int(row[i*24+21])+int(row[i*24+22])+int(row[i*24+23])+int(row[i*24+24]))).reduceByKey(lambda x,y: x+y).first()
    dlieco.append(dleco)
    dlmic = li_mic.map(lambda row: ("d"+str(i+1),int( row[i*24+1])+int(row[i*24+2])+int(row[i*24+3])+int(row[i*24+4])+int(row[i*24+5])+int(row[i*24+6])\
            +int(row[i*24+7])+int(row[i*24+8])+int(row[i*24+9])+int(row[i*24+10])+int(row[i*24+11])+int(row[i*24+12])\
            +int(row[i*24+13])+int(row[i*24+14])+int(row[i*24+15])+int(row[i*24+16])+int(row[i*24+17])+int(row[i*24+18])\
            +int(row[i*24+19])+int(row[i*24+20])+int(row[i*24+21])+int(row[i*24+22])+int(row[i*24+23])+int(row[i*24+24]))).reduceByKey(lambda x,y: x+y).first()
    dlimic.append(dlmic)
    dloba = li_oba.map(lambda row: ("d"+str(i+1),int( row[i*24+1])+int(row[i*24+2])+int(row[i*24+3])+int(row[i*24+4])+int(row[i*24+5])+int(row[i*24+6])\
            +int(row[i*24+7])+int(row[i*24+8])+int(row[i*24+9])+int(row[i*24+10])+int(row[i*24+11])+int(row[i*24+12])\
            +int(row[i*24+13])+int(row[i*24+14])+int(row[i*24+15])+int(row[i*24+16])+int(row[i*24+17])+int(row[i*24+18])\
            +int(row[i*24+19])+int(row[i*24+20])+int(row[i*24+21])+int(row[i*24+22])+int(row[i*24+23])+int(row[i*24+24]))).reduceByKey(lambda x,y: x+y).first()
    dlioba.append(dloba)
    dlpal = li_pal.map(lambda row: ("d"+str(i+1),int( row[i*24+1])+int(row[i*24+2])+int(row[i*24+3])+int(row[i*24+4])+int(row[i*24+5])+int(row[i*24+6])\
            +int(row[i*24+7])+int(row[i*24+8])+int(row[i*24+9])+int(row[i*24+10])+int(row[i*24+11])+int(row[i*24+12])\
            +int(row[i*24+13])+int(row[i*24+14])+int(row[i*24+15])+int(row[i*24+16])+int(row[i*24+17])+int(row[i*24+18])\
            +int(row[i*24+19])+int(row[i*24+20])+int(row[i*24+21])+int(row[i*24+22])+int(row[i*24+23])+int(row[i*24+24]))).reduceByKey(lambda x,y: x+y).first()
    dlipal.append(dlpal)

write_four_array("fb_daily.txt", dfboba, dfbeco, dfbmic, dfbpal, "fb_")
write_four_array("gp_daily.txt", dgpoba, dgpeco, dgpmic, dgppal, "gp_")
write_four_array("li_daily.txt", dlioba, dlieco, dlimic, dlipal, "li_")

#(3)----------------------------------------------------------------------------------------------

def isnum(data):
    try:
        float(data)
        return True
    except:
        return False

#headline score--------------------------

hsemt =  newsdata.filter(lambda s: s[4]=='obama' or s[4]=='economy' or s[4]=='microsoft' or s[4]=='palestine')\
                       .map(lambda row: (row[4], (float(row[6]),1))).reduceByKey(lambda x,y: [x[0]+y[0],x[1]+y[1]])\
                       .sortBy(lambda x:x[1], False).map(lambda data: (data[0], [data[1][0], data[1][0]/data[1][1]]))
write_rdd_to_file("headline_score.txt", hsemt)

#title score----------------------------------

tsemt =  newsdata.filter(lambda s: s[4]=='obama' or s[4]=='economy' or s[4]=='microsoft' or s[4]=='palestine')\
                      .map(lambda row: (row[4], (float(row[7]),1))).reduceByKey(lambda x,y: [x[0]+y[0],x[1]+y[1]])\
                      .sortBy(lambda x:x[1], False).map(lambda data: (data[0], [data[1][0], data[1][0]/data[1][1]]))
write_rdd_to_file("title_score.txt", tsemt)

#(4)----------------------------------------------------------------------------------------------
def write_matrix(filename, matrix):
    file = open(filename, 'a')
    for each in matrix:
        file.write(str(each)+'\n')
    file.close()

topobatitle =  newsdata.filter(lambda s: s[4]=='obama').map(lambda row: row[1] )\
                            .flatMap(lambda r: [item for item in r.split()]).map(lambda wrecord: (wrecord,1))\
                            .reduceByKey(lambda x,y: x+y).sortBy(lambda x:x[1], False).keys().take(100)
topecotitle =  newsdata.filter(lambda s: s[4]=='economy').map(lambda row: row[1] )\
                            .flatMap(lambda r: [item for item in r.split()]).map(lambda wrecord: (wrecord,1))\
                            .reduceByKey(lambda x,y: x+y).sortBy(lambda x:x[1], False).keys().take(100)
topmictitle =  newsdata.filter(lambda s: s[4]=='microsoft').map(lambda row: row[1] )\
                            .flatMap(lambda r: [item for item in r.split()]).map(lambda wrecord: (wrecord,1))\
                            .reduceByKey(lambda x,y: x+y).sortBy(lambda x:x[1], False).keys().take(100)
toppaltitle =  newsdata.filter(lambda s: s[4]=='palestine').map(lambda row: row[1] )\
                            .flatMap(lambda r: [item for item in r.split()]).map(lambda wrecord: (wrecord,1))\
                            .reduceByKey(lambda x,y: x+y).sortBy(lambda x:x[1], False).keys().take(100)
obatitle = newsdata.filter(lambda s: s[4]=='obama').map(lambda row: row[1] ).collect()
ecotitle = newsdata.filter(lambda s: s[4]=='economy').map(lambda row: row[1] ).collect()
mictitle = newsdata.filter(lambda s: s[4]=='microsoft').map(lambda row: row[1] ).collect()
paltitle = newsdata.filter(lambda s: s[4]=='palestine').map(lambda row: row[1] ).collect()

obatm = np.zeros((100,100))
ecotm = np.zeros((100,100))
mictm = np.zeros((100,100))
paltm = np.zeros((100,100))

for i in range(100):
    for k in range(100):
        if i > k:
            obatm[i][k] = obatm[k][i]
            ecotm[i][k] = ecotm[k][i]
            mictm[i][k] = mictm[k][i]
            paltm[i][k] = paltm[k][i]
        else:
            for title in obatitle:
                if topobatitle[i] in title and topobatitle[k] in title:
                    obatm[i][k] += 1
            for title in ecotitle:
                if topecotitle[i] in title and topecotitle[k] in title:
                    ecotm[i][k] += 1
            for title in mictitle:
                if topmictitle[i] in title and topmictitle[k] in title:
                    mictm[i][k] += 1
            for title in paltitle:
                if toppaltitle[i] in title and toppaltitle[k] in title:
                    paltm[i][k] += 1

write_matrix("title_obama.txt", obatm)
write_matrix("title_economy.txt", ecotm)
write_matrix("title_microsoft.txt", mictm)
write_matrix("title_palestine.txt", obatm)

#---------------------------------------------------

topobahead =  newsdata.filter(lambda s: s[4]=='obama').map(lambda row: row[2] ).filter(lambda x: type(x)==str)\
                            .flatMap(lambda r: [item for item in r.split()]).map(lambda wrecord: (wrecord,1))\
                            .reduceByKey(lambda x,y: x+y).sortBy(lambda x:x[1], False).keys().take(100)
topecohead =  newsdata.filter(lambda s: s[4]=='economy').map(lambda row: row[2] ).filter(lambda x: type(x)==str)\
                            .flatMap(lambda r: [item for item in r.split()]).map(lambda wrecord: (wrecord,1))\
                            .reduceByKey(lambda x,y: x+y).sortBy(lambda x:x[1], False).keys().take(100)
topmichead =  newsdata.filter(lambda s: s[4]=='microsoft').map(lambda row: row[2] ).filter(lambda x: type(x)==str)\
                            .flatMap(lambda r: [item for item in r.split()]).map(lambda wrecord: (wrecord,1))\
                            .reduceByKey(lambda x,y: x+y).sortBy(lambda x:x[1], False).keys().take(100)
toppalhead =  newsdata.filter(lambda s: s[4]=='palestine').map(lambda row: row[2] ).filter(lambda x: type(x)==str)\
                            .flatMap(lambda r: [item for item in r.split()]).map(lambda wrecord: (wrecord,1))\
                            .reduceByKey(lambda x,y: x+y).sortBy(lambda x:x[1], False).keys().take(100)
obahead = newsdata.filter(lambda s: s[4]=='obama').map(lambda row: row[2]).filter(lambda x: type(x)==str).collect()
ecohead = newsdata.filter(lambda s: s[4]=='economy').map(lambda row: row[2]).filter(lambda x: type(x)==str).collect()
michead = newsdata.filter(lambda s: s[4]=='microsoft').map(lambda row: row[2]).filter(lambda x: type(x)==str).collect()
palhead = newsdata.filter(lambda s: s[4]=='palestine').map(lambda row: row[2]).filter(lambda x: type(x)==str).collect()

obahm = np.zeros((100,100))
ecohm = np.zeros((100,100))
michm = np.zeros((100,100))
palhm = np.zeros((100,100))

for i in range(100):
    for k in range(100):
        if i > k:
            obahm[i][k] = obahm[k][i]
            ecohm[i][k] = ecohm[k][i]
            michm[i][k] = michm[k][i]
            palhm[i][k] = palhm[k][i]
        else:
            for head in obahead:
                if topobahead[i] in head and topobahead[k] in head:
                    obahm[i][k] += 1
            for head in ecohead:
                if topecohead[i] in head and topecohead[k] in head:
                    ecohm[i][k] += 1
            for head in michead:
                if topmichead[i] in head and topmichead[k] in head:
                    michm[i][k] += 1
            for head in palhead:
                if toppalhead[i] in head and toppalhead[k] in head:
                    palhm[i][k] += 1

write_matrix("headline_obama.txt", obahm)
write_matrix("headline_economy.txt", ecohm)
write_matrix("headline_microsoft.txt", michm)
write_matrix("headline_palestine.txt", obahm)
