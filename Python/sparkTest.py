'''
Created on Dec 22, 2014

@author: greg
'''
from pyspark import SparkContext,SparkConf
from operator import add

def extractTime(l):
    date_time=l.split("\t")[2].split(" ")
    if len(date_time)>1:
        time = date_time[1]
        hour=time.split(":")[0]
        return (hour,1)
    else:
        return ("",0)

if __name__ == '__main__':
    master = "hdfs://192.168.5.142:9000"
    sc = SparkContext(appName="searchByTime")
    lines = sc.textFile(master+"/testData/ex-data")
    times = lines.map(extractTime)\
                 .reduceByKey(add).sortByKey()
    
    times.saveAsTextFile(master+"/testData/timesRes")
    sc.stop()