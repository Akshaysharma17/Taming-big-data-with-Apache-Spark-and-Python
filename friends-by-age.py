from numpy import average
from pyspark import SparkConf, SparkContext
conf=SparkConf().setMaster('local').setAppName("FriendsbyAge")
sc=SparkContext(conf=conf)

def parseline(line):
    fields=line.split(",")
    age=int(fields[2])
    numFriends=int(fields[3])
    return (age,numFriends)

lines=sc.textFile("file:///sparkcourse/fakefriends.csv")
rdd=lines.map(parseline)
totalsbyage=rdd.mapValues(lambda x:(x,1)).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
averagebyage=totalsbyage.mapValues(lambda x:int(x[0]/x[1]))
sortbyage=averagebyage.sortByKey(lambda x : sorted(x))
results=sortbyage.collect()
for result in results:
    print(result)
