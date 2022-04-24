from pyspark import SparkContext, SparkConf
import collections

conf=SparkConf().setMaster("local").setAppName("Ratinghistory")
sc=SparkContext(conf=conf)

lines=sc.textFile("C:/sparkcourse/ml-100k/u.data")
ratings=lines.map(lambda x: x.split()[2])
result=ratings.countByValue()

sortedResults=collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print ("%s %i"%(key,value))