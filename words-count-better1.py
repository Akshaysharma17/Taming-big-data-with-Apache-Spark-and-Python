import re
from pyspark import SparkConf,SparkContext
def normalizewords(text):
    return re.compile(r"\W+",re.UNICODE).split(text.lower())

conf=SparkConf().setMaster('local').setAppName('wordcount')
sc=SparkContext(conf=conf)
input=sc.textFile("file:///sparkcourse/book.txt")
words=input.flatMap(normalizewords)
wordscount=words.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
sortedwords=wordscount.map(lambda x: (x[1],x[0])).sortByKey()
results=sortedwords.collect()
for result in results:
    count=str(result[0])
    cleanword=result[1].encode('ascii','ignore')
    if(cleanword):
        print(cleanword.decode()+"\t\t\t\t"+str(count))