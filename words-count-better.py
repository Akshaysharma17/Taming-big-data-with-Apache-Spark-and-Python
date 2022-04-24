import re
from pyspark import SparkConf,SparkContext
def normalizewords(text):
    return re.compile(r"\W+",re.UNICODE).split(text.lower())

conf=SparkConf().setMaster('local').setAppName('wordcount')
sc=SparkContext(conf=conf)
input=sc.textFile("file:///sparkcourse/book.txt")
words=input.flatMap(normalizewords)
wordscount=words.countByValue()
for word,count in wordscount.items():
    cleanword=word.encode('ascii','ignore')
    if(cleanword):
        print(cleanword.decode()+" "+str(count))