from itertools import count
from pyspark import SparkConf,SparkContext
conf=SparkConf().setMaster('local').setAppName('Wordcount')
sc=SparkContext(conf=conf)

input=sc.textFile('file:///sparkcourse/book.txt')
words=input.flatMap(lambda x: x.split())
wordscount=words.countByValue()


for word,count in wordscount.items():
    cleanWord=word.encode('ascii','ignore')
    if(cleanWord):
        print(cleanWord.decode() + " " + str(count))