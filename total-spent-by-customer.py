from types import LambdaType
from pyspark import SparkConf,SparkContext

conf=SparkConf().setMaster('local').setAppName('totalspentbycustomer')
sc=SparkContext(conf=conf)

def normalize(lines):
    fields=lines.split(',')
    return (int(fields[0]),float(fields[2]))

lines=sc.textFile('file:///sparkcourse/customer-orders.csv')
input=lines.map(normalize)
totalbycustomer=input.reduceByKey(lambda x,y:x+y).sortByKey()
results=totalbycustomer.collect()

for result in results:
    print(result)