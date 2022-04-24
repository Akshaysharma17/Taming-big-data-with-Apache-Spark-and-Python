from matplotlib import lines
from pyparsing import line
from pyspark import SparkConf, SparkContext

conf=SparkConf().setMaster('local').setAppName("MinTemperatures")
sc=SparkContext(conf=conf)

def parseline(lines):
    fields=lines.split(',')
    stationID=fields[0]
    entryType=fields[2]
    temperature=float(fields[3])*0.1*(9.0/5.0)+32.0
    return (stationID,entryType,temperature)


lines=sc.textFile('file:///sparkcourse/1800.csv')
parsedlines=lines.map(parseline)
maxtemp=parsedlines.filter(lambda x: 'TMAX' in x[1])
stationTemps=maxtemp.map(lambda x: (x[0],x[2]))
stationTemp=stationTemps.reduceByKey(lambda x,y:max(x,y))
results=stationTemp.collect()

a=0
for result in results:
    print(result[0]+"\t{:.2f}F".format(result[1]))
    a=a+1
print("Number of stations: ",a)

