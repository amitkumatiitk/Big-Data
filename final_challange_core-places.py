from pyspark import SparkContext
import itertools
 
if __name__=='__main__':
    sc = SparkContext()
    rdd = sc.textFile('hdfs:///data/share/bdm/core-places-nyc.csv')
    header = rdd.first()
    rdd.saveAsTextFile('core-places-nyc')