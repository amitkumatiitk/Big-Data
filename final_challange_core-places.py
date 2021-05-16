from pyspark import SparkContext
import itertools
 
if __name__=='__main__':
    sc = SparkContext()
    rdd = sc.textFile('hdfs:///data/share/bdm/core-places-nyc.csv')
    header = rdd.first()
    rdd.sample(False, 1) \
        .coalesce(1) \
        .mapPartitions(lambda x: itertools.chain([header], x)) \
        .saveAsTextFile('core-places-nyc')


