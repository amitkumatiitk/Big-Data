from pyspark import SparkContext
import sys
import pandas as pd
import numpy as np
import csv
import datetime



if __name__=='__main__':
  
  codes = [['452210','452311'],['445120'],['722410'],['722511'],
          ['722513'],['446110','446191'],['311811','722515'], ['445210', '445220', '445230', '445291', '445292', '445299'],
          ['445110'] ]
  type_list = ['big_box_grocers',
 'convenience_stores',
 'drinking_places',
 'full-service_restaurants',
 'limited-service_restaurants',
 'pharmacies_and_drug_stores',
 'snack_and_bakeries',
 'specialty_food_stores',
 'supermarkets']

def week_day_seq(x):
  start_date = datetime.datetime.strptime(x[1][:10], "%Y-%m-%d")
  final_list = [[start_date,x[2].split(',')[0].split('[')[1]]]

  for i in range(1,7):
    start_date = start_date + datetime.timedelta(days=1)
    final_list.append([start_date,x[2].split(',')[i].split(']')[0]])
  return(final_list)


def non_zero(x):
    if x[3] > 0:
      return x
    else:
      return tuple([x[0],x[1],x[2],0])

def stats(r1,total_l):
  l1= [0]* (total_l - len(r1) )
  for ele in r1:
    l1.append(ele)
  std_l1 = statistics.pstdev(l1)
  median_l1 = statistics.median(list(l1)) 
  low = median_l1 - std_l1
  high = median_l1 + std_l1
  if low < 0:
    low = 0
  if high < 0 :
    high = 0
  return low,median_l1,high

  #NYC_CITIES = set(['New York', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island'])
  
  
for code,type_rst in zip(codes,type_list):
  restaurants = set(sc.textFile("hdfs:///data/share/bdm/core-places-nyc.csv") \
      .map(lambda x: x.split(',')) \
      .map(lambda x: (x[1], x[9], x[13])) \
      .filter(lambda x: (x[1] in code)) \
      .map(lambda x: x[0]) \
      .collect())


  header = sc.parallelize((("year","date" ,"median", "low", "high")))
 

  rdd = sc.textFile("hdfs:///data/share/bdm/weekly-patterns-nyc-2019-2020/*") \
      .map(lambda x: next(csv.reader([x]))) \
      .filter(lambda x: x[1] in restaurants)\
      .map(lambda x: (x[1], x[12], x[16])) \
      .filter(lambda x: (x[1][:10] >= '2019-01-01'))\
      .map(lambda x: week_day_seq(x))\
      .flatMap(lambda x: x)\
      .map(lambda x: (x[0],int(x[1])))\
      .groupByKey()\
      .map(lambda x : (x[0], list(x[1])))\
      .map(lambda x: (x[0],stats(x[1],len(restaurants))))\
      .map(lambda x: (x[0].year,x[0].isoformat()[:10].replace('2019','2020'),x[1][0],x[1][1],x[1][2]))
  
  header.union(rdd).saveAsTextFile(type_rst) 
  
      #.map(lambda x: (x[0],x[1],x[2],x[3]) if x[3]>0 else (x[0],x[1],x[2],0)).saveAsTextFile(type_rst)
 
