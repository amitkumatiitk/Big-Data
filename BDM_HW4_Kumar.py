from pyspark import SparkContext
import sys
import pandas as pd
import numpy as np
import csv



if __name__=='__main__':
  sc = SparkContext()

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
    final_list = [[x[0],start_date,x[2].split(',')[0].split('[')[1]]]

    for i in range(1,7):
      start_date = start_date + datetime.timedelta(days=1)
      final_list.append([x[0],start_date,x[2].split(',')[i].split(']')[0]])
    return(final_list)


  NYC_CITIES = set(['New York', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island'])

    
  for code,type_rst in zip(codes,type_list):
    restaurants = set(sc.textFile("hdfs:///data/share/bdm/core-places-nyc.csv") \
        .map(lambda x: x.split(',')) \
        .map(lambda x: (x[1], x[9], x[13])) \
        .filter(lambda x: (x[0] in code) and (x[2] in NYC_CITIES)) \
        .map(lambda x: x[0]) \
        .collect())


    
    

