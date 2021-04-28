from pyspark import SparkContext
import sys
import pandas as pd
import numpy as np


if __name__=='__main__':

  codes = [['452210','452311'],['445120'],['722410'],['722511'],
          ['722513'],['446110','446191'],['311811','722515'], ['445210', '445220', '445230', '445291', '445292', '445299'],
          ['445110'] ]
  type_list = ['Big Box Grocers','Convenience Stores','Drinking Places','Full-Service Restaurants',
                      'Limited-Service Restaurants','Pharmacies and Drug Stores','Snack and Bakeries','Specialty Food Stores',
                      'Supermarkets']


  def week_day_seq(x):
    start_date = datetime.datetime.strptime(x[1][:10], "%Y-%m-%d")
    final_list = [[x[0],start_date,x[2].split(',')[0].split('[')[1]]]

    for i in range(1,7):
      start_date = start_date + datetime.timedelta(days=1)
      final_list.append([x[0],start_date,x[2].split(',')[i].split(']')[0]])
    return(final_list)


  NYC_CITIES = set(['New York', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island'])

  for code,type_rst in zip(codes[3],type_list[3]):
    restaurants = set(sc.textFile('core_poi_ny.csv') \
        .map(lambda x: x.split(',')) \
        .map(lambda x: (x[1], x[9], x[13])) \
        .filter(lambda x: (x[1] in code) and (x[2] in NYC_CITIES)) \
        .map(lambda x: x[0]) \
        .collect())


    results = sc.textFile('nyc_restaurant_pattern.csv') \
        .map(lambda x: next(csv.reader([x]))) \
        .filter(lambda x: x[1] in restaurants)\
        .map(lambda x: (x[1], x[12], x[16])) \
        .filter(lambda x: (x[1][:4] in ['2020','2019']))\
        .map(lambda x: week_day_seq(x) ).collect()

    results = np.asarray(results)
    results = results.reshape(results.shape[0]*7,3)
    results.shape
    df  = pd.DataFrame(list(results),columns = ["ID","Date","counts_customers"])
    df['counts_customers'] = df['counts_customers'].astype('int')
    final_std = df[['Date','counts_customers']].groupby(['Date']).std().reset_index()
    final_std.rename(columns= {'counts_customers':'std'},inplace=True)
    final_median = df[['Date','counts_customers']].groupby(['Date']).quantile().reset_index()
    final_median.rename(columns ={'counts_customers':'median'},inplace=True)
    final = final_std.merge(final_median)
    final['low'] = final['median'] - final['std']
    final['low'] = np.where(final['low']<0,0,final['low'])
    final['high'] = final['median'] + final['std']
    print(type_rst)
    print(final.shape)
    print(final['median'].min())
    print(final['median'].max())
    final.to_csv(type_rst+'.csv')
