#coding: utf-8
from pyspark import SparkContext
from datetime import datetime
from calendar import monthrange

# Start the Spark Context
sc = SparkContext("local[2]", "query_2")

#Q2 
#Using the following dataset --> somministrazioni-vaccini-latest.csv, for all women and each age group/calendar month/region compute the minimum/maximum/mean number of vaccinations administered
#Only consider the age groups where at least two days of vaccination campaign were recorded for each month, consider only data starting from the 1st of January, 2021

#Load the dataset --> somministrazioni-vaccini-latest.csv
data = sc.textFile("hdfs:///user/query/somministrazioni-vaccini-latest.csv", use_unicode=False)

#Remove the header
header = data.first()
data = data.filter(lambda x: x != header)

#Split commas, filter in only values between the 1st of January to the 30s of June 2021, map month/age group/vaccinated women
data = data.map(lambda line : line.split(',')) \
           .filter(lambda x: x[0].split('-')[0] == '2021') \
           .filter(lambda x: x[0].split('-')[1] < '07') \
           .map(lambda x: ( ( ( datetime.strptime( x[0][:10], '%Y-%m-%d' ).month ), #month number
                           str(x[3]), #age group
                           str(x[12])), #region
                           int(x[5]) #vaccinated women
                           )        
           ) 

#Append 1 if vaccines administered are > 1 and 0 otherwise, delete if 0, simply reorder the RDD
data = data.map( lambda x: ( x[0], ( x[1], 1 ) if x[1] > 1 else ( x[1], 0 ) ) ).filter( lambda x: x[1][1] != 0 ).map( lambda x: ( ( x[0] ) , x[1][0] ) )
                                           
#Map min/max/mean number of vaccinated women, sort by alphabetical order and month order, get month name and sort again
data = data.groupByKey().mapValues(lambda x: (min(x), max(x), round(sum(x)/len(x)))) \
                  .sortBy(lambda x: x[0][1]) \
                  .sortBy(lambda x: x[0][0]) \
                  .map(lambda x: ( datetime.strptime(str(x[0][0]), "%m").strftime("%B"), x[0][1], x[0][2], x[1][0], x[1][1], x[1][2] )) \
                  .sortBy(lambda x: x[2])

#Save the file in the HDFS
data.coalesce(1).saveAsTextFile("hdfs:///user/query/output_query_2")

# Coalesce(1) will only create 1 file as output (which is fine for debugging) but this could result in a performance bottleneck

#Stop SparkContext
sc.stop()
