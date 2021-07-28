# coding: utf-8
from pyspark import SparkContext
from datetime import datetime
from calendar import monthrange

# Start the Spark Context
sc = SparkContext("local[2]", "query_1")

#Q1: Using the following datasets: somministrazioni-vaccini-summary-latest.csv and punti-somministrazione-tipologia.csv, for each calendar month and region 
#compute the mean daily number of vaccinations given in a certain COVID-19 center.
#Consider only data starting from the 1st of January, 2021.

#Load the dataset --> somministrazioni-vaccini-summary-latest.csv creating a RDD
data = sc.textFile("hdfs:///user/query/somministrazioni-vaccini-summary-latest.csv", use_unicode=False)
# Load the dataset --> punti-somministrazione-tipologia.csv creating a RDD
data_2 = sc.textFile("hdfs:///user/query/punti-somministrazione-tipologia.csv", use_unicode=False)

#Remove the header for both RDDs
header = data.first()
data = data.filter(lambda x: x != header)

header_2 = data_2.first()
data_2 = data_2.filter(lambda x: x != header_2)

#Split commas for both the RDDs, filter in only values between the 1st of January to the 30s of June 2021.

data = data.map(lambda line : line.split(',')) \
       .filter(lambda x: x[0].split('-')[0] == '2021') \
       .filter(lambda x: x[0].split('-')[1] < '07')

data_2 = data_2.map(lambda line:line.split(','))

#Map as K (regions and month) and as V (number of vaccinations), reduce by keys K and V, map as K (regions) and as V (month and number of vaccinations), sort by key
data = data.map(lambda x: (  ( ((x[11]) ), ( datetime.strptime( x[0][:10], '%Y-%m-%d' ).month ) ), int(x[2] ) ) ) \
                .reduceByKey(lambda a, b: a + b) \
                .map(lambda x: ( ( x[0][0] ) , (x[0][1], x[1] ) ) ) \
                .sortByKey()

#For the 2nd RDD --> Map as K (regions) and as V(1) to assign ones for each COVID-19 center, sum them to get number of COVID-19 centers by region
clinics = data_2.map(lambda x: ((x[6]), 1)).reduceByKey(lambda x, y: x + y)

#Merge both RDDs to get K (regions) and V((month and number of vaccinations), number of COVID-19 centers)        
rdd = data.join(clinics)

#Map as K(regions) and as V(month, mean number of vaccinations per COVID-19 center), order the RDD as months/regions/mean number of vaccinations per COVID-19 center
#Compute the daily mean for a particular month, sort by month and regions, get the month name as last step
rdd = rdd.map(lambda x: ( x[0], ( x[1][0][0], round( x[1][0][1] / x[1][1] ) ) ) ) \
         .map(lambda x: ( x[1][0] , x[0], x[1][1]) ) \
         .map(lambda x: ( x[0],  x[1], round( ( x[2] / monthrange(2021, x[0] ) [1] ) ) ) )\
         .sortBy(lambda x: x[0]) \
         .sortBy(lambda x: x[1]) \
         .map(lambda x: ( datetime.strptime ( str(x[0]),  "%m" ).strftime ( "%B" ) , x[1], x[2]) )

#Save the file in the HDFS
rdd.coalesce(1).saveAsTextFile("hdfs:///user/query/output_query_1")

# Coalesce(1) will only create 1 file as output (which is fine for debugging) but this could result in a performance bottleneck

#Stop SparkContext
sc.stop()
