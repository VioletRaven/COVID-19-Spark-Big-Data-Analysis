# coding: utf-8
from pyspark import SparkContext
from datetime import datetime
from calendar import monthrange
from heapq import nlargest

# Start the Spark Context
sc = SparkContext("local[2]", "query_3")

#Get regions by their geographical position (north, south, and center) as defined by ISTAT:

#NORTH = Liguria, Lombardia, Piemonte, Valle d'Aosta, Emilia-Romagna, Friuli-Venezia Giulia, Trentino-Alto Adige, Veneto
#CENTER = Lazio, Marche, Toscana, Umbria
#SOUTH = Abruzzo, Basilicata, Calabria, Campania, Molise, Puglia, Sardegna, Sicilia

#Load the dataset --> somministrazioni-vaccini-summary-latest.csv
#Create the RDD
data = sc.textFile("hdfs:///user/query/somministrazioni-vaccini-summary-latest.csv", use_unicode=False)

#Remove the header, split commas, filter in only values between the 27th of December to the 30s of June 2021
header = data.first()
data = data.filter(lambda x: x != header) \
       .map(lambda line: line.split(',')) \
       .filter(lambda x: x[0].split('-')[1] < '07')

#Load the dataset --> totale-popolazione.csv 
#Remove the header, remove empty lines, split commas, map again
data_2 = sc.textFile("hdfs:///user/query/totale-popolazione.csv", use_unicode=False)
header_2 = data_2.first()
data_2 = data_2.filter(lambda x: x != header_2).filter(lambda x: x != '').map(lambda line : line.split(',')).map(lambda x: (x[0], int(x[1] )))

#Create geographical variables
nord = ['Liguria', 'Lombardia', 'Piemonte', "Valle d'Aosta / Vallee d'Aoste / Vallée d'Aoste ", 'Emilia-Romagna', 'Friuli-Venezia Giulia', 'Provincia Autonoma Trento','Veneto', 'Provincia Autonoma Bolzano / Bozen']
centro = ['Lazio', 'Marche', 'Toscana', 'Umbria']
sud = ['Abruzzo', 'Basilicata', 'Calabria', 'Campania', 'Molise', 'Puglia', 'Sardegna', 'Sicilia']

regions = ['Liguria', 'Lombardia', 'Piemonte', "Valle d'Aosta / Vallee d'Aoste / Vallée d'Aoste", 'Emilia-Romagna', 'Friuli-Venezia Giulia', 
           'Provincia Autonoma Trento','Veneto', 'Lazio', 'Marche', 'Toscana', 'Umbria', 'Provincia Autonoma Bolzano / Bozen',
           'Abruzzo', 'Basilicata', 'Calabria', 'Campania', 'Molise', 'Puglia', 'Sardegna', 'Sicilia']

#Define simple function to sort regions by north/south/center
def region_sorter(data):
  for i in regions:
    if i == data:
      if data in nord:
        return 'Nord'
      elif data in centro:
        return 'Centro'
      else:
        return 'Sud'

#Map regions and assign them to their geographical zone and to their respective population
data_2 = data_2.map(lambda x: ( (x[0]), (region_sorter(x[0]), x[1])))

#Map regions/first vaccination, reduce by both keys 
data = data.map(lambda x: (  ( str( x[11] ), int( x[5] ) ) ) ).reduceByKey(lambda x, y: x + y)

#Merge the two RDDs
rdd = data_2.join(data)

#Map to avoid confusion 
rdd = rdd.map(lambda x: (x[0], x[1][0][0], int(x[1][0][1]), int(x[1][1])))

#Get percentage of vaccinations with respect to population by region
rdd_percentage = rdd.map(lambda x: (x[1], x[0], round((float(x[3])/float(x[2])) * 100, 2)))

#Map percentage as K and zone-region as V, sort by higher percentage 
sorted_rdd = rdd_percentage.map(lambda x:  (x[2],  ( x[0], x[1]  ) ) ) \
                           .sortBy(lambda x: x[0], ascending = False)
#Group by area and get the 3 highest percentages for each zone (north/south/center)
final_rdd = sorted_rdd.groupBy(lambda x: x[1][0]) \
                      .flatMap(lambda k: nlargest(3, k[1], key=lambda x: x[0]))

#Map to order everything in a more intuitive way
final_rdd = final_rdd.map(lambda x: ( x[1][0], x[1][1], str(x[0]) + '%'))

#Save output in HDFS
final_rdd.coalesce(1).saveAsTextFile("hdfs:///user/query/output_query_3")

# Coalesce(1) will only create 1 file as output (which is fine for debugging) but this could result in a performance bottleneck

#Stop SparkContext
sc.stop()
