# coding: utf-8
from pyspark import SparkContext
from datetime import datetime
from calendar import monthrange
from heapq import nlargest

# Inizializzo lo Spark Context
sc = SparkContext("local[2]", "query_3")

# secondo la suddivisione geografica fornita dall'ISTAT le 3 zone d'Italia (nord, centro, sud) sono così definite:

# NORD = Liguria, Lombardia, Piemonte, Valle d'Aosta, Emilia-Romagna, Friuli-Venezia Giulia, Trentino-Alto Adige, Veneto

# CENTRO = Lazio, Marche, Toscana, Umbria

# SUD = Abruzzo, Basilicata, Calabria, Campania, Molise, Puglia, Sardegna, Sicilia

# Carico il dataset n°1 somministrazioni-vaccini-summary-latest.csv
# Creo un RDD
data = sc.textFile("hdfs:///user/query/somministrazioni-vaccini-summary-latest.csv", use_unicode=False)
#Identifico la prima riga che contiene le intestazioni e la elimino dall'RDD
#Step 0 Splitto l'RDD del dataset n°1 per il separatore di campo virgola
#Step 1 Filtro l'RDD ottenuto per avere solo i dati compresi tra il 27 dicembre 2020 e il 30 Giugno 2021
header = data.first()
data = data.filter(lambda x: x != header) \
       .map(lambda line: line.split(',')) \
       .filter(lambda x: x[0].split('-')[1] < '07')

data.collect()

#Step 2 carico il dataset n°2 (popolazione) 
#Step 3 Identifico la prima riga che contiene le intestazioni e la elimino dall'RDD
#Step 4 Splitto l'RDD del dataset n°2 per il separatore di campo virgola
data_2 = sc.textFile("hdfs:///user/query/totale-popolazione.csv", use_unicode=False)

header_2 = data_2.first()

#Step 4 Rimuovo le righe vuote presenti nel dataset n°2
data_2 = data_2.filter(lambda x: x != header_2).filter(lambda x: x != '').map(lambda line : line.split(',')).map(lambda x: (x[0], int(x[1] )))

#Creo variabili delle zone per definire la zona per ogni regione
nord = ['Liguria', 'Lombardia', 'Piemonte', "Valle d'Aosta / Vallee d'Aoste / Vallée d'Aoste ", 'Emilia-Romagna', 'Friuli-Venezia Giulia', 'Provincia Autonoma Trento','Veneto', 'Provincia Autonoma Bolzano / Bozen']
centro = ['Lazio', 'Marche', 'Toscana', 'Umbria']
sud = ['Abruzzo', 'Basilicata', 'Calabria', 'Campania', 'Molise', 'Puglia', 'Sardegna', 'Sicilia']

regions = ['Liguria', 'Lombardia', 'Piemonte', "Valle d'Aosta / Vallee d'Aoste / Vallée d'Aoste", 'Emilia-Romagna', 'Friuli-Venezia Giulia', 
           'Provincia Autonoma Trento','Veneto', 'Lazio', 'Marche', 'Toscana', 'Umbria', 'Provincia Autonoma Bolzano / Bozen',
           'Abruzzo', 'Basilicata', 'Calabria', 'Campania', 'Molise', 'Puglia', 'Sardegna', 'Sicilia']

# Creo una funzione in grado di collocare ogni regione nella sua zona

def region_sorter(data):
  for i in regions:
    if i == data:
      if data in nord:
        return 'Nord'
      elif data in centro:
        return 'Centro'
      else:
        return 'Sud'

#assegno ad ogni regione (K) un'area di riferimento (nord, centro, sud), grazie alla funzione creata precedentemente
#e la popolazione (V)
data_2 = data_2.map(lambda x: ( (x[0]), (region_sorter(x[0]), x[1])))

#regione , prime dosi, riduco per chiave 
data = data.map(lambda x: (  ( str( x[11] ), int( x[5] ) ) ) ).reduceByKey(lambda x, y: x + y)

rdd = data_2.join(data)

#nord emilia 30 
rdd = rdd.map(lambda x: (x[0], x[1][0][0], int(x[1][0][1]), int(x[1][1])))

rdd_percentage = rdd.map(lambda x: (x[1], x[0], round((float(x[3])/float(x[2])) * 100, 2)))

# Mappo percentuale (K), zona-regione (V)
# Ordino dalla percentuale piu alta alla piu bassa 
sorted_rdd = rdd_percentage.map(lambda x:  (x[2],  ( x[0], x[1]  ) ) ) \
                           .sortBy(lambda x: x[0], ascending = False)
# Ragguppo per area
# Flatmap per nlargest (le 3 percentuali piu alte)

final_rdd = sorted_rdd.groupBy(lambda x: x[1][0]) \
                      .flatMap(lambda k: nlargest(3, k[1], key=lambda x: x[0]))


# Mappo per avere output zona, regione, percentuale
final_rdd = final_rdd.map(lambda x: ( x[1][0], x[1][1], str(x[0]) + '%'))

#salvo output in hdfs
final_rdd.coalesce(1).saveAsTextFile("hdfs:///user/query/output_query_3")

sc.stop()
