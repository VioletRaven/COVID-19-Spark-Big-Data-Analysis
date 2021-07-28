# coding: utf-8
from pyspark import SparkContext
from datetime import datetime
from calendar import monthrange

# Inizializzo lo Spark Context
sc = SparkContext("local[2]", "query_1")

#Q1 Utilizzando somministrazioni-vaccini-summary-latest.csv e punti-somministrazione-tipologia.csv, per ogni mese solare e per ciascuna area 
#(o regione), calcolare il numero medio di somministrazioni che `e stato effettuato giornalmente in un centro vaccinale generico in quell’area e durante quel mese.
#Considerare i dati a partire dall’1 Gennaio 2021.

# Carico il dataset n°1 somministrazioni-vaccini-summary-latest.csv
# Creo un RDD
data = sc.textFile("hdfs:///user/query/somministrazioni-vaccini-summary-latest.csv", use_unicode=False)
# Carico il dataset n°2 punti-somministrazione-tipologia.csv
# Creo un RDD
data_2 = sc.textFile("hdfs:///user/query/punti-somministrazione-tipologia.csv", use_unicode=False)

#Identifico la prima riga (sia per il dataset n°1 che per il dataset n°2) che contiene le intestazioni e la escludo dall'RDD
header = data.first()
data = data.filter(lambda x: x != header)

header_2 = data_2.first()
data_2 = data_2.filter(lambda x: x != header_2)

#Step 0 Splitto l'RDD del dataset n°1 per il separatore di campo virgola
#Step 1 Filtro l'RDD ottenuto per avere solo i dati compresi tra il 1 Gennaio 2021 e il 30 Giugno 2021
#Step 2 Splitto l'RDD del dataset n°2 per il separatore di campo virgola

data = data.map(lambda line : line.split(',')) \
       .filter(lambda x: x[0].split('-')[0] == '2021') \
       .filter(lambda x: x[0].split('-')[1] < '07')

data_2 = data_2.map(lambda line:line.split(','))

#Dataset n°1 
#Step 3 Mappo con K (Regione, Mese), e V (totale somministrazioni)
#Step 4 Riduco su base chiave con K (Regione, Mese) e V (totale somministrazioni)
#Step 5 Mappo con K (Regione) e V (Mese, totale somministrazioni)
#Step 6 Ordino su base chiave
data = data.map(lambda x: (  ( ((x[11]) ), ( datetime.strptime( x[0][:10], '%Y-%m-%d' ).month ) ), int(x[2] ) ) ) \
                .reduceByKey(lambda a, b: a + b) \
                .map(lambda x: ( ( x[0][0] ) , (x[0][1], x[1] ) ) ) \
                .sortByKey()

#Dataset n°2
#step 7 Mappo con K (Regione) e V(1) per assegnare ad ogni clinica valore 1
#Step 8 Riduco su base chiave con K (Regione) e V (somma tra gli uno) così da contare il numero di centri per regione

clinics = data_2.map(lambda x: ((x[6]), 1)).reduceByKey(lambda x, y: x + y)

#Step 9 Con la funzione di join unisco i due RDD creandone uno unico, 
#ottenendo K (Regione) e V((Mese, totale somministrazioni), numero centri)        
rdd = data.join(clinics)

#Step 10 Mappo con K (Regione) e V (Mese, numero medio di somministrazioni per clinica = totale somministrazioni / numero di centri)
#Step 11 Prendo Mese, Regione, Numero medio
#Step 12 Mappo Mese, Regione, numero medio di somministrazioni effettuato giornalmente in un centro generico = rapporto tra numero medio e i giorni nel mese specifico
#Step 13 Ordino in ordine crescente per i mesi
#Step 14 Ordino in ordine alfabetico le regioni
#Step 15 Mappo trasformando il numero del mese in nome del mese, Regione, numero medio di somministrazioni effettuato giornalmente in un centro generico
rdd = rdd.map(lambda x: ( x[0], ( x[1][0][0], round( x[1][0][1] / x[1][1] ) ) ) ) \
         .map(lambda x: ( x[1][0] , x[0], x[1][1]) ) \
         .map(lambda x: ( x[0],  x[1], round( ( x[2] / monthrange(2021, x[0] ) [1] ) ) ) )\
         .sortBy(lambda x: x[0]) \
         .sortBy(lambda x: x[1]) \
         .map(lambda x: ( datetime.strptime ( str(x[0]),  "%m" ).strftime ( "%B" ) , x[1], x[2]) )

# Salvo il file in hdfs
rdd.coalesce(1).saveAsTextFile("hdfs:///user/query/output_query_1")

sc.stop()
