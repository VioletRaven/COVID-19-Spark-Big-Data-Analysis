#coding: utf-8
from pyspark import SparkContext
from datetime import datetime
from calendar import monthrange

# Inizializzo lo Spark Context
sc = SparkContext("local[2]", "query_2")

#Q2 
#Utilizzando somministrazioni-vaccini-latest.csv, per le donne, per ogni fascia anagrafica, per ogni mese solare e per ogni 
#area (o regione), determinare il minimo, il massimo e la media aritmetica del numero di vaccinazioni effettuate nel mese in esame.
#Per la risoluzione della query, considerare le sole fasce anagrafiche per cui nel mese solare in esa- me vengono registrati almeno 
#due giorni di campagna vaccinale. Considerare i dati a partire dall’1 Gennaio 2021.
#Esempio di output:
#Gennaio, 20-29, Lazio, 20, 50, 42.8 (assumendo che 20 sia il minimo, 50 il massimo e 42.8 il numero medio di vaccinazioni effettuate nel mese di Gennaio dalla regione Lazio per la fascia d’eta 20-29)
#Gennaio, 30-39, Lazio, 30, 60, 51.7 (assumendo che 30 sia il minimo, 60 il massimo e 51.7 il numero medio di vaccinazioni effettuate nel mese di Gennaio dalla regione Lazio per la fascia d’eta` 30-39) ...

# Carico il dataset n°1 somministrazioni-vaccini-latest.csv
# Creo un RDD
data = sc.textFile("hdfs:///user/query/somministrazioni-vaccini-latest.csv", use_unicode=False)

#Identifico la prima riga che contiene le intestazioni e la escludo dall'RDD
header = data.first()
data = data.filter(lambda x: x != header)

#Step 0 Splitto l'RDD del dataset n°1 per il separatore di campo virgola
#Step 1 Filtro l'RDD ottenuto per avere solo i dati compresi tra il 1 Gennaio 2021 e il 30 Giugno 2021 (da fare) NO
#Step 3 Mappo mese, fascia d'età, regione, donne vaccinate
data = data.map(lambda line : line.split(',')) \
           .filter(lambda x: x[0].split('-')[0] == '2021') \
           .filter(lambda x: x[0].split('-')[1] < '07') \
           .map(lambda x: ( ( ( datetime.strptime( x[0][:10], '%Y-%m-%d' ).month ), #mese a numero
                           str(x[3]), #fascia d'età
                           str(x[12])), #regione
                           int(x[5]) #donne vaccinate
                           )        
           ) 

# creo un indice 1 se i vaccini sono > 1 e 0 altrimenti, elimino se 0

data = data.map( lambda x: ( x[0], ( x[1], 1 ) if x[1] > 1 else ( x[1], 0 ) ) ).filter( lambda x: x[1][1] != 0 ).map( lambda x: ( ( x[0] ) , x[1][0] ) )
                                           
#Mappo i valori (minimo massimo e numero medio)
#Ordino in ordine alfabetico e in ordine dei mesi
#trasformo il numero del mese in nome del mese
# sorto 

data = data.groupByKey().mapValues(lambda x: (min(x), max(x), round(sum(x)/len(x)))) \
                  .sortBy(lambda x: x[0][1]) \
                  .sortBy(lambda x: x[0][0]) \
                  .map(lambda x: ( datetime.strptime(str(x[0][0]), "%m").strftime("%B"), x[0][1], x[0][2], x[1][0], x[1][1], x[1][2] )) \
                  .sortBy(lambda x: x[2])

data.coalesce(1).saveAsTextFile("hdfs:///user/query/output_query_2")

sc.stop()
