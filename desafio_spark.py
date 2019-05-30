# -*- coding: utf-8 -*- 
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank , rank, col, max
import json

def main():
	conf = SparkConf().setMaster("local[2]").setAppName("Requisitos cognitivo").set("spark.executor.memory", "1g")
	spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()


	'''
	Justificativa da escolha do formato de aquivo: 
	Os formatos Parquet e ORC são duas boas opções, minha escolha pelo ORC se da ao fato de ser melhor nos seguintes aspectos:
	1 - Suporte a evolução do esquema é melhor no ORC
	2 - Compressão é maior no ORC
	'''
	# Inicio Requisito 1 - convercao csv para colunar

	# Lendo arquivo csv para conversao de formato
	df = spark.read.format("csv").option("inferSchema", "false").option("header", "true").load("data/input/load.csv")

	# Gerando arquivo no formato orc
	df.coalesce(1).write.orc("data/output/requisito_1")

	# Fim Requisito 1 - convercao csv para colunar


	# Inicio Requisito 2 - deduplicação - update_date mais recente

	# Utilizando Window para deduplicar
	windowSpec = Window.partitionBy(df['id']).orderBy(df['update_date'].desc())
	df_2 = df.withColumn("rank",rank().over(windowSpec))
	df_3 = df_2.filter("rank=1").drop("rank")

	# Gerando arquivo no formato orc
	df_3.coalesce(1).write.orc("data/output/requisito_2")

	# Fim Requisito 2 - deduplicação - update_date mais recente

	# Apanas para verificar schema anterior ao requisito 3
	#df_3.printSchema()

	# Inicio Requisito 3 - Conversão do tipo de dados deduplicados usando arquivo types_mapping.json

	# Criando dataframe final
	df_final = df_3

	# Lendo arquivo json com os tipos desejados
	with open('config/types_mapping.json') as json_file:  
	    data = json.load(json_file)
	    for d in data:
	    	# Modificando Schema
	        df_final = df_final.withColumn(d,col(d).cast(data[d]))

	# Gerando arquivo no formato orc
	df_final.coalesce(1).write.orc("data/output/requisito_3")
	        
	# Fim Requisito 3 - Conversão do tipo de dados deduplicados usando arquivo types_mapping.json

	# Apanas para verificar schema após conversão
	#df_final.printSchema()

if __name__ == '__main__': 
	main()
