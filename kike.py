#libs
import sys
sys.path.append('/usr/lib/python3/dist-packages')
import requests
import json
from datetime import date
from datetime import datetime
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Proyexto final').getOrCreate()
sc = spark.sparkContext
#cn api

def getData():
	print("----------------Obteniendo informacion -----------")
	url = "http://45.55.222:8080/api/data"
	response = requests.get(url)
	data = response.text
	parsed = json.loads(data)
	return sc.parallelize(parsed['interruptor']['dato'])
	#return parsed
#op
def getPorcentaje(total,cantidad):
	return (cantidad*100)/total
#main
def main():
	run = True


#arr cicl
	while run:
		arreglo = getData()
		total = arreglo.count()
		datosI1 = arreglo.filter(lambda x: x['nombre'] == "izquierdo").collect()
		datosI2 =  arreglo.filter(lambda x: x['nombre'] == "derecho").collect()
		print("Registros totales:",total)
		print("datos interruptor 1:",len(datosI1))
		print("porcentaje 1:",len(datosI2))
		print(" datos interruptor 2:",getPorcentaje(total,len(datosI1)),"%")
		print(" Porcentaje 2:",getPorcentaje(total,len(datosI2)),"%")
		R = input("Again?\n Continue? 1\nsalir cualquir numero\nR: ")
		if(R!=1):
			run=False

if __name__ == '__main__':
	main()