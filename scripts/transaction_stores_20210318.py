# coding: utf-8

# librairies pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as f_s

# librairies python
import sys
import os
import pandas as pd
import datetime
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

# input argument to get working path
working_path = sys.argv[1]
print(f"working_path: {working_path}")
os.chdir(working_path)

# Spark configurations
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# main function
def main():

	sc.setLogLevel("ERROR")

	start_time = datetime.datetime.now()

	# load input dataset
	dataset = spark.read.option("header", "true")\
						.csv("./datasets/randomized-transactions-202009.psv.gz", sep="|")\
						.select(["code_magasin", "prix", "identifiant_produit"])#\
						#.limit(5000)
	#dataset.persist()
	print("input dataset is loaded and cached in memory")


	list_top_stores = top_stores(dataset)
	print("calcul of top stores is done !")

	top_products_store(dataset, list_top_stores)
	print("calcul of top products is done !")

	# duration
	end_time = datetime.datetime.now()
	duration = end_time - start_time

	print(f"job done ! it takes {duration.seconds//60} mins {duration.seconds%60} sec !")


def top_products_store(dataset, list_top_stores):

	dateset_top_stores = dataset.select(["code_magasin","identifiant_produit"])\
									.where(f_s.col("code_magasin").isin(list_top_stores))
	
	# stocke temporary df in memory, disque
	dateset_top_stores.persist()
	
	# check and create products_directory if not, to save files
	products_directory = 'top-products-by-store'
	if os.path.exists('results/' + products_directory)==False:
		os.mkdir("results/" + products_directory)

	dataviz_directory = 'dataviz'
	if os.path.exists('results/' + dataviz_directory)==False:
		os.mkdir('results/' + dataviz_directory)

	for store in list_top_stores:
		top_products = dateset_top_stores.select([ "identifiant_produit"])\
											.where(f_s.col("code_magasin")==store)\
											.groupBy(f_s.col("identifiant_produit"))\
											.agg(f_s.count("*").alias("cnt"))\
											.orderBy("cnt", ascending=False)\
											.withColumn("code_magasin",
													f_s.lit(store))\
											.limit(100)
		df_top_products = top_products.toPandas()
		df_top_products.to_csv("results/" + products_directory + str('/top-100-products-store-') + str(store)+'.csv', sep='|', index=False, mode='w')

		# sort df by ca
		df_top_products.sort_values("cnt", inplace=True)
		df_top_products.fillna("unknown", inplace=True)
		
		# plot kpi quantite top product of store
		fig, ax = plt.subplots(figsize=(20,30))
		plt.barh(df_top_products["identifiant_produit"], df_top_products["cnt"], color='c', alpha=0.65)
		plt.axvline(df_top_products["cnt"].mean(), color='k', linestyle='dashed', linewidth=2)

		for i, v in enumerate(df_top_products["cnt"]):
			plt.text(v + 3, i + .25,  str("{:,}".format(v)), color='grey')

		plt.xticks(fontsize=12)
		plt.yticks(fontsize=12)
		plt.xlabel("amount products", fontsize=14)
		plt.ylabel("products", fontsize=14)
		plt.title("amount of top {} products of store {}".format(df_top_products.shape[0], store), fontsize=16)

		fig.savefig("results/" + dataviz_directory + "/amount-products-store-" + str(store) + ".png")
		plt.close()

	dateset_top_stores.unpersist()


def top_stores(dataset):

	top_stores_by_ca = dataset.select(["code_magasin", "prix"])\
								.groupBy(["code_magasin"])\
								.agg(f_s.sum("prix")\
											.alias("ca"))\
								.orderBy("ca", ascending=False)\
								.limit(50)

	# convert to pandas dataframe
	df_top_stores = top_stores_by_ca.toPandas()
	
	#df_top_stores["code_magasin"] = df_top_stores["code_magasin"].astype("string")
	#df_top_stores["ca"] = df_top_stores["ca"].astype("int")

	# save locally
	df_top_stores.to_csv("results/top-50-stores.csv", sep="|", index=False, mode="w")

	# sort df by ca
	df_top_stores.sort_values("ca", inplace=True)
	
	# plot kpi
	formatter = FuncFormatter(convert_millions)
	
	fig, ax = plt.subplots(figsize=(20,12))
	# plot bar
	plt.barh(df_top_stores["code_magasin"], df_top_stores["ca"], color="c")
	# mean ca
	plt.axvline(df_top_stores["ca"].mean(), color="k", linestyle="dashed", linewidth=3)

	plt.xticks(fontsize=12)
	plt.yticks(fontsize=12)
	plt.xlabel("outcome", fontsize=14)
	plt.ylabel("stores", fontsize=14)
	plt.title(f"outcome of top {df_top_stores.shape[0]} stores", fontsize=16)

	ax.xaxis.set_major_formatter(formatter)
	for i, v in enumerate(df_top_stores["ca"]):
		plt.text(v + 3, i + .25, str("{:,}".format(v)), color="grey")

	dataviz_directory = 'dataviz'
	if os.path.exists('results/' + dataviz_directory)==False:
		os.mkdir('results/' + dataviz_directory)

	fig.savefig("results/" + dataviz_directory + "/top_stores_ca.png")

	# plot distribution ca
	plt.figure(figsize=(12,8))
	plt.hist(df_top_stores["ca"], bins=50, color='c', edgecolor="k")
	plt.xticks(fontsize=12)
	plt.yticks(fontsize=12)
	plt.xlabel("outcome", fontsize=14)
	plt.ylabel("amount stores", fontsize=14)
	plt.title("distribution of outcome of top stores", fontsize=16)
	plt.savefig("results/" + dataviz_directory + "/dist_ca_stores.png")

	# list of top stores
	# list_stores = [row["code_magasin"] for row in top_stores_by_ca.select("code_magasin").collect()]
	list_top_stores = df_top_stores["code_magasin"].unique().tolist()

	return list_top_stores


# subfunctions
def convert_millions(x, pos):
	return "€%1.1fM" % (x*1e-6)


# to run main function
if __name__ == "__main__":

	main()

