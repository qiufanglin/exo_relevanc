# coding: utf-8

# librairies
import sys
import os
import gzip
import pandas as pd
import dask.dataframe as dd
import datetime
import matplotlib.pyplot as plt


# input argument to get working path
working_path = sys.argv[1]
print(f"working_path: {working_path}")
os.chdir(working_path)


def main():

	start_time = datetime.datetime.now()

	file = gzip.open('./datasets/randomized-transactions-202009.psv.gz')
	df_dataset = pd.read_csv(file, sep='|', iterator=True, chunksize=10000)
	#df_dataset = pd.read_csv("../sample_5000.csv", sep=',', iterator=True, chunksize=1000)
	dataset = pd.concat([chunk for chunk in df_dataset])

	# alternatif load data
	#dd_data = dd.read_csv("./datasets/randomized-transactions-202009.psv.gz", sep="|", compression="gzip")
	#dd_data = dd.read_csv("../sample_5000.csv", sep=',')
	#dataset = dd_data.compute()

	dataset = dataset[["code_magasin", "prix", "identifiant_produit"]]

	# top stores
	list_top_stores = top_stores(dataset)
	print("calcul of top stores is done !")

	# top products by store
	top_products_store(dataset, list_top_stores)
	print("calcul of top products is done !")

	# duration
	end_time = datetime.datetime.now()
	duration = end_time - start_time
	print(f"job done ! it takes {duration.seconds//60} mins {duration.seconds%60} sec !")


def top_products_store(dataset, list_top_stores):

	for store in list_top_stores:
		top_produit = dataset[dataset["code_magasin"]==store]\
									.groupby("identifiant_produit")\
									.agg({'identifiant_produit':"count"})

		top_produit.columns = ["qtt"]
		top_produit.reset_index(inplace=True)
		top_produit = top_produit.sort_values(by="qtt", 
										ascending=False)\
									.iloc[:4,:]
		# get CA of products
		top_produit = pd.merge(top_produit,dataset, on=["identifiant_produit"])

		top_produit = top_produit.groupby("identifiant_produit")\
										.agg({'prix':["sum","count"]})

		top_produit.reset_index(inplace=True)
		top_produit.columns = ["identifiant_produit", "ca", "qtt"]
		top_produit["code_magasin"] = store
		top_produit =  top_produit\
								.sort_values(by="qtt", 
								ascending=False)
		top_produit = top_produit[["code_magasin", "identifiant_produit", "ca", "qtt"]]
			
		products_directory = 'top-products-by-store'
		if os.path.exists("results/" + products_directory)==False:
			os.mkdir("results/" + products_directory)
		
		top_produit.to_csv("results/" + products_directory + str('/top-100-products-store-') + str(store)+'.csv', sep='|', index=False, mode='w')


def top_stores(dataset):
	# top magain by ca
	df_top_stores = dataset.groupby("code_magasin")\
						.agg({"prix":"sum"})\
						.sort_values(by=["prix"], 
									ascending=False)\
						.iloc[:50,:]

	# reset index and rename columns
	df_top_stores.reset_index(inplace=True)
	df_top_stores.columns = ["code_magasin", "ca"]

	# save locally
	df_top_stores.to_csv("results/top-50-stores.csv", sep="|", index=False, mode="w")

	list_top_stores = df_top_stores["code_magasin"].unique().tolist()
	return list_top_stores


if __name__ == "__main__":

	main()
