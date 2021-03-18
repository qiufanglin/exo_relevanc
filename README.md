# KPIs transaction retail
Cet outil permet de calculer et d'extraire le top 50 des magasins en fonctions de leur chiffres d'affaires et le top des 100 produits les plus demandés sur le mois dans chaque magasin, et quelques graphiques pour visualiser ces deux KPIs.

# environement
* SPARK 3.1.1
* Python3.8.5

# configuration
étape:
* git clone the projet
* lance setup.sh pour créer des dossiers nécessaires au bon fonctionnement de l'outil
* les dataset input doivent être placer dans le dossier datasets/

# lancement de l'outil 
* en python: python ./scripts/transaction_stores_pd.py $PWD
* en pyspark: run ./job.sh dans le repertoire du projet