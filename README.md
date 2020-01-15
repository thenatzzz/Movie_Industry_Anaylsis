# CMPT-732 Programming for Big Data I (Simon Fraser University)
# Group members:Nattapat Juthaprachakul, Rui Wang, Siyu Wu, Yihan Lan

# How to Make a Profitable Movie?
The goal of this project is to find out what important features are contributing to a profitable movie. Our goal is to find, preprocess, analyze, and visualize many available movie datasets in order to find important features related to movie profitability.  

The program is written in Pyspark and Python. The visualization tools we used is Tableau.


## Program Requirement
* [Pyspark]: This library provides us with tools to structure and analyse our data before training our model.
* [PysparkML]: PysparkML is an open source Pyspark platform for machine learning containing tools and other resources for the development of ML applications.
* [Matplotlib](https://matplotlib.org/): This library is used to generate plots to analyse the performance of our model.
* [Tableau]: Tableau is powerful visualization software that we are using.


## Datasets (Folder: Dataset)
* Main already complete pre-preprocessed dataset: data_for_map.zip, ml_top_with_index_numerical.csv
* UCI and TMDB dataset that are joined but not yet completely pre-processed :1.cast_past.csv, 2.cast_recent.csv, 3.movie_with_rating_csv.csv
ps. they are extracted from HTML format in UCI(https://archive.ics.uci.edu/ml/datasets/Movie) and downloaded from csv format in TMDB(https://www.kaggle.com/rounakbanik/the-movies-dataset#movies_metadata.csv) which is put into (Folder: Original dataset).


## Main Codes
* To extract, partially pre-process, and join dataset from UCI and TMDB use codes in (Folder: TMDB_edit_code) and (Folder:UCI_edit_code)
* Preprocess code:
  -Use find_unique_feature.py in order
  1.) to find most frequent values in the following features:genres, production_companies, and production_countries and clean/preprocess movie_with_rating_csv.csv ,and
  2.) to find most frequent values in the directors and casts: cast_past.csv and cast_recent.csv

  -Later, Use join_cast_movie_csv.py  join 2 csv files from movie_rating and cast_recent_past

  -(Optional) Use str_to_num_features.py to turn String features in dataset into binary value or numerical value.

*  OR just -Use clean_for_ml.py and clean_movie_with_rating.py for data preprocessing

## Running the program
* Use feature_importance_binary.py or feature_importance_randomforest.py or train_movie.py to train model with Random Forest classifier
* Use plot_movie.py to plot
* Use correlation_coefficient.py to find correlation coefficient among various features

## Web application
* Visualization of result  (Folder: Web)
* Index page path is ~Web/WebContent/index.html

## Plots (Folder: 732_docs_and_report)
