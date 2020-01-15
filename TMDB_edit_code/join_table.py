import sys
import json
import re
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('cast&crew').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+
sc = SparkContext

def main():
    movie_csv = '/Users/ruiwang/movie_meta.csv'
    #cast_csv = '/Users/ruiwang/cast.csv'
    rating_csv = '/Users/ruiwang/rating.csv'
    movie_with_rating = '/Users/ruiwang/movie_with_rating.csv'

    movie_df = spark.read.load(movie_csv,format="csv", sep=",", header="true",escape='"')
    #cast_df = spark.read.load(cast_csv,format="csv", sep=",",Schema=read_schema, header="true",escape='"')
    rating_df = spark.read.load(rating_csv,format="csv", sep=",", header="true",escape='"')

    movie_df_with_rating = movie_df.join(rating_df,rating_df['movieId'] == movie_df['id'],"left").drop(rating_df['movieId'])

    movie_df_with_rating.repartition(1).write.csv(movie_with_rating,mode='overwrite',header="true",escape='"')
    
if __name__ == '__main__':
    main()