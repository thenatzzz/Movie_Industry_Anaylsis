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
    cast_csv = '/Users/ruiwang/cast(full).csv'
    #rating_csv = '/Users/ruiwang/rating.csv'
    movie_with_rating = '/Users/ruiwang/movie_with_rating_csv.csv'
    recent_actor_csv = '/Users/ruiwang/cast(recent).csv'

    movie_df = spark.read.load(movie_with_rating,format="csv", sep=",", header="true",escape='"')
    cast_df = spark.read.load(cast_csv,format="csv", sep=",", header="true",escape='"')
    #rating_df = spark.read.load(rating_csv,format="csv", sep=",", header="true",escape='"')

    movie_df_with_cast = movie_df.join(cast_df,cast_df['id'] == movie_df['id'],"left").drop(cast_df['id'])
    recent_movie_df = movie_df_with_cast.filter(movie_df_with_cast['release_date']>='2000-01-01')
    #recent_movie_df.show(10)
    recent_actor = recent_movie_df.select('actor_1','actor_2','actor_3','actor_4','actor_5','director','id')
    #recent_actor.show(10)
    recent_actor.repartition(1).write.csv(recent_actor_csv,mode='overwrite',header="true",escape='"')
    
if __name__ == '__main__':
    main()