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
    inputs = '/Users/ruiwang/Downloads/the-movies-dataset/ratings.csv'
    ratings_csv = '/Users/ruiwang/ratings.csv'

    df = spark.read.load(inputs,format="csv", sep=",", header="true",escape='"')
    
    #parse_Name = functions.udf(parse_name,types.StringType())

    new_df = df.select('movieId','rating')
    result = new_df.groupBy('movieId').agg(functions.avg('rating').alias('avg_rating'))
    #result.show(10)

    result.repartition(1).write.csv(ratings_csv,mode='overwrite',header="true")

if __name__ == '__main__':
    main()