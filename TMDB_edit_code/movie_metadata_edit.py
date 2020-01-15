import sys
import json
import re
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('cast&crew').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+
sc = SparkContext

def parse_name(line):
    name_list = []
    if line is not None:
        pattern = re.compile(r'\{.*?\}')
        hour = re.findall(pattern, line)
        if len(hour) >= 1:
            for i in range(len(hour)):
                name_list.append(get_name(hour[i]))
            return name_list
        else:
            return str('null')
    else:
        return str('null')


def get_name(word):
    pattern1 = re.compile(r'\'name\'\:.*?[\'|\"].*[\'|\"]')
    hour1 = re.findall(pattern1, word)
    #print(hour1)
    pattern2 = re.compile(r'[\'|\"][A-Z].*[\'|\"]')
    hour2 = re.findall(pattern2, hour1[0])
    #print(hour2[0])
    if len(hour2) >=1:
        if hour2[0].startswith('"'):
            hour3 = hour2[0].split('"',2)
        else: 
            hour3 = hour2[0].split("'",2)
        #print(hour3[1])
        return hour3[1]
    else:
        return str('null')


def main():
    inputs = '/Users/ruiwang/Downloads/the-movies-dataset/movies_metadata.csv'
    movie_metadata_csv = '/Users/ruiwang/movie_metadata_csv.csv'

    df = spark.read.load(inputs,format="csv",header="true",escape='"')# edit company country genre
    
    parse_Name = functions.udf(parse_name,types.StringType())

    new_df = df.select('id','title',parse_Name(df['production_companies']).alias('production_companies'),
                        parse_Name(df['production_countries']).alias('production_countries'),
                        parse_Name(df['genres']).alias('genres'),'release_date','revenue','budget')
    
    new_df.repartition(1).write.csv(movie_metadata_csv,mode='overwrite',header="true",escape='"')

if __name__ == '__main__':
    main()