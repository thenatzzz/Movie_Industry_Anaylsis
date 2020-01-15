import sys
import json
import re
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('cast&crew').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+
sc = SparkContext

read_schema = types.StructType([
    types.StructField('cast', types.StringType()),
    types.StructField('crew', types.StringType()),
    types.StructField('id', types.IntegerType()),
])

def parse_cast_1(line):
    pattern = re.compile(r'\{.*?\}')
    hour = re.findall(pattern, line)
    if len(hour) >= 1:
        return get_name(hour[0])
    else:
        return str('null')

def parse_cast_2(line):
    pattern = re.compile(r'\{.*?\}')
    hour = re.findall(pattern, line)
    if len(hour) >= 2:
        return get_name(hour[1])
    else:
        return str('null')

def parse_cast_3(line):
    pattern = re.compile(r'\{.*?\}')
    hour = re.findall(pattern, line)
    if len(hour) >= 3:
        return get_name(hour[2])
    else:
        return str('null')

def parse_cast_4(line):
    pattern = re.compile(r'\{.*?\}')
    hour = re.findall(pattern, line)
    if len(hour) >= 4:
        return get_name(hour[3])
    else:
        return str('null')

def parse_cast_5(line):
    pattern = re.compile(r'\{.*?\}')
    hour = re.findall(pattern, line)
    if len(hour) >= 5:
        return get_name(hour[4])
    else:
        return str('null')

def parse_director(line):
    pattern = re.compile(r'\'Director\'\, \'name\'\:.*?\'.*?\'')
    hour = re.findall(pattern, line)
    if len(hour) >= 1:
        return get_name(hour[0])
    else:
        return str('null')

def get_name(word):
    pattern1 = re.compile(r'\'name\'\:.*?[\'|\"].*[\'|\"]')
    hour1 = re.findall(pattern1, word)
    pattern2 = re.compile(r'[\'|\"][A-Z].*[\'|\"]')
    hour2 = re.findall(pattern2, hour1[0])
    if len(hour2) >=1:
        if hour2[0].startswith('"'):
            hour3 = hour2[0].split('"',2)
        else: 
            hour3 = hour2[0].split("'",2)
        return hour3[1]
        #return hour2[0]
    else:
        return str('null')


def main():
    inputs = '/Users/ruiwang/Downloads/the-movies-dataset/credits.csv'
    cast_csv = '/Users/ruiwang/cast_csv.csv'

    df = spark.read.load(inputs,format="csv", sep=",",Schema=read_schema, header="true",escape='"')
    
    parse_Cast1 = functions.udf(parse_cast_1,types.StringType())
    parse_Cast2 = functions.udf(parse_cast_2,types.StringType())
    parse_Cast3 = functions.udf(parse_cast_3,types.StringType())
    parse_Cast4 = functions.udf(parse_cast_4,types.StringType())
    parse_Cast5 = functions.udf(parse_cast_5,types.StringType())
    
    parse_Director = functions.udf(parse_director,types.StringType())

    new_df = df.select(parse_Cast1(df['cast']).alias('actor_1'),parse_Cast2(df['cast']).alias('actor_2'),
                            parse_Cast3(df['cast']).alias('actor_3'),parse_Cast4(df['cast']).alias('actor_4'),
                            parse_Cast5(df['cast']).alias('actor_5'),parse_Director(df['crew']).alias('director'),'id')
    
    #new_df.show(10)
    #result = new_df.where(new_df['id']==16934)
    #result.show(10)
    new_df.repartition(1).write.csv(cast_csv,mode='overwrite',header="true")
if __name__ == '__main__':
    main()