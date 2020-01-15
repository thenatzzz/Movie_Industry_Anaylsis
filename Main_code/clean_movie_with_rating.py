
from pyspark import SparkConf, SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

movie_schema = types.StructType([
    types.StructField('id', types.LongType()),
    types.StructField('title', types.StringType()),
    types.StructField('production_companie', types.StringType()),
    types.StructField('production_countries', types.StringType()),
    types.StructField('genres', types.StringType()),
    types.StructField('release_date', types.StringType()),
    types.StructField('revenue', types.LongType()),
    types.StructField('budget', types.LongType()),
    types.StructField('rating', types.StringType()),
])

def choose_one(lst):
        key = lst[0]
        value = lst[1]
        value = value[1:]
        value = value[:-1]
        value = list(value.split(','))
        value = value[0]
        
        return (key,value)



def main(inputs, outputs):
    df_input = spark.read.csv(inputs).toDF('id','title','production_companies','production_countries','genres','release_date','revenue','budget','rating')
    print(df_input.dtypes)
    # get the mean of revenue
    df_1 = df_input.filter(df_input['revenue'] != 0)
    mean_revenue = df_1.groupBy().agg(F.avg(df_1['revenue'])).collect()[0][0]
    # and get the mean of budget
    df_1 = df_input.filter(df_input['budget'] != 0)
    mean_budget = df_1.groupBy().agg(F.avg(df_1['budget'])).collect()[0][0]
    # and get the mean of avg_rating
    df_df = df_input.select('id','rating')
    df_1 = df_df.filter(df_df['rating'] != 'null')
    df_2 = df_df.filter(df_df['rating'] == 'null')
    df_1 = df_1.withColumn("rating", df_1["rating"].cast(DoubleType()))
    df_1 = df_1.filter(df_1['rating'] < 6)
    mean_rating = df_1.groupBy().agg(F.avg(df_1['rating'])).collect()[0][0]
    df_2 = df_2.withColumn('rating',F.lit(mean_rating))
    df_rating = df_1.unionAll(df_2).cache()
    # replace 0 with the mean of revenue
    df_input = df_input.withColumn('revenue', F.when(df_input['revenue'] == 0, mean_revenue).otherwise(df_input['revenue']))
    # replace 0 with the mean of budget
    df_input = df_input.withColumn('budget', F.when(df_input['budget'] == 0, mean_budget).otherwise(df_input['budget']))
    # drop all null
    df_input = df_input.filter(df_input['production_companies'] != 'null')
    df_input = df_input.filter(df_input['production_countries'] != 'null')
    df_input = df_input.filter(df_input['genres'] != 'null')
    df_input = df_input.filter(df_input['production_companies'] != '[null]')
    df_input = df_input.filter(df_input['production_countries'] != '[null]')
    df_input = df_input.filter(df_input['genres'] != '[null]')
    df_input = df_input.filter(df_input['release_date'] != '').cache()
    # keep one company for each movie
    rdd_company = df_input.select('id','production_companies').rdd
    rdd_company = rdd_company.map(choose_one)
    df_company = rdd_company.toDF(['id','production_companies']).cache()
    df_company = df_company.filter(df_company['production_companies'] != 'null')
    # keep one country for each movie
    rdd_country = df_input.select('id','production_countries').rdd
    rdd_country = rdd_country.map(choose_one)
    df_country = rdd_country.toDF(['id','production_countries']).cache() 
    # keep one genre for each movie
    rdd_genre = df_input.select('id','genres').rdd
    rdd_genre = rdd_genre.map(choose_one)
    df_genre = rdd_genre.toDF(['id','genres']).cache()
    # join
    df = df_company.join(df_country, 'id').select(df_company['id'], df_company['production_companies'], df_country['production_countries'])
    df = df.join(df_genre, 'id').select(df['*'], df_genre['genres'])

    df_part = df_input.select('id','title','release_date','revenue','budget')
    df = df.join(df_part, 'id').select(df['*'], df_part['title'], df_part['release_date'], df_part['revenue'], df_part['budget'])

    df = df.join(df_rating, 'id').select(df['*'], df_rating['rating'].alias('avg_rating'))
    # save
    
    df.coalesce(1).write.csv(outputs, mode = 'overwrite',header = True)    


if __name__ == '__main__':
    inputs = sys.argv[1]
    outputs = sys.argv[2]
    main(inputs, outputs)


    
