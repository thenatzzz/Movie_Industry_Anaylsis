import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F
spark = SparkSession.builder.appName('colour prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+



from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.types import DoubleType, DateType
from pyspark.mllib.linalg import Vectors, VectorUDT
from pyspark.sql.functions import udf

def run(data,output_3):
    data = data.withColumn("budget", data["budget"].cast(DoubleType()))
    data = data.withColumn("profit", data["profit"].cast(DoubleType()))
    data = data.withColumn("avg_rating", data["avg_rating"].cast(DoubleType()))
    data = data.withColumn("runtime", data["runtime"].cast(DoubleType()))
    data = data.withColumn("release_date", data["release_date"].cast(DateType()))
    data = data.withColumn("release_date", F.dayofyear(data["release_date"]))

    company_indexer = StringIndexer(inputCol="production_companies", outputCol="production_companies_index",
                                    handleInvalid="skip")
    indexer_fitted = company_indexer.fit(data)
    df_1 = indexer_fitted.transform(data)
    df_1 = df_1.select('id', 'production_companies_index')

    country_indexer = StringIndexer(inputCol="production_countries", outputCol="production_countries_index",
                                    handleInvalid="skip")
    indexer_fitted = country_indexer.fit(data)
    df_2 = indexer_fitted.transform(data)
    df_2 = df_2.select('id', 'production_countries_index')

    genres_indexer = StringIndexer(inputCol="genres", outputCol="genres_index", handleInvalid="skip")
    indexer_fitted = genres_indexer.fit(data)
    df_3 = indexer_fitted.transform(data)
    df_3 = df_3.select('id', 'genres_index')

    cast_indexer = StringIndexer(inputCol="cast", outputCol="cast_index", handleInvalid="skip")
    indexer_fitted = cast_indexer.fit(data)
    df_4 = indexer_fitted.transform(data)
    df_4 = df_4.select('id', 'cast_index')

    director_indexer = StringIndexer(inputCol="director", outputCol="director_index", handleInvalid="skip")
    indexer_fitted = director_indexer.fit(data)
    df_5 = indexer_fitted.transform(data)
    df_5 = df_5.select('id', 'director_index')

    df = df_1.join(df_2, 'id').select(df_1["*"], df_2['production_countries_index'])
    df = df.join(df_3, 'id').select(df["*"], df_3['genres_index'])
    df = df.join(df_4, 'id').select(df["*"], df_4['cast_index'])
    df = df.join(df_5, 'id').select(df["*"], df_5['director_index'])

    data = data.drop("production_companies", "production_countries", "genres", "cast", "director")
    df = data.join(df, 'id').select(data['*'], df['production_countries_index'], df['production_companies_index'],
                                    df['genres_index'], df['cast_index'], df['director_index'])

    df.coalesce(1).write.csv(output_3, mode = 'overwrite',header = True)

    print(df.show())

    # correlations
    print("profit-company" + str(df.stat.corr('profit', 'production_companies_index')))

    print("profit-country" + str(df.stat.corr('profit', 'production_countries_index')))

    print("profit-director" + str(df.stat.corr('profit', 'director_index')))

    print("profit-genres" + str(df.stat.corr('profit', 'genres_index')))

    print("profit-cast" + str(df.stat.corr('profit', 'cast_index')))

    print("profit-runtime" + str(df.stat.corr('profit', 'runtime')))

    print("profit-avg_rating" + str(df.stat.corr('profit', 'avg_rating')))

    print("profit-budget" + str(df.stat.corr('profit', 'budget')))

    print("profit-release_date" + str(df.stat.corr('profit', 'release_date')))



def main(inputs, inputs_2,output_3):

    # clean dataset

    data = spark.read.csv(inputs, header=True)
    run(data,output_3)

    data = spark.read.csv(inputs_2, header = True)
    data = data.withColumn('production_companies', data['key_company'])
    data = data.withColumn('production_countries', data['key_country'])
    data = data.withColumn('cast', data['key_cast'])
    data = data.withColumn('director', data['key_director'])
    data = data.drop("key_company", "key_country", "key_cast", "key_director")
    data.show()
    run(data,output_3)







if __name__ == '__main__':
    inputs = sys.argv[1]
    inputs_2 = sys.argv[2]
    output_3 = sys.argv[3]
    main(inputs, inputs_2,output_3)