import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.types import StructType,ArrayType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType,FloatType,DoubleType
from pyspark.sql.functions import when,col
from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame

spark = SparkSession.builder.appName('join cast with movie csv').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

USE_TOP = True

def union_all(*dfs):
    return reduce(DataFrame.unionAll, dfs)

def main():
    if USE_TOP:
        cast_recent_csv = 'new_clean_cast_dataset/single_cast_recent_top1000/single_cast_recent_top1000.csv'
        cast_past_csv = 'new_clean_cast_dataset/single_cast_past_top1000/single_cast_past_top1000.csv'
    else:
        cast_recent_csv = 'new_clean_cast_dataset/single_cast_recent_random/single_cast_recent_random.csv'
        cast_past_csv = 'new_clean_cast_dataset/single_cast_past_random/single_cast_past_random.csv'

    cast_recent_df = spark.read.load(cast_recent_csv,format="csv", sep=",", header="true",escape='"').cache()
    cast_past_df = spark.read.load(cast_past_csv,format="csv", sep=",", header="true",escape='"').cache()
    cast_past_df = cast_past_df.withColumnRenamed('id','id_past')

    join_cast_df = union_all(cast_recent_df,cast_past_df)
    join_cast_df = join_cast_df.withColumnRenamed('id','id_temp')

    print(join_cast_df.count())
    print(join_cast_df.show())

    if USE_TOP:
        movie_rating_csv = 'new_clean_dataset/clean_top_movie_with_rating_csv/clean_top_movie_with_rating_csv.csv'
    else:
        movie_rating_csv = 'new_clean_dataset/clean_random_movie_with_rating_csv/clean_random_movie_with_rating_csv.csv'

    movie_rating_df = spark.read.load(movie_rating_csv,format="csv", sep=",", header="true",escape='"').cache()
    print(movie_rating_df.show())
    print(movie_rating_df.count())

    joined_movie_df = movie_rating_df.join(join_cast_df,movie_rating_df.id == join_cast_df.id_temp).drop(join_cast_df.id_temp).cache()
    print(joined_movie_df.show())
    print(joined_movie_df.count())

    if USE_TOP:
        outputs= 'new_clean_join_dataset/clean_join_top'
    else:
        outputs= 'new_clean_join_dataset/clean_join_random'

    # joined_movie_df.coalesce(1).write.csv(outputs, mode = 'overwrite',header = True)


if __name__ == '__main__':
    main()
