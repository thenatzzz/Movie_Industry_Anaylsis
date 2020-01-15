import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
import random
from pyspark.sql.types import StructType,ArrayType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType,FloatType,DoubleType
import itertools
import copy
from pyspark.sql.functions import when,col

spark = SparkSession.builder.appName('preprocess unique features').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

INDEX_DIRECTOR = 5
INDEX_ID = 6
NUM_CASTS = 5

INDEX_PRODUCTION_COMPANIES =2
INDEX_PRODUCTION_COUNTRIES =3
INDEX_GENRES =4

######## To specify how many casts and directors ###############################
NUM_TOP_CASTS = 1000
NUM_TOP_DIRECTORS = 1000
NUM_TOP_PRODUCTION_COMPANIES = 100
NUM_TOP_PRODUCTION_COUNTRIES = 50
NUM_TOP_GENRES = 10
###############################################################################

def get_non_null_index(single_rdd,num_cast= NUM_CASTS):
    temp_list = []
    for i in range(NUM_CASTS):
        if single_rdd[i] == 'null' or single_rdd[i]=='Unknown':
            continue
        temp_list.append(i)
    if len(temp_list)==0:
        return [0,1]
    return temp_list
def get_random_one_cast(single_rdd):
    list_non_null_index = get_non_null_index(single_rdd)
    index_random_cast = random.choice(list_non_null_index)
    director = single_rdd[INDEX_DIRECTOR]
    id = single_rdd[INDEX_ID]
    return [single_rdd[index_random_cast],director,id]
def format_string(single_col,which_col):
    col_feature = single_col[which_col]
    col_feature = col_feature.replace("[","")
    col_feature = col_feature.replace("]","")
    col_feature = col_feature.replace(" ","")
    col_feature = col_feature.replace("null","")
    list_feature = col_feature.split(',')
    return list_feature
def get_random_movie_features(single_rdd):
    list_production_companies = format_string(single_rdd,INDEX_PRODUCTION_COMPANIES)
    list_index_production_companies =  [x for x in range(len(list_production_companies))]
    index_random_production_companies = random.choice(list_index_production_companies)

    list_production_countries = format_string(single_rdd,INDEX_PRODUCTION_COUNTRIES)
    list_index_production_countries =  [x for x in range(len(list_production_countries))]
    index_random_countries = random.choice(list_index_production_countries)

    list_genres = format_string(single_rdd,INDEX_GENRES)
    list_index_genres =  [x for x in range(len(list_genres))]
    index_random_genres = random.choice(list_index_genres)
    return [single_rdd[0],single_rdd[1],list_production_companies[index_random_production_companies],list_production_countries[index_random_countries],list_genres[index_random_genres],single_rdd[5],single_rdd[6],single_rdd[7],single_rdd[8]]
def get_single_top_movie_feature(list_feature,list_top_feature,index):
    result = 'not_found'
    for i in range(len(list_feature)):
        for j in range(0,index):
            if list_feature[i] == list_top_feature[j]:
                result = list_top_feature[j]
                break
    if result == 'not_found':
        result = random.choice(list_feature)
    return result
def get_top_movie_features(single_rdd,list_top_feature):
    list_production_companies = format_string(single_rdd,INDEX_PRODUCTION_COMPANIES)
    list_production_countries = format_string(single_rdd,INDEX_PRODUCTION_COUNTRIES)
    list_genres = format_string(single_rdd,INDEX_GENRES)

    list_top_unique_production_companies = list_top_feature[0]
    list_top_unique_production_countries = list_top_feature[1]
    list_top_unique_genre = list_top_feature[2]

    top_production_companies = get_single_top_movie_feature(list_production_companies,list_top_unique_production_companies,NUM_TOP_PRODUCTION_COMPANIES)
    top_production_countries = get_single_top_movie_feature(list_production_countries,list_top_unique_production_countries,NUM_TOP_PRODUCTION_COUNTRIES)
    top_production_genres = get_single_top_movie_feature(list_genres,list_top_unique_genre,NUM_TOP_GENRES)

    return [single_rdd[0],single_rdd[1],top_production_companies,top_production_countries,top_production_genres,single_rdd[5],single_rdd[6],single_rdd[7],single_rdd[8]]
def rdd_to_df(rdd,schema):
    df = spark.createDataFrame(rdd, schema)
    return df
def get_unique_col(which_col,cast_df):
    unique_cast_df=cast_df.groupby(which_col).count().sort(functions.desc("count"))
    return unique_cast_df
def get_top_features(which_col,unique_feature_df):
    list_unique_feature = unique_feature_df.select(which_col).rdd.flatMap(lambda x: x).collect()
    if which_col == 'director':
        num_top = NUM_TOP_DIRECTORS
    else:
        num_top = NUM_TOP_CASTS
    return list_unique_feature[0:num_top]
def get_top_cast(single_rdd,list_top_cast):
    list_non_null_index = get_non_null_index(single_rdd)
    index_random_cast = random.choice(list_non_null_index)
    director = single_rdd[INDEX_DIRECTOR]
    id = single_rdd[INDEX_ID]
    for i in range(NUM_CASTS):
        for elem_cast in list_top_cast:
            if single_rdd[i] == elem_cast:
                cast = elem_cast
                break
    cast = single_rdd[index_random_cast]
    return [str(cast),str(director),str(id)]
def blank_as_str_null(x):
    return when(col(x) != "", col(x)).otherwise('[null]')

def main_cast():
    cast_csv = 'clean_dataset/Dataset_cast_recent.csv'
    # cast_csv = 'clean_dataset/Dataset_cast_past.csv'

    cast_df = spark.read.option("encoding", "UTF-8").load(cast_csv,format="csv", sep=",", header="true",escape='"').cache()
    cast_df = cast_df.select("actor_1","actor_2","actor_3","actor_4","actor_5","director","id")
    print(cast_df.show(5))

    # replace 'Unknown' string with 'null' string in casts and directors
    cast_df = cast_df.withColumn('actor_1', functions.when(cast_df['actor_1']=='Unknown', 'null').otherwise(cast_df['actor_1']))
    cast_df = cast_df.withColumn('actor_2', functions.when(cast_df['actor_2']=='Unknown', 'null').otherwise(cast_df['actor_2']))
    cast_df = cast_df.withColumn('actor_3', functions.when(cast_df['actor_3']=='Unknown', 'null').otherwise(cast_df['actor_3']))
    cast_df = cast_df.withColumn('actor_4', functions.when(cast_df['actor_4']=='Unknown', 'null').otherwise(cast_df['actor_4']))
    cast_df = cast_df.withColumn('actor_5', functions.when(cast_df['actor_5']=='Unknown', 'null').otherwise(cast_df['actor_5']))
    cast_df = cast_df.withColumn('actor_5', functions.when(cast_df['actor_5']=='Unknown', 'null').otherwise(cast_df['actor_5']))
    cast_df = cast_df.withColumn('director', functions.when(cast_df['director']=='Unknown', 'null').otherwise(cast_df['director']))

    # drop all empty cells (null) from directors
    cast_df = cast_df.filter(cast_df['director'].isNotNull())
    # drop all empty cells (null) from casts that have 5 empty cell in each actor
    cast_df = cast_df.filter( (cast_df['actor_1'].isNotNull()) & (cast_df['actor_2'].isNotNull()) & (cast_df['actor_3'].isNotNull()) & (cast_df['actor_4'].isNotNull()) & (cast_df['actor_5'].isNotNull()))
    # drop all cell with 'null' or 'Unknown' string from director
    cast_df = cast_df.filter(cast_df['director']!='null')
    # drop all cell with 'null' or 'Unknown' string from all 5 casts
    cast_df = cast_df.filter( (cast_df['actor_1']!='null') & (cast_df['actor_2']!='null') & (cast_df['actor_3']!='null') & (cast_df['actor_4']!='null')& (cast_df['actor_5']!='null'))

    ########### To get unique director for 'director' column ###################
    unique_director_df=get_unique_col('director',cast_df)
    list_top_unique_director = get_top_features('director',cast_df)

    '''
    ########### To get unique cast for each individual Column ##################
    unique_cast1_df = get_unique_col('actor_1',cast_df)
    list_top_unique_cast1= get_top_features('actor_1',unique_cast1_df)
    print(unique_cast1_df.show())
    print(list_top_unique_cast1)
    unique_cast2_df = get_unique_col('actor_2',cast_df)
    list_top_unique_cast2= get_top_features('actor_2',unique_cast2_df)
    print(unique_cast2_df.show())
    print(list_top_unique_cast2)
    unique_cast3_df = get_unique_col('actor_3',cast_df)
    list_top_unique_cast3= get_top_features('actor_3',unique_cast3_df)
    print(unique_cast3_df.show())
    print(list_top_unique_cast3)
    unique_cast4_df = get_unique_col('actor_4',cast_df)
    list_top_unique_cast4= get_top_features('actor_4',unique_cast4_df)
    print(unique_cast4_df.show())
    print(list_top_unique_cast4)
    unique_cast5_df = get_unique_col('actor_5',cast_df)
    list_top_unique_cast5= get_top_features('actor_5',unique_cast5_df)
    print(unique_cast5_df.show())
    print(list_top_unique_cast5)
    '''

    ########### To get unique cast for combined cast Columns ##################
    cast_df = cast_df.withColumn('joined_column',
                    functions.concat(functions.col('actor_1'),
                    functions.lit(','), functions.col('actor_2'),
                    functions.lit(','), functions.col('actor_3'),
                    functions.lit(','), functions.col('actor_4'),
                    functions.lit(','), functions.col('actor_5')))
    cast_df=cast_df.withColumn('joined_col_temp', functions.split(functions.col('joined_column'), ','))
    unique_cast_df=cast_df.select(functions.explode('joined_col_temp').alias('unique_cast')).groupby('unique_cast').count()
    unique_cast_df= unique_cast_df.withColumn('unique_cast',functions.trim(functions.col('unique_cast')))
    unique_cast_df=unique_cast_df.groupby('unique_cast').agg({'count':'sum'}).sort(functions.desc("sum(count)"))
    list_top_unique_casts = get_top_features('unique_cast',unique_cast_df)

    cast_df = cast_df.drop(cast_df.joined_column)
    cast_df = cast_df.drop(cast_df.joined_col_temp)

    cast_rdd = cast_df.rdd.map(list)

    ''' ######################################################################'''
    ''' #### 1st Function to random casts and 2nd function to pick top casts ###'''
    new_cast_rdd = cast_rdd.map(get_random_one_cast)
    # new_cast_rdd = cast_rdd.map(lambda j: get_top_cast(j, list_top_unique_casts))
    ''' ######################################################################### '''

    new_schema_cast_csv = StructType([StructField('cast', StringType(), True),
                     StructField('director', StringType(), True),
                     StructField('id', StringType(), True)])
    new_cast_df = rdd_to_df(new_cast_rdd,new_schema_cast_csv)

    # outputs= 'new_clean_cast_dataset/single_cast_recent_top1000.csv'
    outputs= 'new_clean_cast_dataset/single_cast_recent_random.csv'
    # outputs= 'new_clean_cast_dataset/single_cast_past_top1000.csv'
    # outputs= 'new_clean_cast_dataset/single_cast_past_random.csv'

    # new_cast_df.repartition(1).write.csv(outputs,mode='overwrite',header="true")

def main_movie_with_rating():
    movie_csv = 'clean_dataset/Dataset_movie_with_rating_csv.csv'
    movie_df = spark.read.load(movie_csv,format="csv", sep=",", header="true",escape='"').cache()
    print(movie_df.show())
    # Find mean of revenue in non-zero row
    movie_df_1 = movie_df.filter(movie_df['revenue'] != 0)
    mean_revenue = movie_df_1.groupBy().agg(functions.avg(movie_df_1['revenue'])).collect()[0][0]
    # Find mean of budget in non-zero row
    movie_df_1 = movie_df.filter(movie_df['budget'] != 0)
    mean_budget = movie_df_1.groupBy().agg(functions.avg(movie_df_1['budget'])).collect()[0][0]
    # Find mean of avg_rating in specified conditions as below
    id_rating_df = movie_df.select('id','avg_rating')
    movie_df_1 = id_rating_df.filter(id_rating_df['avg_rating'] != 'null')
    movie_df_1 = movie_df_1.withColumn("avg_rating", movie_df_1["avg_rating"].cast(DoubleType()))
    movie_df_1 = movie_df_1.filter(movie_df_1['avg_rating'] < 6)
    mean_rating = movie_df_1.groupBy().agg(functions.avg(movie_df_1['avg_rating'])).collect()[0][0]
    # replace null with the mean of avg_rating
    movie_df = movie_df.withColumn('avg_rating', functions.when( movie_df['avg_rating'] != "", movie_df['avg_rating']).otherwise(mean_rating))
    # replace 0 with the mean of revenue
    movie_df = movie_df.withColumn('revenue', functions.when(movie_df['revenue'] == 0, mean_revenue).otherwise(movie_df['revenue']))
    # replace 0 with the mean of budget
    movie_df = movie_df.withColumn('budget', functions.when(movie_df['budget'] == 0, mean_budget).otherwise(movie_df['budget']))
    # drop all empty cells (null) from release_date,production_companies,production_countries,genres
    movie_df = movie_df.filter(movie_df['release_date'].isNotNull())
    movie_df = movie_df.filter(movie_df['production_companies'].isNotNull())
    movie_df = movie_df.filter(movie_df['production_countries'].isNotNull())
    movie_df = movie_df.filter(movie_df['genres'].isNotNull())
    # drop all cell with 'null' string from release_date,production_companies,production_countries,genres
    movie_df = movie_df.filter( (movie_df['release_date']!='null') & (movie_df['release_date']!='[null]'))
    movie_df = movie_df.filter((movie_df['production_companies']!='null') & (movie_df['production_companies']!='[null]'))
    movie_df = movie_df.filter((movie_df['production_countries']!='null') & (movie_df['production_countries']!='[null]'))
    movie_df = movie_df.filter((movie_df['genres']!='null') & (movie_df['genres']!='[null]'))

    movie_df=movie_df.withColumn('genres_temp', functions.split(functions.regexp_extract('genres', '\[(.*)\]',1), ','))
    unique_genre=movie_df.select(functions.explode('genres_temp').alias('unique_genre')).groupby('unique_genre').count()
    unique_genre= unique_genre.withColumn('unique_genre',functions.trim(functions.col('unique_genre')))
    unique_genre = unique_genre.withColumn("unique_genre", blank_as_str_null("unique_genre"))
    unique_genre=unique_genre.groupby('unique_genre').agg({'count':'sum'}).sort(functions.desc("sum(count)")).cache()
    list_top_unique_genre = get_top_features('unique_genre',unique_genre)

    movie_df=movie_df.withColumn('production_companies_temp', functions.split(functions.regexp_extract('production_companies', '\[(.*)\]',1), ','))
    unique_production_companies=movie_df.select(functions.explode('production_companies_temp').alias('unique_production_companies')).groupby('unique_production_companies').count()
    unique_production_companies= unique_production_companies.withColumn('unique_production_companies',functions.trim(functions.col('unique_production_companies')))
    unique_production_companies = unique_production_companies.withColumn("unique_production_companies", blank_as_str_null("unique_production_companies"))
    unique_production_companies=unique_production_companies.groupby('unique_production_companies').agg({'count':'sum'}).sort(functions.desc("sum(count)")).cache()
    list_top_unique_production_companies = get_top_features('unique_production_companies',unique_production_companies)

    movie_df=movie_df.withColumn('production_countries_temp', functions.split(functions.regexp_extract('production_countries', '\[(.*)\]',1), ','))
    unique_production_countries=movie_df.select(functions.explode('production_countries_temp').alias('unique_production_countries')).groupby('unique_production_countries').count()
    unique_production_countries= unique_production_countries.withColumn('unique_production_countries',functions.trim(functions.col('unique_production_countries')))
    unique_production_countries = unique_production_countries.withColumn("unique_production_countries", blank_as_str_null("unique_production_countries"))
    unique_production_countries=unique_production_countries.groupby('unique_production_countries').agg({'count':'sum'}).sort(functions.desc("sum(count)")).cache()
    list_top_unique_production_countries = get_top_features('unique_production_countries',unique_production_countries)

    movie_df = movie_df.drop(movie_df.production_companies_temp).drop(movie_df.production_countries_temp).drop(movie_df.genres_temp)
    movie_rdd = movie_df.rdd.map(list)

    list_top_movie_features = [list_top_unique_production_companies,list_top_unique_production_countries,list_top_unique_genre]
    '''#######################################################################################################'''
    '''#### 1st Function to random production_companies and 2nd function to pick top production_companies ####'''
    # new_movie_rdd = movie_rdd.map(lambda j: get_random_movie_features(j))
    new_movie_rdd = movie_rdd.map(lambda j: get_top_movie_features(j, list_top_movie_features))
    '''####################################################################################################### '''

    new_schema_movie_csv = StructType([StructField('id', StringType(), True),
                     StructField('title', StringType(), True),
                     StructField('production_companies', StringType(), True),
                     StructField('production_countries', StringType(), True),
                     StructField('genres', StringType(), True),
                     StructField('release_date', StringType(), True),
                     StructField('revenue', StringType(), True),
                     StructField('budget', StringType(), True),
                     StructField('avg_rating', StringType(), True)])
    new_movie_df = rdd_to_df(new_movie_rdd,new_schema_movie_csv)
    print(new_movie_df.show())
    
    meta_movie_csv = 'dataset/movies_metadata.csv'
    meta_movie_df = spark.read.load(meta_movie_csv,format="csv", sep=",", header="true",escape='"').cache()
    meta_movie_df = meta_movie_df.select("id","runtime").withColumnRenamed('id','id_meta')
    meta_movie_df=meta_movie_df.withColumn('runtime_temp',meta_movie_df.runtime.cast(DoubleType())).drop('runtime').withColumnRenamed('runtime_temp','runtime').cache()

    joined_movie_df = new_movie_df.join(meta_movie_df,new_movie_df.id == meta_movie_df.id_meta).drop(meta_movie_df.id_meta).cache()
    temp_joined_movie_df = joined_movie_df.filter(joined_movie_df['runtime'].isNotNull())
    joined_movie_df= joined_movie_df.withColumn("temp",functions.when(functions.col('runtime').isNotNull(), col('runtime')).otherwise(None)).drop('temp').cache()
    joined_movie_df = joined_movie_df.fillna(1000,subset=['runtime'])
    mean_runtime = temp_joined_movie_df.groupBy().agg(functions.avg(temp_joined_movie_df['runtime'])).collect()[0][0]
    joined_movie_df = joined_movie_df.withColumn('runtime', functions.when( joined_movie_df['runtime'] == 1000, mean_runtime).otherwise(joined_movie_df['runtime']))
    print(joined_movie_df.show())

    outputs= 'new_clean_dataset/clean_top_movie_with_rating_csv.csv'
    outputs= 'new_clean_dataset/clean_random_movie_with_rating_csv.csv'

    # joined_movie_df.coalesce(1).write.csv(outputs, mode = 'overwrite',header = True)

def main():
    main_movie_with_rating()       # for Dataset_movie_with_rating_csv
    # main_cast()    # for Dataset_cast_past/Dataset_cast_recent.csv

if __name__ == '__main__':
    main()
