import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, types
from pyspark.sql import functions as fn
spark = SparkSession.builder.appName('movie train').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+

from pyspark.ml import Pipeline
from pyspark.ml.feature import  VectorAssembler,StringIndexer
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.types import DoubleType, DateType
import numpy as np
movie_schema = types.StructType([
    types.StructField('id', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('genres', types.StringType()),
    types.StructField('release_date', types.TimestampType()),#types.DateType()),
    types.StructField('revenue', types.FloatType()),
    types.StructField('budget', types.FloatType()),
    types.StructField('profit', types.FloatType()),
    types.StructField('avg_rating', types.FloatType()),
    types.StructField('runtime', types.FloatType()),
    types.StructField('production_countries', types.StringType()),
    types.StructField('production_companies', types.StringType(),nullable=True),
    types.StructField('cast', types.StringType()),
    types.StructField('director', types.StringType()),
    types.StructField('key_country', types.IntegerType()),
    types.StructField('key_company', types.IntegerType()),
    types.StructField('key_cast', types.IntegerType()),
    types.StructField('key_director', types.IntegerType())
])


inputs= 'new_clean_join_dataset_index/clean_join_top_with_index_numerical/ml_top_with_index_numerical.csv'
# inputs= 'new_clean_join_dataset_index/clean_join_top_with_index_binary/ml_top_with_index_binary.csv'

data = spark.read.option("encoding", "UTF-8").schema(movie_schema).load(inputs,format="csv", sep=",", header="true",escape='"').cache()

data = data.withColumn('day_of_year', fn.dayofyear('release_date'))
data = data.withColumn('year',fn.year('release_date'))
data = data.withColumn('month',fn.month('release_date'))
data = data.withColumn('day',fn.dayofmonth('release_date'))
# data = data.withColumn("release_date", data["release_date"].cast(DateType()))


print(data.show(5))

def train_validate_model(list_features):
    train, validation = data.randomSplit([0.80, 0.20])
    train = train.cache()
    validation = validation.cache()

    LABEL = 'profit'
    company_indexer = StringIndexer(inputCol="key_company", outputCol="key_company_index",handleInvalid="skip")

    country_indexer = StringIndexer(inputCol="key_country", outputCol="key_country_index",handleInvalid="skip")
    # country_indexer = StringIndexer(inputCol="production_countries", outputCol="key_country_index",handleInvalid="skip")

    genres_indexer = StringIndexer(inputCol="genres", outputCol="genres_index", handleInvalid="skip")
    cast_indexer = StringIndexer(inputCol="key_cast", outputCol="key_cast_index", handleInvalid="skip")
    director_indexer = StringIndexer(inputCol="key_director", outputCol="key_director_index", handleInvalid="skip")

    train_feature_assembler = VectorAssembler(inputCols=list_features,outputCol='features')


    ############# DIFFERENT ML ALGORITHMS TO BE USED ####################
    # classifier = GeneralizedLinearRegression(featuresCol = 'features', labelCol=LABEL )
    # classifier = GBTRegressor( maxDepth=5,featuresCol = 'features', labelCol=LABEL,maxIter=10 )
    classifier = RandomForestRegressor(numTrees=5, maxDepth=5,featuresCol = 'features', labelCol=LABEL )
    #####################################################################

    train_pipeline = Pipeline(stages=[company_indexer, country_indexer,genres_indexer, cast_indexer, director_indexer,train_feature_assembler, classifier])
    model = train_pipeline.fit(train)
    prediction = model.transform(validation)

    evaluator = RegressionEvaluator(predictionCol="prediction",labelCol=LABEL,metricName='r2') #rmse
    score = evaluator.evaluate(prediction)
    print('Validation score for movie model: %g' % (score, ))
    trained_model = model.stages[-1]
    model_vector_assembler = model.stages[-2]
    list_feature_importance = list(zip(model_vector_assembler.getInputCols(),trained_model.featureImportances))
    sorted_list_feature_importance = sorted(list_feature_importance,key=lambda tup:tup[1],reverse=True)
    print(sorted_list_feature_importance)

    f= open('debug.txt','w')
    f.write(trained_model.toDebugString)
    # print('\n')
    return score

def main1():
    # without budget, with day/month/year
    list_features = ['key_company_index','key_country_index','genres_index','key_director_index','avg_rating','runtime','day_of_year','year','month','day']
    # with budget, with day/month/year
    list_features = ['budget','key_company_index','key_country_index','genres_index','key_director_index','avg_rating','runtime','day_of_year','year','month','day']

    dict_num_features = {}
    for num_features in range(5,len(list_features)):
        print("ith features:",num_features)
        list_each_num_features = []
        for i in range(10):
            new_list_features = np.random.choice(list_features,size=num_features,replace=False)
            score = train_validate_model(new_list_features)
            list_each_num_features.append([score,new_list_features])
            print("iter_",i)
            print(list_each_num_features)
            if num_features == len(list_features)-1 and i==2:
                break
        sorted_list =sorted(list_each_num_features, key = lambda i: i[0],reverse=True)
        iter_features = 'iter_features_'+str(num_features)
        dict_num_features[iter_features] = sorted_list
        print(dict_num_features)
        print('\n\n')
    # print(dict_num_features)

    for key,val in dict_num_features.items():
        print(key,val)
        print('\n\n')
def main2():
    '''################# Numerical data ######################################'''
    # original 11 features# with budget,with day,month,year --->r2=0.35 ,budget=0.60,key_country_index=0.08
    # list_features = ['budget','key_company_index','key_country_index','genres_index','key_director_index','avg_rating','runtime','day_of_year','year','month','day']

    # 10 features--->r2=0.349 budget=0.55, runtime=0.123, genres_index=0.08
    # list_features= ['day_of_year', 'runtime', 'avg_rating', 'budget','key_country_index', 'key_director_index', 'month','key_company_index', 'year', 'genres_index']

    # 9 features--->r2=0.37 budget=0.65, runtime=0.11, key_company_index=0.06
    # list_features =['day', 'month', 'runtime', 'key_director_index', 'key_company_index', 'key_country_index', 'avg_rating',  'day_of_year', 'budget']
        ##        --->r2=0.416 budget=0.71 runtime=0.1 key_country_index=0.04
    # list_features =['avg_rating', 'day', 'budget', 'day_of_year', 'key_director_index','key_company_index', 'year', 'key_country_index', 'runtime']

    # 8 features--->r2=0.33 budget=0.65, genres_index=0.11
    # list_features =['month', 'avg_rating', 'day', 'genres_index', 'budget','key_company_index', 'key_director_index', 'day_of_year']

    # 7 features--->r2=0.34 budget=0.61, genres_index=0.14
    # list_features =['runtime', 'avg_rating', 'key_director_index', 'key_country_index','genres_index', 'budget', 'day']

    # 6 features--->r2=0.30, budget=0.62, genres_index=0.11
    # list_features=['month', 'day_of_year', 'genres_index', 'day', 'budget','avg_rating']

    # 5 features--->r2=0.36, budget=0.75, year=0.08
    # list_features=['budget', 'day_of_year', 'year', 'key_company_index', 'key_country_index']
        ##        --->r2=0.45, budget=0.65, runtime=0.139, genres_index=0.103, day=0.06
    # list_features=['budget', 'genres_index', 'day', 'year', 'runtime']
    ''' ####################################################################'''
    '''################# Binary data ######################################'''
    # original 11 features# with budget,with day,month,year --->r2=0.33 ,budget=0.657,runtime=0.078
    list_features = ['budget','key_company_index','key_country_index','genres_index','key_director_index','avg_rating','runtime','day_of_year','year','month','day']

    # 10 features--->r2=0.40 budget=0.64, runtime=0.117, genres_index=0.08
    # list_features= ['day_of_year', 'runtime', 'avg_rating', 'budget','key_country_index', 'key_director_index', 'month','key_company_index', 'year', 'genres_index']

    # 9 features--->r2=0.34 budget=0.71, runtime=0.11, month=0.04
    # list_features =['day', 'month', 'runtime', 'key_director_index', 'key_company_index', 'key_country_index', 'avg_rating',  'day_of_year', 'budget']
        ##        --->r2=0.35 budget=0.71 year=0.097 runtime=0.06
    # list_features =['avg_rating', 'day', 'budget', 'day_of_year', 'key_director_index','key_company_index', 'year', 'key_country_index', 'runtime']

    # 8 features--->r2=0.37 budget=0.656, genres_index=0.09
    # list_features =['month', 'avg_rating', 'day', 'genres_index', 'budget','key_company_index', 'key_director_index', 'day_of_year']

    # 7 features--->r2=0.34 budget=0.635, genres_index=0.15, runtime=0.1
    # list_features =['runtime', 'avg_rating', 'key_director_index', 'key_country_index','genres_index', 'budget', 'day']

    # 6 features--->r2=0.30, budget=0.71, day_of_year=0.1,genres_index=0.08
    # list_features=['month', 'day_of_year', 'genres_index', 'day', 'budget','avg_rating']

    # 5 features--->r2=0.36, budget=0.77, day_of_year=0.11, year=0.087
    # list_features=['budget', 'day_of_year', 'year', 'key_company_index', 'key_country_index']
        ##        --->r2=0.38, budget=0.68, runtime=0.1, genres_index=0.09, year=0.076
    # list_features=['budget', 'genres_index', 'day', 'year', 'runtime']
    ''' ####################################################################'''
    list_features = ['budget','key_company_index','key_country_index','genres_index','key_director_index','avg_rating','runtime','day_of_year','year','month','day']
    list_features = ['budget','genres_index','runtime']
    #'key_company_index','key_country_index',
    score = train_validate_model(list_features)

    # weather_model.write().overwrite().save()
if __name__ == '__main__':
    # main1()
    main2()
