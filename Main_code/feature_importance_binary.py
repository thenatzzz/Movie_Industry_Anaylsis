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
from pyspark.sql.types import DoubleType, DateType, StringType


def main(inputs):
    data = spark.read.csv(inputs, header = True)

    data = data.withColumn("budget", data["budget"].cast(DoubleType()))
    data = data.withColumn("profit", data["profit"].cast(DoubleType()))
    data = data.withColumn("avg_rating", data["avg_rating"].cast(DoubleType()))
    data = data.withColumn("runtime", data["runtime"].cast(DoubleType()))
    data = data.withColumn("release_date", data["release_date"].cast(DateType()))

    data = data.withColumn("key_country", data["key_country"].cast(StringType()))
    data = data.withColumn("key_company", data["key_company"].cast(StringType()))
    data = data.withColumn("key_cast", data["key_cast"].cast(StringType()))
    data = data.withColumn("key_director", data["key_director"].cast(StringType()))

    company_indexer = StringIndexer(inputCol="key_company", outputCol="key_company_index",
                                    handleInvalid="skip")
    country_indexer = StringIndexer(inputCol="key_country", outputCol="key_country_index",
                                    handleInvalid="skip")
    genres_indexer = StringIndexer(inputCol="genres", outputCol="genres_index", handleInvalid="skip")
    cast_indexer = StringIndexer(inputCol="key_cast", outputCol="key_cast_index", handleInvalid="skip")
    director_indexer = StringIndexer(inputCol="key_director", outputCol="key_director_index", handleInvalid="skip")

    data = data.withColumn("release_date", F.dayofyear(data["release_date"]))



    # indexed = indexer.fit(data).transform(data)


    # Train and validation sets
    
    train, validation = data.randomSplit([0.80, 0.20])
    train = train.cache()
    validation = validation.cache()
    
    # Pipeline and model

    assembler = VectorAssembler(inputCols = ["key_company_index", "key_country_index", "genres_index", "release_date", "budget", "avg_rating", "runtime", "key_cast_index", "key_director_index"], outputCol = 'features')
    # assembler = VectorAssembler(inputCols = ["genres_index", "release_date", "budget", "avg_rating", "runtime"], outputCol = 'features')

    regressor = RandomForestRegressor(labelCol = 'profit')
    
    pipeline = Pipeline(stages=[company_indexer, country_indexer,genres_indexer, cast_indexer, director_indexer,  assembler, regressor])
    # pipeline = Pipeline(stages=[genres_indexer, assembler, regressor])

    model = pipeline.fit(train)

    
    # Evaluation
    
    evaluator = RegressionEvaluator(labelCol="profit")
    predictions = model.transform(validation)
    score = evaluator.evaluate(predictions)
    print('Validation score for RandomForest model:', score)
    print(model.stages[-1].featureImportances)


    # The featureImportance score for budget is too high, drop it and see the proformance of others

    second_assembler = VectorAssembler(inputCols = ["key_company_index", "key_country_index", "genres_index", "release_date", "avg_rating", "runtime", "key_cast_index", "key_director_index"], outputCol = 'features')

    second_pipeline = Pipeline(stages=[company_indexer, country_indexer,genres_indexer, cast_indexer, director_indexer,  second_assembler, regressor])

    second_model = second_pipeline.fit(train)

    second_evaluator = RegressionEvaluator(labelCol="profit")
    second_predictions = second_model.transform(validation)
    second_score = second_evaluator.evaluate(second_predictions)
    print('Validation score for RandomForest model:', second_score)
    print(second_model.stages[-1].featureImportances)


    # The featureImportance of genres is too high, drop it and run again

    third_assembler = VectorAssembler(
        inputCols=["key_company_index", "key_country_index", "release_date", "avg_rating", "runtime", "key_cast_index", "key_director_index"], outputCol='features')

    third_pipeline = Pipeline(
        stages=[company_indexer, country_indexer, cast_indexer, director_indexer,third_assembler, regressor])

    third_model = third_pipeline.fit(train)

    third_evaluator = RegressionEvaluator(labelCol="profit")
    third_predictions = third_model.transform(validation)
    third_score = third_evaluator.evaluate(third_predictions)
    print('Validation score for RandomForest model:', third_score)
    print(third_model.stages[-1].featureImportances)


    # Final time !!!!!!

    forth_assembler = VectorAssembler(
        inputCols=["key_company_index", "key_country_index", "release_date", "avg_rating", "key_cast_index", "key_director_index"], outputCol='features')

    forth_pipeline = Pipeline(
        stages=[company_indexer, country_indexer, cast_indexer, director_indexer,forth_assembler, regressor])

    forth_model = forth_pipeline.fit(train)

    forth_evaluator = RegressionEvaluator(labelCol="profit")
    forth_predictions = forth_model.transform(validation)
    forth_score = forth_evaluator.evaluate(forth_predictions)
    print('Validation score for RandomForest model:', forth_score)
    print(forth_model.stages[-1].featureImportances)



if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)
