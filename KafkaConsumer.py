from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 pyspark-shell'

kafka_topic="new_topic"
kafka_bootstrap_servers="localhost:9092"


spark=SparkSession\
    .builder\
    .appName("kafka1")\
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.jars", os.getcwd() + "/jars/spark-sql-kafka-0-10_2.12-3.4.1.jar" + "," + os.getcwd() + "/jars/kafka-clients-2.1.1.jar") \
    .master("local[*]")\
    .getOrCreate()
 
df=spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers",kafka_bootstrap_servers)\
    .option("subscribe",kafka_topic)\
    .option("startingOffsets", "earliest")\
    .load()


schema_csv = "id INTEGER, description STRING,category STRING,product_description BOOLEAN, link BOOLEAN, gln_of_supplier BOOLEAN,taric BOOLEAN, details BOOLEAN, product_article BOOLEAN,number_of_supplier BOOLEAN, gln_of_manufacturer BOOLEAN,gtin BOOLEAN, brand BOOLEAN, eclassnumber INTEGER"
df = df.select(from_csv(col("value").cast("string"), schema_csv).alias("data"), "timestamp")
df = df.select("data.*", "timestamp")


parquet_output_path="C:/bigData"
data_write_stream = df \
    .writeStream \
    .trigger(processingTime='5 seconds') \
    .outputMode("append") \
    .foreachBatch(lambda df, batchId: df.write.parquet(parquet_output_path + "/batch_" + str(batchId), mode="overwrite"))\
    .start()
    
data_write_stream.stop()

schema = StructType([
    StructField("id", IntegerType(), nullable=True),
    StructField("description", StringType(), nullable=True),
    StructField("category", StringType(), nullable=True),
    StructField("product_description", BooleanType(), nullable=True),
    StructField("link", BooleanType(), nullable=True),
    StructField("gln_of_supplier", BooleanType(), nullable=True),
    StructField("taric", BooleanType(), nullable=True),
    StructField("details", BooleanType(), nullable=True),
    StructField("product_article", BooleanType(), nullable=True),
    StructField("number_of_supplier", BooleanType(), nullable=True),
    StructField("gln_of_manufacturer", BooleanType(), nullable=True),
    StructField("gtin", BooleanType(), nullable=True),
    StructField("brand", BooleanType(), nullable=True),
    StructField("eclassnumber", IntegerType(), nullable=True),
])

# Read the data from the Parquet file sink as a static DataFrame with the specified schema

static_df = spark.read.schema(schema).parquet("C:/bigData/batch_1")
static_df.show()

from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF,VectorAssembler,NGram
from pyspark.ml import Pipeline

df=static_df.withColumn('description_length',length(static_df['description']))

tokenizer=Tokenizer(inputCol='description', outputCol='token_text')
stop_remove=StopWordsRemover(inputCol='token_text',outputCol='token_stop')
ngram=NGram(n=5,inputCol='token_stop', outputCol='ngram')
count_vec=CountVectorizer(inputCol='ngram', outputCol='count_vec')
idf=IDF(inputCol='count_vec',outputCol='tf-idf')

tokenizer_cat=Tokenizer(inputCol='category', outputCol='token_category')
stop_remove_cat=StopWordsRemover(inputCol='token_category',outputCol='category_stop')
count_vec_cat = CountVectorizer(inputCol='category_stop', outputCol='count_vec_cat')
idf_cat=IDF(inputCol='count_vec_cat',outputCol='tf-idf-cat')



transformed_df=VectorAssembler(inputCols=['id','description_length','tf-idf','tf-idf-cat','product_description',
                                          'link', 'gln_of_supplier','taric','details',
                                          'product_article','number_of_supplier','gln_of_manufacturer',
                                          'gtin','brand'],outputCol='features')



df_pipe=Pipeline(stages=[tokenizer, stop_remove, ngram, count_vec, idf,tokenizer_cat,stop_remove_cat, count_vec_cat,idf_cat, transformed_df])

final_df=df_pipe.fit(df).transform(df).select('eclassnumber','features')

train_data, test_data=final_df.randomSplit([0.7,0.3], seed=123)

final_df.describe().show()
train_data.describe().show()
test_data.describe().show()

from pyspark.ml.regression import LinearRegression
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator

ln=LinearRegression(labelCol='eclassnumber',elasticNetParam=0.5,maxIter=20)

param_grid = ParamGridBuilder() \
    .addGrid(ln.regParam, [0.0, 0.1, 0.01]) \
    .build()


crossval = CrossValidator(estimator=ln,
                          estimatorParamMaps=param_grid,
                          evaluator=RegressionEvaluator(labelCol='eclassnumber'),
                          numFolds=5)


cv_model = crossval.fit(train_data)
best_model = cv_model.bestModel

test_predictions = best_model.transform(test_data)
evaluator = RegressionEvaluator(labelCol='eclassnumber')
test_results = evaluator.evaluate(test_predictions)


evaluator = RegressionEvaluator(labelCol='eclassnumber')
mae = evaluator.evaluate(test_predictions, {evaluator.metricName: 'mae'})
mse = evaluator.evaluate(test_predictions, {evaluator.metricName: 'mse'})
rmse = evaluator.evaluate(test_predictions, {evaluator.metricName: 'rmse'})
r2 = evaluator.evaluate(test_predictions, {evaluator.metricName: 'r2'})


print("MAE: ", mae)
print("MSE: ", mse)
print("RMSE: ", rmse)
print("R2: ", r2)


result_table = test_predictions.select('eclassnumber', col('prediction').cast('int'))
result_table.show(100)

sparksesion = SparkSession.builder.appName("ActualVsPredicted").getOrCreate()

import matplotlib.pyplot as plt
import pandas as pd
# Convert the result_table DataFrame to a Pandas DataFrame
result_pandas = result_table.toPandas()

# Sort the data based on 'eclassnumber' for plotting
result_pandas = result_pandas.sort_values(by='eclassnumber')

# Extract the values for plotting
eclassnumbers = result_pandas['eclassnumber']
predictions = result_pandas['prediction']

# Create the line plot using Matplotlib
plt.figure(figsize=(20, 10))
plt.scatter(eclassnumbers, predictions, marker='o', color='blue', label='Predictions', alpha=0.2)
plt.scatter(eclassnumbers, eclassnumbers, marker='s', color='red', label='Eclass Numbers', s=50)
plt.xlabel('Eclass Number')
plt.ylabel('Prediction')
plt.title('Predictions vs. Eclass Numbers')
plt.legend()
plt.grid(True)
plt.show()
