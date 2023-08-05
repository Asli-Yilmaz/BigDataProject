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
# Define a UDF to encode the 'value' column to UTF-8 format
def encode_utf8(value):
    print(value.encode('utf-8'))
    return value.encode('utf-8')

# Register the UDF
'''encode_utf8_udf = udf(encode_utf8, StringType())

# Cast the 'value' column to a string and then encode it to utf-8 using the UDF
df = df.withColumn("encoded_value", encode_utf8_udf(col("value").cast("string")))
df = df.drop("value").withColumnRenamed("encoded_value", "value")'''

schema_csv = "id LONG, description STRING, eclassnumber LONG"
df = df.select(from_csv(col("value").cast("string"), schema_csv).alias("data"), "timestamp")
df = df.select("data.*", "timestamp")

parquet_output_path="C:/bigData"
data_write_stream = df \
    .writeStream \
    .trigger(processingTime='5 seconds') \
    .outputMode("append") \
    .foreachBatch(lambda df, batchId: df.write.parquet(parquet_output_path+ "/batch_" + str(batchId), mode="overwrite")) \
    .start()
    
data_write_stream.stop()

# '''data_write_stream=df\
#     .writeStream\
#     .trigger(processingTime='7 seconds')\
#     .outputMode("append")\
#     .option("truncate","False")\
#     .foreachBatch(lambda df, batchId: df.write.csv(parquet_output_path + "/batch_" + str(batchId), mode="overwrite")) \
#     .queryName("abc")\
#     .start()
# '''
# # time.sleep(20)
# # # Stop the streaming query after the desired duration
# # data_write_stream.stop()
# # # Define the schema for the data
schema = StructType([
    StructField("id", LongType(), nullable=True),
    StructField("description", StringType(), nullable=True),
    StructField("eclassnumber", LongType(), nullable=True)
])

# Read the data from the Parquet file sink as a static DataFrame with the specified schema

static_df = spark.read.schema(schema).parquet("C:/bigData/batch_1")
static_df.show()
print("---------->column number:"+str(static_df.count()))
df=static_df.withColumn('length',length(static_df['description']))
df.show()
df.printSchema()

from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF,VectorAssembler,NGram

tokenizer=Tokenizer(inputCol='description', outputCol='token_text')
stop_remove=StopWordsRemover(inputCol='token_text',outputCol='token_stop')
ngram=NGram(n=5,inputCol='token_stop', outputCol='ngram')
count_vec=CountVectorizer(inputCol='ngram', outputCol='count_vec')
idf=IDF(inputCol='count_vec',outputCol='tf-idf')

transformed_df=VectorAssembler(inputCols=['id','length','tf-idf'],outputCol='features')

from pyspark.ml import Pipeline

df_pipe=Pipeline(stages=[tokenizer,stop_remove,ngram,count_vec,idf,transformed_df])

final_df=df_pipe.fit(df).transform(df).select('eclassnumber','features')
final_df.show()

final_df.describe().show()
train_data, test_data=final_df.randomSplit([0.7,0.3], seed=123)
train_data.describe().show()
test_data.describe().show()

from pyspark.ml.regression import LinearRegression
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator

ln=LinearRegression(labelCol='eclassnumber')
# Create a parameter grid for the estimator (optional if you want to tune hyperparameters)
param_grid = ParamGridBuilder().addGrid(ln.regParam, [0.0, 0.1, 0.01]).build()

# Create the CrossValidator with the estimator, evaluator, and parameter grid
crossval = CrossValidator(estimator=ln,
                          estimatorParamMaps=param_grid,
                          evaluator=RegressionEvaluator(labelCol='eclassnumber'),
                          numFolds=5)

# Run cross-validation to train the model
cv_model = crossval.fit(final_df)

# Get the best model from cross-validation
best_model = cv_model.bestModel

# Evaluate the model on the test set
test_predictions = best_model.transform(test_data)
evaluator = RegressionEvaluator(labelCol='eclassnumber')
test_results = evaluator.evaluate(test_predictions)
# Evaluate the model on the test set
evaluator = RegressionEvaluator(labelCol='eclassnumber')
mae = evaluator.evaluate(test_predictions, {evaluator.metricName: 'mae'})
mse = evaluator.evaluate(test_predictions, {evaluator.metricName: 'mse'})
rmse = evaluator.evaluate(test_predictions, {evaluator.metricName: 'rmse'})
r2 = evaluator.evaluate(test_predictions, {evaluator.metricName: 'r2'})

# Print the evaluation metrics
print("MAE: ", mae)
print("MSE: ", mse)
print("RMSE: ", rmse)
print("R2: ", r2)
# Display the table with predicted values and actual eclassnumber
result_table = test_predictions.select('eclassnumber', format_number('prediction', 2).alias('prediction'))
result_table.show()
