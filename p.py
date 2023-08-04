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

from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF,VectorAssembler

tokenizer=Tokenizer(inputCol='description', outputCol='token_text')
stop_remove=StopWordsRemover(inputCol='token_text',outputCol='token_stop')
count_vec=CountVectorizer(inputCol='token_stop', outputCol='count_vec')
idf=IDF(inputCol='count_vec',outputCol='tf-idf')
transformed_df=VectorAssembler(inputcols=['length','tf_idf'],outputCol='features')

from pyspark.ml import Pipeline

df_pipe=Pipeline(stages=[tokenizer,stop_remove,count_vec,idf,transformed_df])

final_df=df_pipe.fit(df).transform(df).select('eclassnumber','features')
final_df.show()
# '''def process_stream(rdd):
#     squared_rdd = rdd.map(lambda x: int(x[1]) ** 2)
#     result = squared_rdd.collect()
#     print(result)

# data_write_stream.foreachRDD(process_stream)

# data_write_stream.start()
# data_write_stream.awaitTermination()

# time.sleep(20)
# data_write_stream.count()


# # Stop the streaming query after the initialization
# data_write_stream.stop()

# # Define the schema for the data
# schema = StructType([
#     StructField("id", LongType(), nullable=True),
#     StructField("description", StringType(), nullable=True),
#     StructField("eclassnumber", LongType(), nullable=True)
# ])

# # Read the data from the Parquet file sink as a static DataFrame with the specified schema
# static_df = spark.read.schema(schema).parquet(parquet_output_path)


# # Use static_df for your AI model or further processing
# static_df.show()
# static_df.count()

# # Don't forget to stop the SparkSession when you're done
# spark.stop()
# #data_write_stream.awaitTermination()
# df = spark.sql("SELECT * FROM abc")
# df.show()
# df.count()
# console_output_stream = df \
#     .writeStream \
#     .trigger(processingTime='5 seconds') \
#     .outputMode("update") \
#     .format("console") \
#     .option("truncate", "False") \
#     .start()
# time.sleep(20)
# console_output_stream.awaitTermination()'''
