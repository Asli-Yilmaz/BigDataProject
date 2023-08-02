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

path="C:\\bigData"
data_write_stream=df\
    .writeStream\
    .trigger(processingTime='5 seconds')\
    .outputMode("append")\
    .option("truncate","False")\
    .format("parquet")\
    .option("path", path) \
    .queryName("abc")\
    .start()
batch_df = spark.read.parquet(checkpointPath)
batch_df.write.mode("overwrite").csv(path)

#data_write_stream.awaitTermination()
df = spark.sql("SELECT * FROM abc")
df.show()
df.count()
'''console_output_stream = df \
    .writeStream \
    .trigger(processingTime='5 seconds') \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "False") \
    .start()
time.sleep(20)
console_output_stream.awaitTermination()'''
