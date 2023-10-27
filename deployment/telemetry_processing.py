import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,to_json,col,lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

REDPANDA_TOPIC_IN = 'telemetry-in'
REDPANDA_TOPIC_OUT = 'telemetry-out'


sensor_schema = StructType([StructField("sensor1_temp", StringType()), \
                            StructField("sensor2_temp",StringType()) ,  \
                            StructField("sensor3_temp",StringType()) ,  \
                            StructField("sensor4_temp",StringType())]) \

if __name__ == '__main__':

    #Stage 1: Open Spark Session
    spark = SparkSession \
        .builder \
        .appName("SparkStructredStream") \
        .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3")\
        .getOrCreate()


    #Stage 2: Read incoming IoT telemetry data stream from Redpanda topic
    kafka_in_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "redpanda-0:9092") \
        .option("subscribe", REDPANDA_TOPIC_IN) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss","false") \
        .load()


    #Stage 3: Perfrom average temperature calculation on dataframe and reformat as JSON string
    sensor_reading_dataframe = kafka_in_stream.withColumn("value",from_json(col("value").cast("string"), sensor_schema)) \
                                            .select(col("value.*")) \
                                            .select("sensor1_temp", "sensor2_temp" , "sensor3_temp", "sensor4_temp" ,\
                                                ((col("sensor1_temp") + col("sensor2_temp") + col("sensor3_temp") + col("sensor4_temp")) / lit(4)).alias("avg").cast("String")) \
                                            .selectExpr('to_json(struct(*)) as value')


    #Stage 4: Stream out the processed dataframe to console and another Redpanda topic
    kafka_out_stream = sensor_reading_dataframe.writeStream \
        .outputMode("append") \
        .option("truncate", False) \
        .format("console") \
        .start()

    kafka_out_stream = sensor_reading_dataframe.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "redpanda-0:9092") \
        .option("topic", REDPANDA_TOPIC_OUT) \
        .option("checkpointLocation","/tmp") \
        .start() \
        .awaitTermination()
