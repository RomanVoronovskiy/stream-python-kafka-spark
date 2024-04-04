import os

from pyspark.sql import SparkSession
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 pyspark-shell'

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("ProduceConsoleApp") \
        .getOrCreate()

    source = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", "127.0.0.1:9092")
              .option("subscribe", "stream_topic")
              .option("startingOffsets", "earliest")
              .load()
              )
    source.printSchema()
    df = (source
          .selectExpr('CAST(value AS STRING)', 'offset'))

    console = (df
               .writeStream
               .format('console'))

    console.start().awaitTermination()
