import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# ===============================================================================
# CONFIGURATION DYNAMIQUE DE L'ENVIRONNEMENT DE PRODUCTION (WINDOWS COMPATIBILITY)
# ===============================================================================
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
os.environ["HADOOP_HOME"] = os.path.join(BASE_DIR, "hadoop")
os.environ["PATH"] += os.path.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")

# Configuration des paramètres réseau et de stockage
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "industrial-incidents"
OUTPUT_DIR = "data/raw_logs"
CHECKPOINT_DIR = "data/checkpoints"

def main():
    # Initialisation de la SparkSession avec bridage JVM pour préserver la RAM
    spark = SparkSession.builder \
        .appName("FactoryIncidentConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.driver.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    # Niveau de log limité aux avertissements pour éviter de polluer le terminal
    spark.sparkContext.setLogLevel("WARN")
    print(" Initializing PySpark Structured Streaming Consumer...")

    # Définition du contrat d'interface (Schema)
    log_schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("unit_id", IntegerType(), True),
        StructField("cycle", IntegerType(), True),
        StructField("error_code", StringType(), True),
        StructField("component", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("log_message", StringType(), True)
    ])

    # Ingestion du flux Kafka
    kafka_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Désérialisation et structuration
    parsed_stream_df = kafka_stream_df \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), log_schema).alias("data")) \
        .select("data.*")

    # Console Sink (Monitoring en temps réel)
    console_query = parsed_stream_df.writeStream \
        .trigger(processingTime="2 seconds") \
        .format("console") \
        .outputMode("append") \
        .start()

    # File Sink (Stockage physique en Parquet compressé)
    file_query = parsed_stream_df.writeStream \
        .trigger(processingTime="5 seconds") \
        .format("parquet") \
        .option("path", OUTPUT_DIR) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .outputMode("append") \
        .start()

    print(" Connection established. Waiting for incoming stream events...")
    
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n Shutting down stream processing engine safely...")
        console_query.stop()
        file_query.stop()

if __name__ == "__main__":
    main()