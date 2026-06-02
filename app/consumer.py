from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)
from pyspark.sql import SparkSession
import pyspark

def get_gps_schema() -> StructType:
    return StructType([
        StructField("vehicle_id", StringType(), True),
        StructField("delivery_id", StringType(), True),
        StructField("driver_name", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("speed", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])

def create_spark_session(app_name: str) -> SparkSession:
    # Dynamically find your installed PySpark version to prevent version conflicts
    spark_version = pyspark.__version__
    
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version}")
        .getOrCreate()
    )

def read_kafka_stream(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str
):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .load()
    )
