from consumer import (
    get_gps_schema,
    create_spark_session,
    read_kafka_stream,
)

from transform import (
    parse_json_stream,
    add_delay_flag,
    filter_valid_coordinates,
    remove_invalid_speed
)

from metrics import estimate_eta, build_metrics_report, save_metrics_report_to_json

from stream import (
    write_to_console,
    write_to_postgres,
    write_to_dashboard_topic
)

from dotenv import load_dotenv
import os
import logging
from pyspark.sql.functions import to_json, struct, col, lit

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Validate required environment variables
required_vars = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"]
missing_vars = [var for var in required_vars if not os.getenv(var)]
if missing_vars:
    logger.error(f"Missing required environment variables: {missing_vars}")
    logger.error("Please check your .env file")
    exit(1)


def build_pipeline():
    """
    Build and run the complete streaming pipeline with multiple sinks:
    1. PostgreSQL sink for persistent storage
    2. Kafka sink for real-time dashboard
    3. Console sink for debugging
    """
    
    logger.info("Starting Logistics Streaming Pipeline")
    
    # Creat SparkSession
    try:
        spark = create_spark_session("LogisticsStreamingPipeline")
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created successfully")
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        return

    # Read from kafka
    try:
        raw_stream = read_kafka_stream(
            spark=spark,
            bootstrap_servers="localhost:9092",
            topic="gps_updates"
        )
        logger.info("Kafka stream created successfully")
    except Exception as e:
        logger.error(f"Failed to create Kafka stream: {e}")
        spark.stop()
        return

    # Transform data
    try:
        # Parse JSON data
        gps_df = parse_json_stream(
            raw_stream,
            get_gps_schema()
        )
        
        # Apply transformations
        gps_df = filter_valid_coordinates(gps_df)
        gps_df = remove_invalid_speed(gps_df)
        gps_df = add_delay_flag(gps_df)
        gps_df = estimate_eta(gps_df)

        # Metric_type field for the dashboard to identify data type
        gps_df = gps_df.withColumn("data_type", lit("gps_update"))

        # Ensures correct types
        gps_df = gps_df.withColumn("speed", col("speed").cast("double"))
        gps_df = gps_df.withColumn("latitude", col("latitude").cast("double"))
        gps_df = gps_df.withColumn("longitude", col("longitude").cast("double"))
        gps_df = gps_df.withColumn("eta_minutes", col("eta_minutes").cast("double"))
        gps_df = gps_df.withColumn("delay_flag", col("delay_flag").cast("boolean"))
        
        logger.info("Transformations applied successfully")
    except Exception as e:
        logger.error(f"Failed to apply transformations: {e}")
        spark.stop()
        return

    def write_metrics_batch(batch_df, batch_id):
        try:
            if batch_df.rdd.isEmpty():
                logger.info(f"Batch {batch_id}: no rows to generate metrics report")
                return

            report = build_metrics_report(batch_df)
            report_path = save_metrics_report_to_json(
                report,
                filename=f"metrics_reports/metrics_report_{batch_id}.json"
            )
            logger.info(f"Batch {batch_id}: metrics report saved to {report_path}")
        except Exception as e:
            logger.error(f"Batch {batch_id}: failed to generate metrics report - {e}")

    # Starting all sinks
    queries = []
    
    # Metrics report sink
    try:
        logger.info("Starting Metrics report sink...")
        metrics_query = (
            gps_df.writeStream
            .foreachBatch(write_metrics_batch)
            .outputMode("append")
            .option("checkpointLocation", "/tmp/checkpoints/metrics_report")
            .start()
        )
        queries.append(metrics_query)
        logger.info("Metrics report sink started successfully")
    except Exception as e:
        logger.error(f"Failed to start Metrics report sink: {e}")

    # Postgres
    try:
        logger.info("Starting PostgreSQL sink...")
        postgres_query = write_to_postgres(
            df=gps_df,
            host=DB_HOST,
            port=int(DB_PORT),
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            table_name="vehicle_tracking"
        )
        queries.append(postgres_query)
        logger.info("PostgreSQL sink started successfully")
    except Exception as e:
        logger.error(f"Failed to start PostgreSQL sink: {e}")

    # Dashboard kafka topic
    try:
        logger.info("Starting Dashboard Kafka sink...")
        dashboard_query = write_to_dashboard_topic(
            gps_df,
            kafka_server="localhost:9092",
            topic="dashboard_stream"
        )
        queries.append(dashboard_query)
        logger.info("Dashboard Kafka sink started successfully")
    except Exception as e:
        logger.error(f"Failed to start Dashboard Kafka sink: {e}")

    # Console (for debugging)
    try:
        logger.info("Starting Console sink...")
        console_query = write_to_console(gps_df)
        queries.append(console_query)
        logger.info("Console sink started successfully")
    except Exception as e:
        logger.error(f"Failed to start Console sink: {e}")

    if not queries:
        logger.error("No sinks were started successfully. Shutting down.")
        spark.stop()
        return

    logger.info(f"All sinks started ({len(queries)} total)")
    logger.info("Streaming pipeline is now running...")
    logger.info("Press Ctrl+C to stop")

    # Terminate sinks (ctrl + c)
    try:
        for query in queries:
            query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal. Stopping all streams...")
        for query in queries:
            try:
                query.stop()
                logger.info(f"Stopped query: {query.name}")
            except Exception as e:
                logger.error(f"Error stopping query: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in stream processing: {e}")
        for query in queries:
            try:
                query.stop()
            except:
                pass
    finally:
        logger.info("Shutting down Spark session...")
        spark.stop()
        logger.info("Pipeline shutdown complete")


if __name__ == "__main__":
    build_pipeline()