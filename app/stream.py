from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    to_json,
    struct,
    window,
    count,
    col,
    to_timestamp
)
import psycopg2
from psycopg2.extras import execute_values
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Windowed metrics
def deliveries_per_window(
    df: DataFrame,
    window_duration: str = "5 minutes"
) -> DataFrame:

    timestamp_df = df.withColumn(
        "event_time",
        to_timestamp("timestamp")
    )

    return (
        timestamp_df
        .groupBy(
            window(
                col("event_time"),
                window_duration
            )
        )
        .agg(
            count("*").alias("delivery_count")
        )
    )


# Consil sink
def write_to_console(
    df: DataFrame,
    output_mode: str = "append"
):
    return (
        df.writeStream
        .format("console")
        .outputMode(output_mode)
        .option("truncate", False)
        .option("numRows", 10)
        .start()
    )


# Postgres sink
def write_to_postgres(
    df: DataFrame,
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    table_name: str
):
    def write_batch(batch_df, batch_id):
        try:
            # Convert to Pandas for easier handling
            pdf = batch_df.toPandas()
            
            if pdf.empty:
                logger.info(f"Batch {batch_id}: Empty batch, skipping")
                return
            
            logger.info(f"Batch {batch_id}: Processing {len(pdf)} records")
            
            # Create connection
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password
            )
            
            cursor = conn.cursor()
            
            # Prepare the insert query
            insert_query = f"""
            INSERT INTO {table_name}
            (
                vehicle_id,
                delivery_id,
                driver_name,
                latitude,
                longitude,
                speed,
                status,
                timestamp,
                delay_flag,
                eta_minutes
            )
            VALUES %s
            """
            
            # Prepare data as list of tuples
            data = [
                (
                    row.vehicle_id if hasattr(row, 'vehicle_id') else None,
                    row.delivery_id if hasattr(row, 'delivery_id') else None,
                    row.driver_name if hasattr(row, 'driver_name') else None,
                    float(row.latitude) if hasattr(row, 'latitude') and row.latitude is not None else None,
                    float(row.longitude) if hasattr(row, 'longitude') and row.longitude is not None else None,
                    float(row.speed) if hasattr(row, 'speed') and row.speed is not None else None,
                    row.status if hasattr(row, 'status') else None,
                    row.timestamp if hasattr(row, 'timestamp') else None,
                    bool(row.delay_flag) if hasattr(row, 'delay_flag') and row.delay_flag is not None else False,
                    float(row.eta_minutes) if hasattr(row, 'eta_minutes') and row.eta_minutes is not None else None
                )
                for _, row in pdf.iterrows()
            ]
            
            # Execute batch insert
            execute_values(cursor, insert_query, data)
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Batch {batch_id}: Successfully inserted {len(data)} records")
            
        except Exception as e:
            logger.error(f"Batch {batch_id}: Error - {str(e)}")

    return (
        df.writeStream
        .foreachBatch(write_batch)
        .outputMode("append")
        .option(
            "checkpointLocation",
            "/tmp/checkpoints/postgres"
        )
        .start()
    )


# Dashboard sink
def write_to_dashboard_topic(
    df,
    kafka_server,
    topic
):
    # Convert DataFrame to JSON
    kafka_df = df.select(
        to_json(struct("*")).alias("value")
    )
    
    return (
        kafka_df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_server)
        .option("topic", topic)
        .option("checkpointLocation", "/tmp/checkpoints/dashboard")
        .outputMode("append")
        .start()
    )