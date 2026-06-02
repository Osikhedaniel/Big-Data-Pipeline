from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    window,
    count,
    col,
    to_timestamp
)


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

def write_to_console(
    df: DataFrame,
    output_mode: str = "append"
):

    return (
        df.writeStream
        .format("console")
        .outputMode(output_mode)
        .start()
    )

# def write_to_snowflake():
#     pass

# def write_to_postgres():
#     pass

# def write_to_s3():
#     pass

def write_to_dashboard_topic():
    pass