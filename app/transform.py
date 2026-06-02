from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    from_json,
    col,
    when
)

def parse_json_stream(
    df: DataFrame,
    schema
) -> DataFrame:

    parsed_df = df.select(
        from_json(
            col("value").cast("string"),
            schema
        ).alias("data")
    )

    return parsed_df.select("data.*")

def add_delay_flag(df: DataFrame) -> DataFrame:

    return df.withColumn(
        "delay_flag",
        when(
            col("speed") < 15,
            "Delayed"
        ).otherwise("On Schedule")
    )

def filter_valid_coordinates(df: DataFrame) -> DataFrame:

    return df.filter(
        (col("latitude").isNotNull()) &
        (col("longitude").isNotNull())
    )

def remove_invalid_speed(df: DataFrame) -> DataFrame:

    return df.filter(
        col("speed") >= 0
    )