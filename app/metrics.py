from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    round
)
from pyspark.sql.functions import (
    count,
    avg
)
from pyspark.sql import DataFrame

def estimate_eta(
    df: DataFrame,
    default_distance_km: float = 25
) -> DataFrame:

    return df.withColumn(
        "eta_minutes",
        round(
            (
                lit(default_distance_km)
                / col("speed")
            ) * 60,
            2
        )
    )

def calculate_summary_metrics(
    df: DataFrame
):

    return df.agg(
        count("*").alias("total_events"),
        avg("speed").alias("average_speed")
    )

