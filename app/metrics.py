from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    round,
    count,
    avg,
    sum,
    when,
    min,
    max,
    stddev,
    expr,
    countDistinct,
    mean,
    percentile_approx
)
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType


def estimate_eta(
    df: DataFrame,
    default_distance_km: float = 25
) -> DataFrame:
    """
    Estimate ETA in minutes based on current speed
    """
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


def calculate_summary_metrics(df: DataFrame):
    """
    Calculate comprehensive summary metrics
    """
    return df.agg(
        # Basic metrics
        count("*").alias("total_events"),
        avg("speed").alias("average_speed"),
        min("speed").alias("min_speed"),
        max("speed").alias("max_speed"),
        stddev("speed").alias("speed_stddev"),
        
        # Geographic metrics
        avg("latitude").alias("avg_latitude"),
        avg("longitude").alias("avg_longitude"),
        min("latitude").alias("min_latitude"),
        max("latitude").alias("max_latitude"),
        min("longitude").alias("min_longitude"),
        max("longitude").alias("max_longitude"),
        
        # Status metrics
        countDistinct("status").alias("unique_statuses"),
        countDistinct("vehicle_id").alias("unique_vehicles"),
        countDistinct("delivery_id").alias("unique_deliveries"),
        countDistinct("driver_name").alias("unique_drivers"),
        
        # ETA metrics
        avg("eta_minutes").alias("avg_eta_minutes"),
        min("eta_minutes").alias("min_eta_minutes"),
        max("eta_minutes").alias("max_eta_minutes")
    )


def calculate_status_breakdown(df: DataFrame):
    """
    Calculate breakdown by delivery status
    """
    return (
        df.groupBy("status")
        .agg(
            count("*").alias("count"),
            round(avg("speed"), 2).alias("avg_speed"),
            round(avg("eta_minutes"), 2).alias("avg_eta"),
            round(avg("latitude"), 4).alias("avg_latitude"),
            round(avg("longitude"), 4).alias("avg_longitude")
        )
        .orderBy(col("count").desc())
    )


def calculate_delay_metrics(df: DataFrame):
    """
    Calculate metrics related to delivery delays
    """
    return (
        df.groupBy("delay_flag")
        .agg(
            count("*").alias("count"),
            round(avg("speed"), 2).alias("avg_speed"),
            round(avg("eta_minutes"), 2).alias("avg_eta"),
            round(percentile_approx("speed", 0.5), 2).alias("median_speed"),
            round(percentile_approx("eta_minutes", 0.5), 2).alias("median_eta")
        )
    )


def calculate_vehicle_performance(df: DataFrame):
    """
    Calculate performance metrics per vehicle
    """
    return (
        df.groupBy("vehicle_id", "driver_name")
        .agg(
            count("*").alias("total_updates"),
            round(avg("speed"), 2).alias("avg_speed"),
            round(max("speed"), 2).alias("max_speed"),
            round(min("speed"), 2).alias("min_speed"),
            sum(when(col("delay_flag") == True, 1).otherwise(0)).alias("delayed_updates"),
            sum(when(col("delay_flag") == False, 1).otherwise(0)).alias("on_time_updates"),
            round(avg("eta_minutes"), 2).alias("avg_eta"),
            round(avg("latitude"), 4).alias("avg_latitude"),
            round(avg("longitude"), 4).alias("avg_longitude")
        )
        .withColumn(
            "delay_percentage",
            round((col("delayed_updates") / col("total_updates")) * 100, 2)
        )
        .orderBy(col("total_updates").desc())
    )


def calculate_status_transitions(df: DataFrame):
    """
    Calculate transitions between statuses for each delivery
    """
    # This requires window functions to track status changes
    from pyspark.sql.window import Window
    from pyspark.sql.functions import lag, lead
    
    window_spec = Window.partitionBy("delivery_id").orderBy("timestamp")
    
    return (
        df.withColumn("prev_status", lag("status", 1).over(window_spec))
        .withColumn("next_status", lead("status", 1).over(window_spec))
        .select("delivery_id", "vehicle_id", "timestamp", "status", "prev_status", "next_status")
        .filter(col("prev_status").isNotNull())
        .groupBy("prev_status", "status")
        .agg(count("*").alias("transition_count"))
        .orderBy(col("transition_count").desc())
    )


def calculate_spatial_metrics(df: DataFrame):
    """
    Calculate spatial distribution metrics
    """
    return (
        df.select(
            round(avg("latitude"), 4).alias("center_latitude"),
            round(avg("longitude"), 4).alias("center_longitude"),
            round(max("latitude") - min("latitude"), 4).alias("latitude_range"),
            round(max("longitude") - min("longitude"), 4).alias("longitude_range"),
            round(stddev("latitude"), 4).alias("latitude_spread"),
            round(stddev("longitude"), 4).alias("longitude_spread")
        )
    )


def calculate_time_based_metrics(df: DataFrame):
    """
    Calculate time-based metrics using timestamp
    """
    from pyspark.sql.functions import to_timestamp, hour, dayofweek, date_format
    
    # Add time columns
    df_with_time = df.withColumn("event_time", to_timestamp("timestamp")) \
                     .withColumn("hour_of_day", hour("event_time")) \
                     .withColumn("day_of_week", dayofweek("event_time")) \
                     .withColumn("date", date_format("event_time", "yyyy-MM-dd"))
    
    # Metrics by hour
    hourly_metrics = (
        df_with_time.groupBy("hour_of_day")
        .agg(
            count("*").alias("events"),
            round(avg("speed"), 2).alias("avg_speed"),
            sum(when(col("delay_flag") == True, 1).otherwise(0)).alias("delays")
        )
        .orderBy("hour_of_day")
    )
    
    return hourly_metrics


def calculate_safety_metrics(df: DataFrame):
    """
    Calculate safety-related metrics
    """
    return df.select(
        # Speeding events (assuming speed > 80 is excessive)
        sum(when(col("speed") > 80, 1).otherwise(0)).alias("speeding_events"),
        
        # Very slow events (assuming speed < 10 is too slow)
        sum(when(col("speed") < 10, 1).otherwise(0)).alias("slow_events"),
        
        # Fast average speed per vehicle
        round(avg("speed"), 2).alias("overall_avg_speed"),
        
        # Maximum speed observed
        round(max("speed"), 2).alias("max_speed_observed"),
        
        # Count of vehicles with speed > 100
        countDistinct(
            when(col("speed") > 100, col("vehicle_id"))
        ).alias("vehicles_exceeding_100kmh")
    )