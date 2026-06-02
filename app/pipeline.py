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
from metrics import estimate_eta
from stream import write_to_console

def build_pipeline():

    spark = create_spark_session(
        "LogisticsStreamingPipeline"
    )

    raw_stream = read_kafka_stream(
        spark=spark,
        bootstrap_servers="localhost:9092",
        topic="gps_updates"
    )

    gps_df = parse_json_stream(
        raw_stream,
        get_gps_schema()
    )

    gps_df = filter_valid_coordinates(gps_df)

    gps_df = remove_invalid_speed(gps_df)

    gps_df = add_delay_flag(gps_df)

    gps_df = estimate_eta(gps_df)

    query = write_to_console(gps_df)

    query.awaitTermination()


if __name__ == "__main__":
    build_pipeline()