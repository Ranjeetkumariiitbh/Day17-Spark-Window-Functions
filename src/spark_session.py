"""SparkSession factory for the Day 17 window-function exercises."""
from pyspark.sql import SparkSession


def get_spark(app_name: str = "Day17-Spark-Window-Functions",
              shuffle_partitions: int = 4) -> SparkSession:
    """Create a local SparkSession.

    spark.sql.shuffle.partitions defaults to 200, which is wasteful for
    tiny sample data: every window's partitionBy triggers a shuffle into
    that many partitions. 4 keeps the local plans and Spark UI readable.
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
