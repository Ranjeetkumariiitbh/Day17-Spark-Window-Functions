"""CSV loading helpers."""
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from src import schemas

DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def read_csv(spark: SparkSession, filename: str, schema: StructType) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")          # fail loudly on malformed rows
        .option("dateFormat", "yyyy-MM-dd")
        .schema(schema)
        .csv(str(DATA_DIR / filename))
    )


def load_employees(spark): return read_csv(spark, "employees.csv", schemas.EMPLOYEE_SCHEMA)
def load_orders(spark): return read_csv(spark, "orders.csv", schemas.ORDER_SCHEMA)
def load_students(spark): return read_csv(spark, "students.csv", schemas.STUDENT_SCHEMA)
def load_policies(spark): return read_csv(spark, "policies.csv", schemas.POLICY_SCHEMA)
def load_route_trips(spark): return read_csv(spark, "route_trips.csv", schemas.ROUTE_TRIP_SCHEMA)
