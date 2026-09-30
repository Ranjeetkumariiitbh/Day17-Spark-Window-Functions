"""Explicit schemas: no inferSchema, so loading is a single cheap pass."""
from pyspark.sql.types import (
    DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType,
)

EMPLOYEE_SCHEMA = StructType([
    StructField("emp_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("department", StringType(), False),
    StructField("salary", IntegerType(), False),
    StructField("hire_date", DateType(), True),
])

ORDER_SCHEMA = StructType([
    StructField("order_id", LongType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_date", DateType(), False),
    StructField("amount", DoubleType(), False),
])

STUDENT_SCHEMA = StructType([
    StructField("student_id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("course", StringType(), False),
    StructField("score", IntegerType(), False),
])

POLICY_SCHEMA = StructType([
    StructField("policy_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("policy_type", StringType(), False),
    StructField("start_date", DateType(), False),
    StructField("premium", IntegerType(), False),
    StructField("status", StringType(), False),
])

ROUTE_TRIP_SCHEMA = StructType([
    StructField("route", StringType(), False),
    StructField("trip_date", DateType(), False),
    StructField("tickets_sold", IntegerType(), False),
    StructField("revenue", DoubleType(), False),
])
