"""Reusable window-function transformations.

Every function takes a DataFrame and returns a new DataFrame (transformations
are lazy; nothing runs until an action such as show()/count()/write is called).
"""
from typing import Optional, Sequence

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

_RANKING_FUNCS = {
    "row_number": F.row_number,
    "rank": F.rank,
    "dense_rank": F.dense_rank,
}


def rank_employees_by_salary(df: DataFrame) -> DataFrame:
    """Compare row_number / rank / dense_rank inside each department.

    row_number gets a name tie-breaker so its output is deterministic;
    rank and dense_rank deliberately use only salary so ties are visible.
    """
    w_unique = Window.partitionBy("department").orderBy(
        F.col("salary").desc(), F.col("name").asc()
    )
    w_ties = Window.partitionBy("department").orderBy(F.col("salary").desc())
    return (
        df.withColumn("row_number", F.row_number().over(w_unique))
        .withColumn("rank", F.rank().over(w_ties))
        .withColumn("dense_rank", F.dense_rank().over(w_ties))
    )


def latest_record_per_group(
    df: DataFrame,
    partition_col: str,
    order_cols: Sequence[str],
) -> DataFrame:
    """Keep exactly one row per partition: the newest by `order_cols` (desc).

    Pass a tie-breaker as the last order column (e.g. an id) so that two rows
    with the same date resolve deterministically.
    """
    w = Window.partitionBy(partition_col).orderBy(*[F.col(c).desc() for c in order_cols])
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def latest_order_per_customer(orders: DataFrame) -> DataFrame:
    return latest_record_per_group(orders, "customer_id", ["order_date", "order_id"])


def latest_policy_per_customer(policies: DataFrame) -> DataFrame:
    return latest_record_per_group(policies, "customer_id", ["start_date", "policy_id"])


def top_n_per_group(
    df: DataFrame,
    group_col: str,
    order_col: str,
    n: int = 3,
    method: str = "dense_rank",
    tie_breaker: Optional[str] = None,
) -> DataFrame:
    """Top-N rows per group, with a choice of ranking semantics.

    method="row_number": exactly N rows per group (ties cut arbitrarily
                         unless `tie_breaker` is given).
    method="rank":       ties share a rank, gaps follow (1,2,2,4); may return
                         more than N rows, but never skips past position N.
    method="dense_rank": ties share a rank, no gaps (1,2,2,3); returns every
                         row belonging to the top-N distinct scores.
    """
    if method not in _RANKING_FUNCS:
        raise ValueError(f"method must be one of {sorted(_RANKING_FUNCS)}, got {method!r}")

    order_exprs = [F.col(order_col).desc()]
    if method == "row_number" and tie_breaker:
        order_exprs.append(F.col(tie_breaker).asc())

    w = Window.partitionBy(group_col).orderBy(*order_exprs)
    return (
        df.withColumn("position", _RANKING_FUNCS[method]().over(w))
        .filter(F.col("position") <= n)
        .orderBy(group_col, "position")
    )


def top_students_per_course(students: DataFrame, n: int = 3, method: str = "dense_rank") -> DataFrame:
    return top_n_per_group(students, "course", "score", n=n, method=method, tie_breaker="name")


def add_lag_lead(
    df: DataFrame,
    partition_col: str,
    order_col: str,
    value_col: str,
) -> DataFrame:
    """Add previous/next value plus change and % change vs the previous row."""
    w = Window.partitionBy(partition_col).orderBy(order_col)
    prev_col, next_col = f"prev_{value_col}", f"next_{value_col}"
    return (
        df.withColumn(prev_col, F.lag(value_col, 1).over(w))
        .withColumn(next_col, F.lead(value_col, 1).over(w))
        .withColumn(f"{value_col}_change", F.col(value_col) - F.col(prev_col))
        .withColumn(
            f"{value_col}_pct_change",
            F.round((F.col(value_col) - F.col(prev_col)) / F.col(prev_col) * 100, 2),
        )
    )


def route_ticket_trend(trips: DataFrame) -> DataFrame:
    """Day-over-day ticket movement per route (lag/lead partitioned by route)."""
    return add_lag_lead(trips, "route", "trip_date", "tickets_sold")


def order_gaps_per_customer(orders: DataFrame) -> DataFrame:
    """Days between consecutive orders of the same customer (lag on date)."""
    w = Window.partitionBy("customer_id").orderBy("order_date", "order_id")
    return (
        orders.withColumn("prev_order_date", F.lag("order_date").over(w))
        .withColumn("next_order_date", F.lead("order_date").over(w))
        .withColumn("days_since_prev_order", F.datediff("order_date", "prev_order_date"))
    )
