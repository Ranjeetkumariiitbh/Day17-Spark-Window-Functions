import datetime as dt

from src import window_functions as wf

D = dt.date


def rows(df, *cols):
    return [tuple(r[c] for c in cols) for r in df.collect()]


def test_ranking_functions_handle_ties_differently(spark):
    df = spark.createDataFrame(
        [("A", "x", 100), ("A", "y", 100), ("A", "z", 90), ("B", "w", 50)],
        ["department", "name", "salary"],
    )
    out = wf.rank_employees_by_salary(df).orderBy("department", "row_number")
    got = rows(out, "name", "row_number", "rank", "dense_rank")
    assert got == [
        ("x", 1, 1, 1),
        ("y", 2, 1, 1),
        ("z", 3, 3, 2),   # rank skips 2, dense_rank does not
        ("w", 1, 1, 1),   # ranking restarts in each partition
    ]


def test_latest_order_per_customer_breaks_same_day_tie_by_order_id(spark):
    df = spark.createDataFrame(
        [(1, "C1", D(2026, 1, 1), 10.0),
         (2, "C1", D(2026, 2, 1), 20.0),
         (3, "C1", D(2026, 2, 1), 30.0),
         (4, "C2", D(2026, 1, 5), 40.0)],
        ["order_id", "customer_id", "order_date", "amount"],
    )
    out = wf.latest_order_per_customer(df).orderBy("customer_id")
    assert rows(out, "customer_id", "order_id") == [("C1", 3), ("C2", 4)]


def test_top_n_methods_differ_on_ties(spark):
    df = spark.createDataFrame(
        [("Physics", "Gopal", 92), ("Physics", "Hema", 92), ("Physics", "Imran", 88),
         ("Physics", "Jaya", 84), ("Physics", "Kabir", 80)],
        ["course", "name", "score"],
    )
    by_method = {
        m: {r["name"] for r in wf.top_students_per_course(df, 3, m).collect()}
        for m in ("row_number", "rank", "dense_rank")
    }
    assert by_method["row_number"] == {"Gopal", "Hema", "Imran"}
    assert by_method["rank"] == {"Gopal", "Hema", "Imran"}          # rank 4 is excluded
    assert by_method["dense_rank"] == {"Gopal", "Hema", "Imran", "Jaya"}  # 3rd distinct score


def test_top_n_rejects_unknown_method(spark):
    df = spark.createDataFrame([("a", 1)], ["course", "score"])
    try:
        wf.top_n_per_group(df, "course", "score", method="ntile")
    except ValueError as exc:
        assert "method must be one of" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_latest_policy_per_customer(spark):
    df = spark.createDataFrame(
        [("P1", "C1", D(2023, 1, 1)), ("P2", "C1", D(2025, 6, 15)),
         ("P3", "C2", D(2025, 1, 1)), ("P4", "C2", D(2025, 1, 1))],
        ["policy_id", "customer_id", "start_date"],
    )
    out = wf.latest_policy_per_customer(df).orderBy("customer_id")
    assert rows(out, "customer_id", "policy_id") == [("C1", "P2"), ("C2", "P4")]


def test_lag_lead_edges_are_null_and_change_is_computed(spark):
    df = spark.createDataFrame(
        [("R", D(2026, 9, 1), 100), ("R", D(2026, 9, 2), 150), ("R", D(2026, 9, 3), 120)],
        ["route", "trip_date", "tickets_sold"],
    )
    out = wf.route_ticket_trend(df).orderBy("trip_date")
    got = rows(out, "prev_tickets_sold", "next_tickets_sold",
               "tickets_sold_change", "tickets_sold_pct_change")
    assert got == [
        (None, 150, None, None),
        (100, 120, 50, 50.0),
        (150, None, -30, -20.0),
    ]


def test_order_gaps_use_previous_order_of_same_customer_only(spark):
    df = spark.createDataFrame(
        [(1, "C1", D(2026, 1, 1)), (2, "C1", D(2026, 1, 11)), (3, "C2", D(2026, 1, 5))],
        ["order_id", "customer_id", "order_date"],
    )
    out = wf.order_gaps_per_customer(df).orderBy("order_id")
    assert rows(out, "order_id", "days_since_prev_order") == [(1, None), (2, 10), (3, None)]
