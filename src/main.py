"""Day 17 - Spark Window Functions: run every scenario and inspect the results.

Run from the project root:
    python -m src.main
    # or
    spark-submit src/main.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src import data_loader as dl
from src import window_functions as wf
from src.spark_session import get_spark


def banner(title: str) -> None:
    print("\n" + "=" * 80 + f"\n{title}\n" + "=" * 80)


def main() -> None:
    spark = get_spark()

    employees = dl.load_employees(spark)
    orders = dl.load_orders(spark)
    students = dl.load_students(spark)
    policies = dl.load_policies(spark)
    trips = dl.load_route_trips(spark)

    banner("Schema + row counts (count() is an ACTION: it triggers a job)")
    for name, df in [("employees", employees), ("orders", orders), ("students", students),
                     ("policies", policies), ("route_trips", trips)]:
        print(f"{name}: {df.count()} rows")
    employees.printSchema()

    banner("1. row_number vs rank vs dense_rank per department")
    ranked = wf.rank_employees_by_salary(employees)
    ranked.orderBy("department", "row_number").show(truncate=False)

    banner("2. Latest order per customer (row_number desc, filter = 1)")
    wf.latest_order_per_customer(orders).orderBy("customer_id").show()

    banner("3. Top 3 students per course - three ranking semantics")
    for method in ("row_number", "rank", "dense_rank"):
        result = wf.top_students_per_course(students, n=3, method=method)
        print(f"--- method = {method} ({result.count()} rows) ---")
        result.show()

    banner("4. Latest policy per customer")
    wf.latest_policy_per_customer(policies).orderBy("customer_id").show()

    banner("5. lag / lead: ticket trend per route")
    wf.route_ticket_trend(trips).orderBy("route", "trip_date").show()

    banner("6. lag: days between consecutive orders per customer")
    wf.order_gaps_per_customer(orders).orderBy("customer_id", "order_date", "order_id").show()

    banner("7. Same 'latest order per customer' in Spark SQL")
    orders.createOrReplaceTempView("orders")
    spark.sql("""
        SELECT order_id, customer_id, order_date, amount
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY customer_id ORDER BY order_date DESC, order_id DESC) AS rn
            FROM orders)
        WHERE rn = 1
        ORDER BY customer_id
    """).show()

    banner("8. Execution plan: Exchange -> Sort -> Window")
    print("Look for: Exchange hashpartitioning(department, N), Sort, Window [...]")
    ranked.explain(mode="formatted")

    banner("9. Partition inspection around the shuffle")
    print("input partitions :", employees.rdd.getNumPartitions())
    print("window partitions:", ranked.rdd.getNumPartitions(),
          "(= spark.sql.shuffle.partitions, unless AQE coalesces them)")

    spark.stop()


if __name__ == "__main__":
    main()
