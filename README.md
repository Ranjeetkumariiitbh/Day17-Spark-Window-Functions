# Day17-Spark-Window-Functions

Hands-on PySpark practice for **window functions**: `row_number`, `rank`,
`dense_rank`, `lag`, `lead`. The project partitions data by department,
customer, course and route, then solves real scenarios such as the latest
record per customer, Top 3 students per course and latest policy per customer.

## Scenarios

| # | Scenario | Partition by | Order by | Functions |
|---|----------|--------------|----------|-----------|
| 1 | Salary ranking with ties | `department` | `salary desc` | row_number, rank, dense_rank |
| 2 | Latest order per customer | `customer_id` | `order_date desc, order_id desc` | row_number |
| 3 | Top 3 students per course | `course` | `score desc` | row_number / rank / dense_rank |
| 4 | Latest policy per customer | `customer_id` | `start_date desc, policy_id desc` | row_number |
| 5 | Ticket trend per route | `route` | `trip_date` | lag, lead |
| 6 | Days between orders | `customer_id` | `order_date, order_id` | lag, lead, datediff |
| 7 | Latest order in Spark SQL | `customer_id` | `order_date desc` | `ROW_NUMBER() OVER (...)` |

## Project structure

```
Day17-Spark-Window-Functions/
├── data/                       # sample CSVs (employees, orders, students, policies, route_trips)
├── src/
│   ├── spark_session.py        # local SparkSession, shuffle partitions = 4
│   ├── schemas.py              # explicit StructType schemas
│   ├── data_loader.py          # CSV loaders (FAILFAST, no schema inference)
│   ├── window_functions.py     # reusable window transformations
│   └── main.py                 # runs every scenario, shows results and the plan
├── tests/
│   ├── conftest.py             # shared SparkSession fixture
│   └── test_window_functions.py
├── docs/SPARK_EXECUTION_CONCEPTS.md
├── requirements.txt
└── README.md
```

## Setup

Requires Python 3.9+ and Java 17 or 21 (needed by Spark).

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Run

```bash
python -m src.main               # all scenarios + explain plan
pytest -v                        # unit tests on tiny in-memory data
```

While `main.py` runs, the Spark UI is at http://localhost:4040.

## Expected results (worked out by hand from the sample data)

**Engineering salaries** (tie at 120000):

| name | salary | row_number | rank | dense_rank |
|------|--------|-----------|------|-----------|
| Bhanu | 135000 | 1 | 1 | 1 |
| Asha | 120000 | 2 | 2 | 2 |
| Chitra | 120000 | 3 | 2 | 2 |
| Dev | 95000 | 4 | 4 | 3 |

**Latest order per customer:** C001 → 1003, C002 → 1005, C003 → 1008, C004 → 1009.

**Latest policy per customer:** C001 → P003, C002 → P005, C003 → P006,
C004 → P008 (P007 and P008 share a start date; the higher `policy_id` wins).

**Top 3 students per course** (row count depends on the ranking function):

| Method | Math | Physics | CS | Total |
|--------|------|---------|----|-------|
| row_number | 3 | 3 | 3 | 9 |
| rank | 4 (two tied at 91) | 3 | 3 | 10 |
| dense_rank | 4 | 4 (Jaya, 3rd distinct score) | 3 | 11 |

**BUS-101 ticket trend:**

| trip_date | tickets_sold | prev | next | change | pct_change |
|-----------|-------------|------|------|--------|-----------|
| 2026-09-01 | 120 | null | 135 | null | null |
| 2026-09-02 | 135 | 120 | 110 | 15 | 12.5 |
| 2026-09-03 | 110 | 135 | 150 | -25 | -18.52 |
| 2026-09-04 | 150 | 110 | 150 | 40 | 36.36 |
| 2026-09-05 | 150 | 150 | null | 0 | 0.0 |

## Key takeaways

- `row_number` is unique, `rank` leaves gaps after ties, `dense_rank` does not.
- Always add a deterministic tie-breaker to `row_number` before filtering on it.
- `partitionBy` triggers a shuffle (`Exchange hashpartitioning`), then a `Sort`
  and a `Window` operator. Never use a window with no `partitionBy` on big data.
- Filtering on a window result cannot be pushed below the window.

Full write-up: [docs/SPARK_EXECUTION_CONCEPTS.md](docs/SPARK_EXECUTION_CONCEPTS.md)

## Push to GitHub

```bash
git init -b main
git add .
git commit -m "Day 17: Spark window functions (row_number, rank, dense_rank, lag, lead)"

# Create an empty repo named Day17-Spark-Window-Functions on github.com, then:
git remote add origin https://github.com/<your-username>/Day17-Spark-Window-Functions.git
git push -u origin main
```
