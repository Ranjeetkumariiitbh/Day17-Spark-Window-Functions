# Spark Execution Concepts: Window Functions

## 1. Transformations vs actions
Every function in `src/window_functions.py` only *describes* work: `withColumn`,
`filter`, `orderBy` are lazy transformations that extend the logical plan.
Nothing runs until an action (`show`, `count`, `collect`, `write`). In `main.py`
each `.show()` / `.count()` launches a separate job.

## 2. What a window function needs
A window spec has up to three parts:

| Part | API | Purpose |
|------|-----|---------|
| Partitioning | `Window.partitionBy("customer_id")` | Independent groups; ranking restarts in each |
| Ordering | `.orderBy(F.col("date").desc())` | Row order *inside* a partition |
| Frame | `.rowsBetween(...)` / `.rangeBetween(...)` | Which rows an aggregate sees |

- `row_number`, `rank`, `dense_rank`, `lag`, `lead` **require** an ordering.
- Unlike `groupBy`, a window keeps every input row and adds a column.
- Ranking functions and `lag`/`lead` ignore custom frames; frames matter for
  aggregates such as `sum().over(w)`. With an `orderBy` and no explicit frame,
  aggregates default to `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
  (a running total).

## 3. Ranking functions on ties
Scores 92, 92, 88, 84, 80:

| score | row_number | rank | dense_rank |
|-------|-----------|------|-----------|
| 92 | 1 | 1 | 1 |
| 92 | 2 | 1 | 1 |
| 88 | 3 | 3 | 2 |
| 84 | 4 | 4 | 3 |
| 80 | 5 | 5 | 4 |

- `row_number` is always unique, so add a tie-breaker column or the winner among
  equal rows is arbitrary and can change between runs.
- `rank` leaves gaps after ties; `dense_rank` never does.
- "Top 3" therefore depends on the function: `rank <= 3` gives 3 rows here,
  `dense_rank <= 3` gives 4 (everyone in the top three distinct scores).

## 4. lag / lead
`lag(col, n)` reads n rows *before* the current row in window order,
`lead(col, n)` n rows *after*. The first/last row of each partition gets `null`
(or a default if one is supplied). Subtracting them gives day-over-day change,
and `datediff` on a lagged date gives gaps between events.

## 5. What happens physically
Run `ranked.explain(mode="formatted")`. The plan has this shape (read bottom-up):

```
Window [rank(...), dense_rank(...)]     <- compute ranking per partition
+- Sort [department, salary DESC]       <- order rows inside each partition
   +- Exchange hashpartitioning(department, 4)   <- SHUFFLE
      +- Scan csv
```

- **Exchange (shuffle):** all rows sharing a `partitionBy` key must land in the
  same task, so Spark hash-partitions by that key. This is the expensive step:
  network + disk I/O.
- **Sort:** rows are sorted within each shuffle partition by (partition keys,
  order keys).
- **Window:** a single streaming pass over the sorted rows.
- Two specs with the *same* `partitionBy` but different `orderBy` (as in
  `rank_employees_by_salary`) reuse the one Exchange but add an extra Sort +
  Window node. Specs with *different* partition keys add another Exchange.
- **Skew:** one hot key (a customer with millions of rows) means one task does
  most of the work.
- **Always partition.** `Window.orderBy(...)` alone moves every row into a
  single partition and Spark logs a "No Partition Defined for Window operation"
  warning. Fine for samples, fatal at scale.

## 6. Filtering after a window
`filter(rn == 1)` runs *after* the window because it depends on the computed
column, so Spark cannot push it below the shuffle. The full partition is still
shuffled and sorted, then discarded. Cheaper alternatives when only the
newest row is needed:
- `groupBy(key).agg(max(struct(date, *cols)))` avoids the sort of all rows.
- Filter rows that cannot qualify (e.g. old dates) *before* the window.

## 7. Partition counts
- `df.rdd.getNumPartitions()` on the input reflects file splits / cores.
- After the window it equals `spark.sql.shuffle.partitions` (set to 4 here;
  default 200), unless Adaptive Query Execution coalesces small partitions.
- On 12 rows, most shuffle partitions are empty: that is why tuning
  `shuffle.partitions` matters even for tiny data.

## 8. Where to look in the Spark UI (http://localhost:4040 while running)
- **Jobs tab:** one job per action.
- **Stages tab:** a shuffle splits work into two stages (map side writes
  shuffle files, reduce side reads them). Check Shuffle Write / Read sizes.
- **SQL tab:** the plan graph with Exchange, Sort and Window nodes and row counts.
