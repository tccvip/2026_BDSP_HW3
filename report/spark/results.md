# Spark HW3 results

Run: `20261107T093200113988276-20197`; UTC completion: 2026-11-07T09:32:25.240299+00:00.
Dataset: official MovieLens Latest Small, original CSV files in HDFS.
Environment: Python 3.12.16, Spark 3.5.9,
Java 1.8.0_504; master `local[*]`.
Hadoop client 3.3.4; default FS `hdfs://localhost:9000`.
Host memory and active Java/MongoDB processes are captured in the run log.

## Inputs and correctness

- Ratings CSV: 100836 rows.
- Movies CSV: 9742 rows.
- Ratings Parquet: 100836 rows.
- Rated movie groups: 9724.
- Movies retained with at least 20 ratings: 1297.
- Exact typed CSV/Parquet row multiset equality: True.
- Exact CSV/Parquet group/count equality in all trials: True.
- Parquet and JSON summary read-back equality: True.

Explicit input schemas (no inference pass):

```text
ratings: struct<userId:int,movieId:int,rating:double,timestamp:int>
movies: struct<movieId:int,title:string,genres:string>
```

## Movie summary sample

Sorted by rating_count descending, then movie_id ascending to break ties.

| movie_id | title | rating_count | avg_rating |
|---|---|---|---|
| 356 | Forrest Gump (1994) | 329 | 4.1641 |
| 318 | Shawshank Redemption, The (1994) | 317 | 4.4290 |
| 296 | Pulp Fiction (1994) | 307 | 4.1971 |
| 593 | Silence of the Lambs, The (1991) | 279 | 4.1613 |
| 2571 | Matrix, The (1999) | 278 | 4.1924 |
| 260 | Star Wars: Episode IV - A New Hope (1977) | 251 | 4.2311 |
| 480 | Jurassic Park (1993) | 238 | 3.7500 |
| 110 | Braveheart (1995) | 237 | 4.0316 |
| 589 | Terminator 2: Judgment Day (1991) | 224 | 3.9710 |
| 527 | Schindler's List (1993) | 220 | 4.2250 |

## HDFS paths

- ratings_csv: `hdfs://localhost:9000/user/bdsp/hw3/raw/ratings.csv`
- movies_csv: `hdfs://localhost:9000/user/bdsp/hw3/raw/movies.csv`
- ratings_parquet: `hdfs://localhost:9000/user/bdsp/hw3/curated/ratings_parquet`
- movie_stats_parquet: `hdfs://localhost:9000/user/bdsp/hw3/curated/movie_stats_parquet`
- movie_stats_json: `hdfs://localhost:9000/user/bdsp/hw3/curated/movie_stats_json`

## E2 benchmark

One SparkSession, local[*], four shuffle partitions, no persist/cache calls. Each timing includes input DataFrame construction, query planning, HDFS read, CSV parsing or Parquet decoding, groupBy/count, and collect() to the driver. CSV uses an explicit schema; no CSV schema inference is timed. Parquet's stored schema metadata is read during construction. Startup, exports, validation, result comparison, and artifact writing are outside timed trials. One warm-up per format precedes all six timed trials (CSV three, then Parquet three). Earlier validation/export scans and warm-ups can populate HDFS/OS caches; caches are not flushed. These are warm-run measurements, not cold-disk tests.

| Format | Phase | Trial | Seconds | Groups |
|---|---|---|---|---|
| csv | warmup | 1 | 0.236203 | 9724 |
| parquet | warmup | 1 | 0.201415 | 9724 |
| csv | timed | 1 | 0.249926 | 9724 |
| csv | timed | 2 | 0.208591 | 9724 |
| csv | timed | 3 | 0.232674 | 9724 |
| parquet | timed | 1 | 0.184764 | 9724 |
| parquet | timed | 2 | 0.172608 | 9724 |
| parquet | timed | 3 | 0.175566 | 9724 |

Only the three timed trials contribute to these statistics:

| Format | Mean (s) | Median (s) | Min (s) | Max (s) |
|---|---|---|---|---|
| csv | 0.230397 | 0.232674 | 0.208591 | 0.249926 |
| parquet | 0.177646 | 0.175566 | 0.172608 | 0.184764 |

CSV mean minus Parquet mean: 0.052751 seconds.
CSV mean / Parquet mean: 1.2969x (above 1 means Parquet faster).
CSV median / Parquet median: 1.3253x.

## Q3

CSV and Parquet represent the same 100836 typed ratings.
Writing a different physical representation must not add or remove logical records.
Grouping the same movieId values with count(*) must therefore produce the same
9724 groups and per-movie counts. This run checked full
typed row equality and compared actual aggregation mappings in every warm-up and
timed trial; any discrepancy causes a nonzero exit rather than being assumed away.

## Q4

In this run Parquet's mean query runtime was faster than CSV's:
0.177646 s versus 0.230397 s.
The observed mean ratio was 1.2969x; the table also reports medians and ranges
to show variability across only three trials. CSV requires text parsing, while
Parquet stores typed columns and can read the movieId column for this query.
Those properties are possible contributors, not isolated causal measurements.
CSV schema inference and Spark startup were excluded, so they cannot explain the
timed differences here. Warm-ups and earlier scans can benefit HDFS and operating-system
caches. The small dataset, scheduling/JIT overhead, sequential format order,
other host workloads, and measurement variability limit generalization; these
results do not establish that Parquet is always faster. No cache-hit rates or
individual overhead components were measured.
