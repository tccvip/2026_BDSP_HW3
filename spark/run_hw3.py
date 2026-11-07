#!/usr/bin/env python3
"""HW3 batch processing, output validation, and CSV/Parquet experiment E2."""

import argparse
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import platform
import statistics
import time

from pyspark.sql import SparkSession, functions as F, types as T


ROOT = Path(__file__).resolve().parents[1]
BASE = "hdfs://localhost:9000/user/bdsp/hw3"
PATHS = {
    "ratings_csv": f"{BASE}/raw/ratings.csv",
    "movies_csv": f"{BASE}/raw/movies.csv",
    "ratings_parquet": f"{BASE}/curated/ratings_parquet",
    "movie_stats_parquet": f"{BASE}/curated/movie_stats_parquet",
    "movie_stats_json": f"{BASE}/curated/movie_stats_json",
}
RATINGS_SCHEMA = T.StructType([
    T.StructField("userId", T.IntegerType()),
    T.StructField("movieId", T.IntegerType()),
    T.StructField("rating", T.DoubleType()),
    T.StructField("timestamp", T.IntegerType()),
])
MOVIES_SCHEMA = T.StructType([
    T.StructField("movieId", T.IntegerType()),
    T.StructField("title", T.StringType()),
    T.StructField("genres", T.StringType()),
])
POLICY = (
    "One SparkSession, local[*], four shuffle partitions, no persist/cache calls. "
    "Each timing includes input DataFrame construction, query planning, HDFS read, "
    "CSV parsing or Parquet decoding, groupBy/count, and collect() to the driver. "
    "CSV uses an explicit schema; no CSV schema inference is timed. Parquet's stored "
    "schema metadata is read during construction. Startup, exports, validation, "
    "result comparison, and artifact writing are outside timed trials. "
    "One warm-up per format precedes all six timed trials (CSV three, then Parquet three). "
    "Earlier validation/export scans and warm-ups can populate HDFS/OS caches; "
    "caches are not flushed. These are warm-run measurements, not cold-disk tests."
)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_csv(spark, path, schema):
    return (spark.read.schema(schema).option("header", True)
            .option("escape", '"').option("enforceSchema", False)
            .option("mode", "FAILFAST").csv(path))


def aggregate(frame):
    return frame.groupBy("movieId").agg(F.count("*").alias("rating_count"))


def as_counts(rows):
    return {row.movieId: row.rating_count for row in rows}


def benchmark(spark, expected):
    records = []
    outputs = {}
    # Both formats are warmed up before any measured trial.
    for phase, runs in (("warmup", 1), ("timed", 3)):
        for format_name in ("csv", "parquet"):
            for trial in range(1, runs + 1):
                start = time.perf_counter()
                if format_name == "csv":
                    frame = read_csv(spark, PATHS["ratings_csv"], RATINGS_SCHEMA)
                else:
                    frame = spark.read.parquet(PATHS["ratings_parquet"])
                rows = aggregate(frame).collect()
                elapsed = time.perf_counter() - start
                actual = as_counts(rows)
                require(actual == expected, f"{format_name} {phase} {trial}: aggregate mismatch")
                outputs[format_name] = actual
                record = {"format": format_name, "phase": phase, "trial": trial,
                          "elapsed_seconds": elapsed, "groups": len(rows),
                          "matches_reference": True}
                records.append(record)
                print("BENCHMARK", json.dumps(record), flush=True)
    require(outputs["csv"] == outputs["parquet"], "CSV/Parquet aggregates differ")
    return records


def save_artifacts(summary, trials):
    report_dir = ROOT / "report/spark"
    report_dir.mkdir(parents=True, exist_ok=True)
    # Archive each execution as well as publishing the required latest results.
    archive = report_dir / "runs" / summary["run_id"]
    archive.mkdir(parents=True, exist_ok=False)
    stats = summary["statistics"]
    difference = summary["comparison"]["csv_mean_minus_parquet_mean_seconds"]
    speedup = summary["comparison"]["csv_mean_divided_by_parquet_mean"]
    direction = "faster" if difference > 0 else "slower"
    sample = "\n".join(
        f"| {r['movie_id']} | {r['title'].replace('|', '&#124;')} | {r['rating_count']} | {r['avg_rating']:.4f} |"
        for r in summary["movie_summary_sample"]
    )
    trial_table = "\n".join(
        f"| {r['format']} | {r['phase']} | {r['trial']} | {r['elapsed_seconds']:.6f} | {r['groups']} |"
        for r in trials
    )
    stat_table = "\n".join(
        f"| {name} | {s['mean_seconds']:.6f} | {s['median_seconds']:.6f} | {s['min_seconds']:.6f} | {s['max_seconds']:.6f} |"
        for name, s in stats.items()
    )
    markdown = f"""# Spark HW3 results

Run: `{summary['run_id']}`; UTC completion: {summary['timestamp']}.
Dataset: official MovieLens Latest Small, original CSV files in HDFS.
Environment: Python {summary['environment']['python']}, Spark {summary['environment']['spark']},
Java {summary['environment']['java']}; master `{summary['environment']['master']}`.
Hadoop client {summary['environment']['hadoop_client']}; default FS `{summary['environment']['default_fs']}`.
Host memory and active Java/MongoDB processes are captured in the run log.

## Inputs and correctness

- Ratings CSV: {summary['checks']['ratings_csv_count']} rows.
- Movies CSV: {summary['checks']['movies_csv_count']} rows.
- Ratings Parquet: {summary['checks']['ratings_parquet_count']} rows.
- Rated movie groups: {summary['checks']['movie_groups']}.
- Movies retained with at least 20 ratings: {summary['checks']['movie_summary_count']}.
- Exact typed CSV/Parquet row multiset equality: {summary['checks']['ratings_rows_equal']}.
- Exact CSV/Parquet group/count equality in all trials: {summary['checks']['aggregations_equal']}.
- Parquet and JSON summary read-back equality: {summary['checks']['summary_outputs_equal']}.

Explicit input schemas (no inference pass):

```text
ratings: {RATINGS_SCHEMA.simpleString()}
movies: {MOVIES_SCHEMA.simpleString()}
```

## Movie summary sample

Sorted by rating_count descending, then movie_id ascending to break ties.

| movie_id | title | rating_count | avg_rating |
|---|---|---|---|
{sample}

## HDFS paths

""" + "\n".join(f"- {name}: `{path}`" for name, path in PATHS.items()) + f"""

## E2 benchmark

{POLICY}

| Format | Phase | Trial | Seconds | Groups |
|---|---|---|---|---|
{trial_table}

Only the three timed trials contribute to these statistics:

| Format | Mean (s) | Median (s) | Min (s) | Max (s) |
|---|---|---|---|---|
{stat_table}

CSV mean minus Parquet mean: {difference:.6f} seconds.
CSV mean / Parquet mean: {speedup:.4f}x (above 1 means Parquet faster).
CSV median / Parquet median: {summary['comparison']['csv_median_divided_by_parquet_median']:.4f}x.

## Q3

CSV and Parquet represent the same {summary['checks']['ratings_csv_count']} typed ratings.
Writing a different physical representation must not add or remove logical records.
Grouping the same movieId values with count(*) must therefore produce the same
{summary['checks']['movie_groups']} groups and per-movie counts. This run checked full
typed row equality and compared actual aggregation mappings in every warm-up and
timed trial; any discrepancy causes a nonzero exit rather than being assumed away.

## Q4

In this run Parquet's mean query runtime was {direction} than CSV's:
{stats['parquet']['mean_seconds']:.6f} s versus {stats['csv']['mean_seconds']:.6f} s.
The observed mean ratio was {speedup:.4f}x; the table also reports medians and ranges
to show variability across only three trials. CSV requires text parsing, while
Parquet stores typed columns and can read the movieId column for this query.
Those properties are possible contributors, not isolated causal measurements.
CSV schema inference and Spark startup were excluded, so they cannot explain the
timed differences here. Warm-ups and earlier scans can benefit HDFS and operating-system
caches. The small dataset, scheduling/JIT overhead, sequential format order,
other host workloads, and measurement variability limit generalization; these
results do not establish that Parquet is always faster. No cache-hit rates or
individual overhead components were measured.
"""
    for directory in (archive, report_dir):
        with (directory / "benchmark_results.csv").open("w", newline="", encoding="utf-8") as out:
            writer = csv.DictWriter(out, fieldnames=list(trials[0]))
            writer.writeheader()
            writer.writerows(trials)
        (directory / "benchmark_summary.json").write_text(
            json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        (directory / "results.md").write_text(markdown, encoding="utf-8")
    print(f"Verified results saved to {report_dir}; archive: {archive}")


def run(spark, run_id):
    require(spark.version == "3.5.9", "PySpark 3.5.9 is required")
    require(platform.python_version_tuple()[:2] == ("3", "12"), "Python 3.12 is required")
    java = spark._jvm.java.lang.System.getProperty("java.version")
    require(java.startswith("1.8."), f"Java 8 required, got {java}")
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    print("Environment:", platform.python_version(), spark.version, java,
          spark.sparkContext.master, conf.get("fs.defaultFS"), flush=True)
    ratings = read_csv(spark, PATHS["ratings_csv"], RATINGS_SCHEMA)
    movies = read_csv(spark, PATHS["movies_csv"], MOVIES_SCHEMA)
    ratings.printSchema()
    movies.printSchema()
    rating_count, movie_count = ratings.count(), movies.count()
    print(f"Input counts: ratings={rating_count}, movies={movie_count}", flush=True)
    require(rating_count > 0 and movie_count > 0, "Empty input dataset")
    for name, frame in (("ratings", ratings), ("movies", movies)):
        nulls = [F.col(column).isNull() for column in frame.columns]
        condition = nulls[0]
        for other in nulls[1:]:
            condition = condition | other
        require(frame.filter(condition).limit(1).count() == 0, f"Null field in {name}")
    require(movies.select("movieId").distinct().count() == movie_count, "Duplicate movie IDs")
    require(ratings.join(movies, "movieId", "left_anti").limit(1).count() == 0,
            "Ratings reference missing movie IDs")
    summary = (ratings.groupBy("movieId")
               .agg(F.count("*").alias("rating_count"), F.avg("rating").alias("avg_rating"))
               .join(movies, "movieId")
               .filter(F.col("rating_count") >= 20)
               .select(F.col("movieId").alias("movie_id"), "title", "rating_count",
                       F.round("avg_rating", 4).alias("avg_rating")))
    ordered = summary.orderBy(F.desc("rating_count"), "movie_id")
    ordered.show(10, truncate=False)
    sample = [row.asDict() for row in ordered.limit(10).collect()]
    summary_count = summary.count()
    require(summary_count > 0, "Movie summary is empty")
    ratings.write.mode("overwrite").parquet(PATHS["ratings_parquet"])
    summary.write.mode("overwrite").parquet(PATHS["movie_stats_parquet"])
    summary.coalesce(1).write.mode("overwrite").json(PATHS["movie_stats_json"])
    for name in ("ratings_parquet", "movie_stats_parquet", "movie_stats_json"):
        path = spark._jvm.org.apache.hadoop.fs.Path(PATHS[name])
        require(path.getFileSystem(conf).exists(path), f"Missing output {path}")
        print("Output exists:", PATHS[name], flush=True)
    parquet = spark.read.parquet(PATHS["ratings_parquet"])
    require(parquet.schema.simpleString() == RATINGS_SCHEMA.simpleString(), "Typed ratings schema differs")
    parquet_count = parquet.count()
    require(parquet_count == rating_count, "CSV/Parquet counts differ")
    require(ratings.exceptAll(parquet).limit(1).count() == 0 and
            parquet.exceptAll(ratings).limit(1).count() == 0, "CSV/Parquet typed rows differ")
    reference_summary = summary.orderBy("movie_id").collect()
    for name, frame in (
        ("parquet", spark.read.parquet(PATHS["movie_stats_parquet"])),
        ("json", spark.read.schema(summary.schema).json(PATHS["movie_stats_json"])),
    ):
        actual = frame.select(summary.columns).orderBy("movie_id").collect()
        require(actual == reference_summary, f"{name} summary differs from source")
        print(f"Read-back {name} summary: {len(actual)} rows, exact match", flush=True)
    csv_counts = as_counts(aggregate(ratings).collect())
    parquet_counts = as_counts(aggregate(parquet).collect())
    require(csv_counts == parquet_counts, "Reference CSV/Parquet aggregations differ")
    require(sum(csv_counts.values()) == rating_count, "Aggregation lost ratings")
    print("Benchmark policy:", POLICY, flush=True)
    trials = benchmark(spark, csv_counts)
    stats = {}
    for name in ("csv", "parquet"):
        values = [r["elapsed_seconds"] for r in trials if r["format"] == name and r["phase"] == "timed"]
        stats[name] = {"runtimes_seconds": values, "mean_seconds": statistics.mean(values),
                       "median_seconds": statistics.median(values), "min_seconds": min(values),
                       "max_seconds": max(values)}
    results = {
        "run_id": run_id, "timestamp": datetime.now(timezone.utc).isoformat(),
        "environment": {"python": platform.python_version(), "spark": spark.version,
                        "java": java, "master": spark.sparkContext.master,
                        "hadoop_client": spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion(),
                        "default_fs": conf.get("fs.defaultFS"), "shuffle_partitions": 4},
        "schemas": {"ratings": RATINGS_SCHEMA.jsonValue(), "movies": MOVIES_SCHEMA.jsonValue()},
        "paths": PATHS, "benchmark_policy": POLICY, "statistics": stats,
        "checks": {"ratings_csv_count": rating_count, "movies_csv_count": movie_count,
                   "ratings_parquet_count": parquet_count, "movie_groups": len(csv_counts),
                   "movie_summary_count": summary_count, "ratings_rows_equal": True,
                   "aggregations_equal": True, "summary_outputs_equal": True},
        "movie_summary_sample": sample,
        "comparison": {
            "csv_mean_minus_parquet_mean_seconds": stats["csv"]["mean_seconds"] - stats["parquet"]["mean_seconds"],
            "csv_mean_divided_by_parquet_mean": stats["csv"]["mean_seconds"] / stats["parquet"]["mean_seconds"],
            "csv_median_divided_by_parquet_median": stats["csv"]["median_seconds"] / stats["parquet"]["median_seconds"],
        },
    }
    save_artifacts(results, trials)
    print(json.dumps(results["checks"], indent=2))
    print(json.dumps(stats, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"))
    args = parser.parse_args()
    require(Path(args.run_id).name == args.run_id and args.run_id not in (".", ".."), "Invalid run ID")
    spark = (SparkSession.builder.master("local[*]").appName("BDSP-HW3-Spark")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.hadoop.dfs.replication", "1").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        run(spark, args.run_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
