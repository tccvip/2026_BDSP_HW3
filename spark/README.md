# HW3 Spark batch pipeline

Run from the project root:

```bash
bash scripts/run_spark_hw3.sh
```

From another working directory, use the absolute launcher path:

```bash
bash /home/canh/code/Big_data_store_processing/2026_BDSP_HW3/scripts/run_spark_hw3.sh
```

Prerequisites: the existing `.venv` with Python 3.12 and PySpark 3.5.9;
Java 8 at `/usr/lib/jvm/java-8-openjdk-amd64`; Hadoop at `~/hadoop`; and
an operational HDFS NameNode and DataNode. The launcher sets `JAVA_HOME`,
`HADOOP_HOME`, `HADOOP_CONF_DIR`, and the PySpark Python interpreter for this
process only. No installation or changes to shell profiles are performed.
Stop unnecessary services before measuring on the 6.6 GiB machine.

The application uses one SparkSession in `local[*]` mode with four shuffle
partitions. It loads CSV with explicit schemas and standard doubled-quote
escaping, validates nonempty inputs and movie IDs, calculates counts and means,
joins titles, retains movies with at least 20 ratings, and rounds means to four
decimal places. It displays ten rows by descending count, breaking ties by ID.

All paths use the full `hdfs://localhost:9000` URI, even if the default filesystem
is `file:///`. Raw inputs are read only:

- `/user/bdsp/hw3/raw/ratings.csv`
- `/user/bdsp/hw3/raw/movies.csv`

Derived directories are replaced on every run using overwrite mode:

- `/user/bdsp/hw3/curated/ratings_parquet`: typed ratings.
- `/user/bdsp/hw3/curated/movie_stats_parquet`: filtered movie summary.
- `/user/bdsp/hw3/curated/movie_stats_json`: JSON Lines in one `part-*.json` file.

Only the JSON part file is suitable for a later MongoDB import; `_SUCCESS` is
metadata. No Kafka streaming, Spark cluster, or MongoDB import is implemented.

Before benchmarking, all outputs are read back. The application checks exact
CSV/Parquet typed row multisets, input counts, summary rows in both export formats,
and movie group/count mappings. Any mismatch raises an exception and returns a
nonzero exit code. Each benchmark result is also checked against the reference.

E2 uses the same `groupBy(movieId).count()` logic for both inputs. One warm-up
per format precedes three CSV trials followed by three Parquet trials. Every
timer starts before DataFrame construction and ends immediately after `collect()`.
Thus runtime includes planning, input reading/decoding, aggregation, and result
materialization. Spark startup, export, validation, comparisons, and artifact
writing are excluded. CSV schema inference is disabled; Parquet reads its stored
schema metadata. No DataFrame is cached or persisted. Earlier scans and warm-ups
can populate HDFS and OS caches; no cache flushing is attempted. The sequential
order, small dataset, JIT/scheduling overhead, and other host activity affect
comparisons. This is not a cold-cache storage benchmark.

Artifacts from a successful validated run:

- `logs/spark/<UTC-timestamp>-<pid>.log`: combined stdout/stderr, environment,
  process inventory, trial output, and application exit code. Failed runs also
  retain their logs. The launcher preserves the application exit code through tee.
- `report/spark/benchmark_results.csv`: both warm-ups and all six timed runs,
  elapsed seconds, group counts, and validation flags.
- `report/spark/benchmark_summary.json`: environment, schemas, paths, checks,
  sample, individual timed values, mean/median/min/max, differences and ratios.
- `report/spark/results.md`: report material including measured Q3/Q4 discussion.
- `report/spark/runs/<run-id>/`: archived copies of all three report artifacts.

The three top-level report artifacts describe the latest successful run;
timestamped archives preserve earlier results. A failed run does not replace
them: always match the report run ID to the log and check the exit code.
Rerunning replaces derived HDFS outputs but never writes to the raw input paths.

Reference behavior: [Spark CSV options](https://spark.apache.org/docs/3.5.7/sql-data-sources-csv.html)
and [Parquet schema preservation](https://spark.apache.org/docs/3.5.7/sql-data-sources-parquet.html).
