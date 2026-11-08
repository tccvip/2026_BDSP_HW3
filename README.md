# Big Data Storage and Processing — Homework 3

An individual MovieLens pipeline using **HDFS, Kafka, Spark, MongoDB and PrestoDB**.
The assignment emphasizes reproducible observations: Kafka producer throughput
(E1), Spark CSV versus Parquet (E2), and MongoDB indexing (E3). Original requirements:
[HW3_BDSP_2026.docx](HW3_BDSP_2026.docx).

## Architecture and repository layout

```text
MovieLens CSVs → HDFS raw → Spark local batch → HDFS curated Parquet/JSON
                                                              │ JSON transfer
                                                              ▼
                                                  MongoDB movie_stats ← Presto SQL
ratings.csv → Python producer → Kafka (separate E1 replay branch)
```

Kafka-to-Spark streaming is not implemented or required. MongoDB remains the
persistent serving source; Presto queries it through its MongoDB connector without
permanently copying the collection. There is no Hive Metastore or Hive/HDFS Presto
connector. See the [architecture figure](report/assets/architecture.png).

```text
kafka/               KRaft Compose, Python producer, instructions
spark/               Batch application and instructions
mongodb/             MongoDB Compose, import/index experiment, offline tests
presto/              Presto Compose, etc/, sql/, verifier, offline tests
scripts/             Phase launchers and Word-report generator
logs/                Retained hdfs/, e1/, spark/, mongodb/, presto/ evidence
report/              Final DOCX, diagram, host snapshots and report validation
report/{spark,mongodb,presto}/
                     Latest results plus timestamped runs/ archives
requirements.txt     Pinned runtime Python dependencies
data/                Downloaded dataset/staging; intentionally ignored
.venv/               Existing local environment; intentionally ignored
```

## Prerequisites

Recorded host: Ubuntu 26.04 LTS, kernel 7.0.0-38-generic, Ryzen 5 5500U
(6 cores / 12 threads), approximately 6.64 GiB usable RAM. Use phases and stop
unneeded services manually; this is not a production cluster.

| Component | Recorded version / configuration |
|---|---|
| Python | 3.12.16 in the existing `.venv` for Spark/MongoDB/Presto |
| Java for Hadoop/Spark | OpenJDK 8u504 |
| Hadoop | 3.3.5 installation; Spark's bundled Hadoop client is 3.3.4 |
| Kafka | `apache/kafka:3.9.1`; one KRaft broker/controller; 3 partitions; RF=1 |
| Spark | PySpark 3.5.9, `local[*]` |
| MongoDB | `mongo:7.0`; observed 7.0.45 |
| PrestoDB | `prestodb/presto:0.298.1`; observed `0.298.1-9e1b45f` |
| Python clients | kafka-python 3.0.13; PyMongo 4.19.0 |

Docker Engine with Compose is required for the containers. Launchers expect
Hadoop at `$HOME/hadoop`, configuration at `$HOME/hadoop/etc/hadoop`, and Java 8 at
`/usr/lib/jvm/java-8-openjdk-amd64`. Adapt launcher paths if needed. Configure
`fs.defaultFS=hdfs://localhost:9000` and `dfs.replication=1` in Hadoop before use.
Presto supplies Java 17 inside its image; host Java does not need changing.

Reuse `.venv` when present. For a fresh checkout only, create it once with an
installed Python 3.12, then install dependencies:

```bash
python3.12 -m venv .venv  # skip when .venv already exists
source .venv/bin/activate
python -m pip install -r requirements.txt
```

If an existing environment lacks pip, the project also supports installation via:

```bash
uv pip install --python .venv/bin/python -r requirements.txt
```

The commands below are reproduction instructions, **not commands executed during
the submission audit**. Docker commands must be executed manually by the user.
No phase launcher invokes Docker. Inspect saved evidence without rerunning experiments.

## Part 1 — HDFS and original inputs

From the repository root:

```bash
mkdir -p data
curl -fL https://files.grouplens.org/datasets/movielens/ml-latest-small.zip -o data/ml-latest-small.zip
unzip -n data/ml-latest-small.zip -d data
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME="$HOME/hadoop"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
start-dfs.sh
hdfs dfsadmin -report
hdfs dfs -mkdir -p /user/bdsp/hw3/raw
hdfs dfs -put data/ml-latest-small/ratings.csv data/ml-latest-small/movies.csv /user/bdsp/hw3/raw/
hdfs dfs -ls -h /user/bdsp/hw3/raw
hdfs fsck /user/bdsp/hw3/raw/ratings.csv -files -blocks -locations
```

This assumes an initialized single-node HDFS installation with working SSH.
NameNode formatting is not automated: **never format an existing NameNode**.
The upload intentionally fails if destination files exist; inspect and reuse them
instead of overwriting raw data. Saved evidence shows 100,836 ratings, 610 users,
9,742 movies, one block per CSV, replication 1 and HEALTHY fsck status.
See `logs/hdfs/`. Stop HDFS manually with `stop-dfs.sh` only after Spark and any
required JSON transfer have finished.

## Part 2 — Kafka replay and E1

```bash
docker compose -f kafka/docker-compose.yml up -d
```

Use [kafka/README.md](kafka/README.md) to verify broker readiness. Host clients use
`localhost:9092`; the controller listener on 9093 is not published. Create the
assignment topics, inspect their settings, and run the producer:

```bash
docker compose -f kafka/docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic ratings-5k --partitions 3 --replication-factor 1
docker compose -f kafka/docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic ratings-30k --partitions 3 --replication-factor 1
docker compose -f kafka/docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic ratings-5k
docker compose -f kafka/docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic ratings-30k
.venv/bin/python kafka/producer.py --topic ratings-5k --num-records 5000
.venv/bin/python kafka/producer.py --topic ratings-30k --num-records 30000
docker compose -f kafka/docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic ratings-5k
docker compose -f kafka/docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic ratings-30k
```

The producer keys complete JSON records by userId and uses `acks="all"`. Timing
includes CSV iteration, serialization, sending and flush, excluding initialization.
N is event volume, not producer `batch.size` tuning. TXT/JSON logs under `logs/e1/`
preserve repeated runs with suffixed filenames. For existing topics, compare
starting and ending offsets; saved final totals of 10,000 and 60,000 correspond to
the user-reported two replays, not a single 5K/30K invocation.

For an intentionally fresh measurement, use the same broker CLI prefix with
`kafka-topics.sh --delete --topic <topic>` for only these two assignment topics,
wait for deletion, then recreate them with the commands above. This destroys their
existing events: retain evidence first. Do not delete volumes to reset E1.

```bash
docker compose -f kafka/docker-compose.yml stop kafka
```

## Part 3 — Spark batch processing and E2

Keep HDFS running; Kafka is unnecessary:

```bash
bash scripts/run_spark_hw3.sh
```

The launcher selects the existing `.venv`, Java 8 and Hadoop configuration. One
SparkSession reads explicit CSV schemas from full HDFS URIs, aggregates ratings,
joins titles, retains rating_count >=20 and rounds averages to four decimals.
Curated HDFS outputs under `/user/bdsp/hw3/curated/` are `ratings_parquet`,
`movie_stats_parquet` and `movie_stats_json` (one JSON Lines part file).
Reruns overwrite only curated destinations; raw input paths remain intact.

All 1,297 summary rows read back correctly. E2 uses the same groupBy(movieId)/count
query, warms each format once, then performs three materializing timed runs per
format. Counts, full typed rows and actual group results must agree. Neither
format is explicitly cached; earlier reads and HDFS/OS caches affect these warm
measurements. See [spark/README.md](spark/README.md) for timing boundaries.

## Part 4 — MongoDB serving and E3

Start MongoDB manually, then run the Docker-free host workflow:

```bash
docker compose -f mongodb/compose.yaml up -d
docker compose -f mongodb/compose.yaml ps
bash scripts/run_mongodb_hw3.sh
```

The launcher pings `mongodb://localhost:27017` and fails if unavailable. It
downloads HDFS JSON to `data/mongodb/<run-id>/movie_stats_json` and imports only
`part-*.json`. **It validates input before dropping and rebuilding only
`bdsp_hw3.movie_stats` and its indexes.** Keep unrelated data out of that collection.
The import verifies all 1,297 documents and integer IDs/counts plus double averages.
It explains `{movie_id: 1}` before/after creating `movie_id_1`. See
[mongodb/README.md](mongodb/README.md); MongoDB must already be running.

## Part 5 — PrestoDB SQL over MongoDB

Startup order is MongoDB first, then Presto. The existing external network is
`bdsp-hw3-mongodb_default`; Presto's connector uses `mongodb:27017` inside it.
Do not start a second MongoDB instance. If a Compose project override changes the
network name, set `MONGODB_NETWORK` to that existing network.

```bash
docker compose -f presto/compose.yaml up -d
docker compose -f presto/compose.yaml logs --tail=100 presto
```

Wait for SQL readiness, then run:

```bash
bash scripts/run_presto_hw3.sh --server http://localhost:8080
```

The verifier checks catalog/schema/table visibility and types, executes
`presto/sql/top10.sql`, and compares every field of all ten ranked rows with
MongoDB. **An existing valid `bdsp_hw3._schema` definition is required before SQL**:
the connector otherwise creates metadata even for DESCRIBE. If missing, stop and
resolve metadata setup explicitly rather than bypassing the guard under a
no-database-writes constraint. No automatic metadata bootstrap script exists.
See [presto/README.md](presto/README.md) for CLI and metadata details.

Normal manual shutdown preserves persistent data:

```bash
docker compose -f presto/compose.yaml stop presto
docker compose -f mongodb/compose.yaml stop mongodb
```

Never use `down -v` or remove named volumes for normal shutdown. Stop Presto before
MongoDB. HDFS must remain available whenever the MongoDB workflow downloads input.

## Recorded results and final report

| Experiment | Saved observation | Primary artifacts |
|---|---|---|
| E1, 5K | 0.600682 s; 8,323.88 records/s; zero failures | `logs/e1/ratings-5k.json` and `.txt` |
| E1, 30K | 3.011591 s; 9,961.51 records/s; zero failures | `logs/e1/ratings-30k.json` and `.txt` |
| E2 | CSV mean 0.230397 s; Parquet mean 0.177646 s; ratio 1.2969× | `report/spark/benchmark_results.csv`, `benchmark_summary.json` |
| E3 | COLLSCAN: 1,297 examined / 0 ms; IXSCAN + FETCH: 1 examined / 1 ms; nReturned=1 in both | `report/mongodb/benchmark_results.json` and both complete explains |
| Part 5 | All 40 field comparisons match; no final errors/warnings | `report/presto/verification_results.json` |

E2 medians are 0.232674 s (CSV) and 0.175566 s (Parquet); all 100,836 rows and
9,724 groups match. Parquet is not necessarily always faster. E3 reduces examined
documents by 99.923%; 0 ms versus 1 ms does not establish a general latency change.

Saved E1 artifacts differ from earlier user-supplied timings (0.652027 s and
3.012877 s); the report uses the saved artifacts and discloses this discrepancy.
Kafka topic-description output and complete historical service inventories were
not retained. Failed attempts and the earlier Presto build-suffix warning remain
archived; the final Presto run completed without warnings.

The editable report is [report/BDSP_HW3_Report.docx](report/BDSP_HW3_Report.docx).
It answers Q1–Q8 in the tutorial's required structure and has six designed pages;
rendered pagination is unverified and no PDF has been produced. To regenerate
from saved evidence after restoring the ignored dataset:

```bash
uv pip install --python .venv/bin/python -r report/report_requirements.txt
.venv/bin/python scripts/generate_hw3_report.py
```

Generation reads CSV/ZIP files and saved evidence and rewrites only the report,
diagram and report-validation metadata, not experiment measurements. See
`report/report_validation.json` for structural checks and evidence hashes.
Latest summaries, full explains, raw Presto responses and timestamped `runs/`
archives are retained intentionally.

## Offline checks and GitHub submission

```bash
for script in scripts/run_*_hw3.sh; do bash -n "$script"; done
.venv/bin/python -m unittest discover -s mongodb -p 'test_*.py' -v
.venv/bin/python -m unittest discover -s presto -p 'test_*.py' -v
git diff --check
git diff --cached --check
git status --short
```

Tests use temporary synthetic fixtures and do not run experiments. Ignore local
environments/caches, downloaded/staged data, secrets and service storage. Retain
source, Compose/configuration, SQL, evidence and final report. Already-tracked
bytecode requires explicit index cleanup: `.gitignore` alone does not untrack it.

This single-machine deployment has no independent-node redundancy or production
authentication. Logs/results contain local home paths, hostnames and LAN addresses;
review that provenance before public release. Evidence has not been redacted or
rewritten. Review assignment redistribution permission before publishing the
original tutorial. Reproduction commands were checked against source/configuration
but were not executed during the submission audit.
