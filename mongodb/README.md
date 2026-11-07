# HW3 Part 4: MongoDB import and indexing

## Prerequisites

- Existing `.venv` with Python 3.12 and `pymongo` from `requirements.txt`.
  PyMongo was added using `uv pip install --python .venv/bin/python pymongo==4.19.0`.
- Java 8 at `/usr/lib/jvm/java-8-openjdk-amd64`, Hadoop at `~/hadoop`, and
  working HDFS at `hdfs://localhost:9000`.
- Spark's completed JSON Lines export at
  `/user/bdsp/hw3/curated/movie_stats_json`, containing `part-*.json` files.
- An already-running, healthy MongoDB server at `mongodb://localhost:27017`.
  The current user-managed container is `mongodb`, using `mongo:7.0`.
  The workflow uses PyMongo directly and never manages containers or volumes.

## Run manually

Start MongoDB manually before running the workflow. From the repository root:

```bash
bash scripts/run_mongodb_hw3.sh
```

From another directory, use the absolute path to the launcher. It resolves the
project root, activates the existing environment, sets Java/Hadoop variables,
checks dependencies, verifies MongoDB using `client.admin.command("ping")`,
then checks HDFS and downloads into `data/mongodb/<run-id>/movie_stats_json`. It imports only
`part-*.json`, ignoring `_SUCCESS` and hidden metadata. Downloads are retained;
no original HDFS paths are written or deleted. Errors return a nonzero exit code
and retain combined terminal output in `logs/mongodb/<run-id>.log`.

**Each import drops and rebuilds only `bdsp_hw3.movie_stats`, including its indexes.**
Do not store unrelated documents in that assignment collection. Source validation
and MongoDB connectivity checks happen before the reset. Failed imports may leave
a partial collection; rerunning starts clean. Other databases and collections are
not reset. This is an explicit batch replacement strategy for Q7.

If the initial ping fails, the launcher prints the connection error and asks the
user to start/check MongoDB manually, then exits before HDFS transfer or import.
The Python importer repeats the ping and exits on a connection failure at any
later stage; it never attempts to start MongoDB. Resolve the error and confirm
reachability before retrying. The actual server version is saved in each report.
Existing `mongodb/compose.yaml` is not modified or used by this workflow.

To transfer and run the Python application separately (after starting MongoDB):

```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME="$HOME/hadoop"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH"
mkdir -p data/mongodb
hdfs dfs -get hdfs://localhost:9000/user/bdsp/hw3/curated/movie_stats_json data/mongodb/movie_stats_json
.venv/bin/python mongodb/import_and_benchmark.py --input-dir data/mongodb/movie_stats_json
```

The standalone download intentionally fails if the destination already exists;
reuse validated local files or choose a fresh staging directory. The complete
launcher avoids this by using a new timestamped directory each time.

## Validation and E3

The application validates required fields, positive BSON-sized integer IDs/counts,
minimum 20 ratings, finite rating averages, titles, and unique movie IDs before
resetting the collection. JSON averages are explicitly converted to Python float
for BSON double encoding. It inserts batches of 500, compares every imported
document with the source (excluding generated `_id`), and uses MongoDB `$type`
to check types across the whole collection and for a sample document.

Dropping the collection ensures the baseline has only the default `_id_` index;
the script verifies this. It calls the database `explain` command with the same
`find` filter `{movie_id: 1}` and `verbosity="executionStats"` before and after
creating `{movie_id: 1}`. It does not force the planner with a hint. Nested winning
plan and execution stage trees are inspected, including SBE wrappers. Rejected
plans are not mistaken for the executed plan. Index usage is checked against the
index name in the actual winning plan. Unexpected plans are reported as warnings.

Full query documents must match before/after, match source JSON, and agree with
both `nReturned` values. Full explains are retained using Extended JSON so BSON
values such as ObjectIds remain representable. COLLSCAN and IXSCAN/FETCH are
expected outcomes, never substituted for observed statistics. Tiny execution-time
differences on this small, warm collection are not general performance evidence.

## Inspect manually

```bash
mongosh "mongodb://localhost:27017/bdsp_hw3"
```

Inside `mongosh`:

```javascript
db.movie_stats.countDocuments({})
db.movie_stats.findOne({movie_id: 1})
db.movie_stats.getIndexes()
db.movie_stats.aggregate([
  {$match: {movie_id: 1}},
  {$project: {_id: 0, movie_id: {$type: "$movie_id"},
    rating_count: {$type: "$rating_count"}, avg_rating: {$type: "$avg_rating"}}}
])
db.movie_stats.find({movie_id: 1}).explain("executionStats")
```

After a completed run, this explain is the indexed case. Rerun the workflow to
recreate the unindexed baseline rather than treating this as a before-index result.

## Outputs and shutdown

Successful runs generate these latest report files under `report/mongodb/`:

- `explain_before_index.json` and `explain_after_index.json`: complete explains.
- `benchmark_results.json`: server version, source hashes, count, sample/types,
  index definitions, nested stages, execution metrics, query results and checks.
- `results.md`: observed findings and explicit Q5 answer.

All files are archived under `report/mongodb/runs/<run-id>/` before updating
the latest copies. Failed runs retain their log and any explain files collected
in the run archive; latest successful reports may therefore be from an earlier
run. Always match run IDs. No measured report is generated before a real run.

Container lifecycle and persistent volumes remain under the user's manual
control. The workflow performs no start, stop, recreate, or volume operations.

References: [explain command](https://www.mongodb.com/docs/manual/reference/command/explain/)
and [aggregation $type](https://www.mongodb.com/docs/manual/reference/operator/aggregation/type/).
