# HW3 Part 5: PrestoDB SQL over MongoDB

## Setup and version

Live Part 5 verification completed successfully against the existing services.
See `report/presto/results.md` for actual SQL output and MongoDB comparisons,
and `report/presto/README.md` for the final run ID and log location.
No Docker commands, container operations, or MongoDB writes were executed.

The image is pinned to **`prestodb/presto:0.298.1`**, not Trino. The image includes
the matching MongoDB connector and `presto-cli`; no external connector JAR is
mixed with another server version. The pinned connector source supports
`connector.name=mongodb` and `mongodb.seeds`, maps MongoDB integer fields to
Presto `bigint`, and maps doubles to `double`. Part 4 verified integer IDs/counts
and double averages. Live verification succeeded against MongoDB 7.0.45 with
Presto reporting `0.298.1-9e1b45f`; the suffix identifies the build and is retained
in the report rather than treated as a different release. The connector bundles MongoDB Java Sync Driver
5.6.5. This is a reproducible selected version, not a claim that it is latest.

The image supplies Java 17, so host Java 8 and the Spark environment are unchanged.
The JVM options match the pinned image defaults with a 1 GiB heap. The container
is limited to 1.5 GiB; query memory is bounded to leave heap headroom. On the
6.6 GiB host, stop unneeded services manually before running MongoDB and Presto.
These are initial limits, not measured capacity guarantees.

Configuration is mounted read-only at `/opt/presto-server/etc`; SQL is mounted at
`/opt/hw3/sql`. One coordinator also executes queries. The health check runs
`SELECT 1` through the bundled CLI; it proves SQL readiness, not MongoDB catalog
connectivity. Only host loopback port 8080 is published. No Hive, metastore, HDFS
connector, second MongoDB, or persistent copy of the movie collection is added.
Presto's internal data directory is for runtime state, not a serving copy of MongoDB.

## MongoDB hostname and network

The existing `mongodb/compose.yaml` declares project `bdsp-hw3-mongodb`, service
and container `mongodb`, and no explicit networks. Compose therefore derives
`bdsp-hw3-mongodb_default` as its default network. This is inferred from the file;
actual container network membership was not inspected because Docker execution
is prohibited. Presto joins this network as **external** instead of creating an
isolated network. `mongodb.seeds=mongodb:27017` uses the MongoDB service DNS name;
`localhost` inside Presto would refer to Presto itself. Host PyMongo comparisons
continue to use `mongodb://localhost:27017`.

Before starting Presto, run these **manually**, from the repository root:

```bash
docker inspect mongodb --format '{{json .NetworkSettings.Networks}}'
docker inspect mongodb --format '{{.State.Health.Status}}'
```

MongoDB must already be running and healthy with Part 4 imported. If the actual
network differs because MongoDB was started with a project-name override, set:

```bash
export MONGODB_NETWORK=the_actual_existing_network_name
```

The Compose default is `bdsp-hw3-mongodb_default`. Do not create a replacement
network or a second MongoDB. An absent external network is a setup error to fix
manually using the existing MongoDB deployment.

## Connector schema metadata behavior

The connector stores table definitions in MongoDB's `_schema` collection. If no
definition exists for `movie_stats`, its first DESCRIBE/SELECT infers the fields
and **creates metadata and a table-name index in `_schema`**. This is an inherent
connector side effect, even for SELECT and even with Presto read-only SQL access
control. It does not rewrite existing `movie_stats` documents or copy the collection.
No prepared script explicitly creates/updates/deletes MongoDB documents, indexes,
or schemas; PyMongo calls are reads only. **The verifier requires an existing
`bdsp_hw3._schema` definition for `movie_stats` with the expected types before
sending SQL.** If absent or incorrect, it stops without running discovery queries
or changing metadata. Existing definitions are used as-is, not overwritten.
If this precheck fails, share the error and resolve metadata setup explicitly;
do not rerun Part 4 or silently allow metadata writes. Manual CLI discovery can
have the side effect described above, so do not use it to bypass this guard under
a strict no-MongoDB-writes policy.
The verifier checks the complete `movie_stats` document fingerprint before and
after querying. It does not treat that as proof that `_schema` was unchanged.

## Startup order and manual CLI verification

Startup order: **existing MongoDB first, then Presto**. Do not rerun Part 4 just to
start Presto; Part 4 resets the collection. Do not change `mongodb/compose.yaml`.
After confirming MongoDB's network and health, run manually:

```bash
docker compose -f presto/compose.yaml up -d
docker compose -f presto/compose.yaml logs --tail=100 presto
docker inspect presto --format '{{.State.Health.Status}}'
```

Wait for healthy status / `SERVER STARTED`. The UI is at `http://localhost:8080`.
Run the verification SQL and top-ten query with the matching bundled CLI:

```bash
docker compose -f presto/compose.yaml exec -T presto presto-cli --server http://localhost:8080 --file /opt/hw3/sql/verify.sql
docker compose -f presto/compose.yaml exec -T presto presto-cli --server http://localhost:8080 --file /opt/hw3/sql/top10.sql
```

The first file executes:

```sql
SHOW SCHEMAS FROM mongodb;
SHOW TABLES FROM mongodb.bdsp_hw3;
DESCRIBE mongodb.bdsp_hw3.movie_stats;
```

Expect `bdsp_hw3`, `movie_stats`, and fields `movie_id bigint`, `title varchar`,
`rating_count bigint`, `avg_rating double`. These are expected schema mappings,
not observed Part 5 output. An inferred hidden `_id` is not part of the SQL query.
`sql/top10.sql` filters rating_count >= 20, sorts by avg_rating descending, count
descending, then ID ascending, and returns ten rows.

## Logged verification and MongoDB comparison

The user has already started both services; no lifecycle commands are needed for
verification. Once the services and existing connector metadata are ready, run:

```bash
bash scripts/run_presto_hw3.sh
```

The script resolves the project root from any working directory, uses the existing
Python 3.12 `.venv` and PyMongo, and never calls Docker. It submits SELECT/SHOW/
DESCRIBE through Presto's HTTP statement API using only Python's standard library.
It checks coordinator readiness and MongoDB ping, verifies catalog/schema/table
visibility and types, then checks **all four fields for all ten ranked rows**
against PyMongo's equivalently sorted query. This includes the required top three.
Query failure, missing metadata, different ordering/values, or a changed collection
produces a nonzero exit. It never starts, restarts, or repairs services automatically.

Actual outputs will be written only when this workflow is executed:

- `logs/presto/<run-id>.log`: timestamped combined stdout/stderr and exit code.
- `report/presto/runs/<run-id>/`: full paginated HTTP responses for SHOW CATALOGS,
  schemas, tables, DESCRIBE and top ten; comparison JSON and report on success.
- `report/presto/verification_results.json`: latest successful comparison, including
  explicit Presto version, visibility/type checks, warnings/errors, per-field
  comparisons for each ranked row, top-three match flag and SQL output.
- `report/presto/results.md`: latest successful schema, query, top-ten table,
  comparison table, warnings/errors and Q6.
- Each run archive includes `status.json` (`verified` or `failed`), and the
  coordinator info response is preserved as `presto_info.json` when available.
- On error, the run archive retains `error.txt`, HTTP error bodies and collected responses. Latest
  successful results are not replaced; match report and log run IDs.

Live verification is **complete**. Read `report/presto/README.md` for the final
run ID and evidence. Offline test fixtures are temporary and are never copied
into the report directory.

For manual document inspection, use a host-installed MongoDB shell:

```bash
mongosh 'mongodb://localhost:27017/bdsp_hw3'
```

Then `db.movie_stats.find({movie_id: <actual_id_from_SQL>}, {_id: 0})` for each
of the top three IDs, comparing movie_id, title, rating_count and avg_rating.
Do not substitute an expected list from Spark for actual Presto results.

If a query reports unknown MongoDB host, check shared-network membership and the
seed hostname. For missing table/columns or permissions, inspect `_schema` and the
full error without rewriting the data. If the coordinator fails to start, inspect
logs for JVM/memory/config errors. Resolve unavailable services manually before
retrying; never fabricate query output.

## Q6

PrestoDB is a distributed SQL query engine, here running on one node. MongoDB
remains the persistent data source. The MongoDB connector translates metadata
access and supported SQL operations into MongoDB requests; Presto performs the
remaining query work. Presto reads through that connector and does not permanently
copy the entire collection into its own storage.

## Offline checks

```bash
bash -n scripts/run_presto_hw3.sh
.venv/bin/python -m unittest discover -s presto -p 'test_*.py' -v
```

These tests use synthetic temporary fixtures and mocked HTTP responses. They do
not execute the live verifier or create measured report artifacts.

## Version-specific references

- [Official image and CLI paths](https://github.com/prestodb/presto/blob/0.298.1/docker/Dockerfile)
- [Image entrypoint](https://github.com/prestodb/presto/blob/0.298.1/docker/entrypoint.sh)
- [Pinned JVM defaults](https://github.com/prestodb/presto/blob/0.298.1/docker/etc/jvm.config.example)
- [Connector properties](https://github.com/prestodb/presto/blob/0.298.1/presto-mongodb/src/main/java/com/facebook/presto/mongodb/MongoClientConfig.java)
- [Connector schema inference and metadata writes](https://github.com/prestodb/presto/blob/0.298.1/presto-mongodb/src/main/java/com/facebook/presto/mongodb/MongoSession.java)
- [Bundled driver version](https://github.com/prestodb/presto/blob/0.298.1/presto-mongodb/pom.xml)
