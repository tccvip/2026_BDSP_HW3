# MongoDB HW3 Part 4 / E3

Run: `20261107T095311717879893-29703`; UTC: 2026-11-07T09:53:16.593046+00:00.
MongoDB version: 7.0.45.

## Transfer, import, and validation

The launcher used `hdfs dfs -get` to copy `hdfs://localhost:9000/user/bdsp/hw3/curated/movie_stats_json` into a fresh timestamped
directory under `data/mongodb/`. Only `part-*.json` files were parsed;
Spark metadata, hidden files, and `_SUCCESS` were ignored. Source file paths
and SHA-256 hashes are recorded in `benchmark_results.json`.
All source records were validated before resetting only `bdsp_hw3.movie_stats`.
Dropping and recreating this assignment collection removes old documents and indexes;
batch insertion makes reruns repeatable. No HDFS files were modified.

Imported documents: **1297**.
All imported values match the source: **True**.

Sample document (Extended JSON):

```json
{
  "_id": {
    "$oid": "6aeef50cec5bb70aa7ad782c"
  },
  "movie_id": 1,
  "title": "Toy Story (1995)",
  "rating_count": 215,
  "avg_rating": 3.9209
}
```

Sample BSON types from `$type`: `{"movie_id": "int", "rating_count": "int", "avg_rating": "double"}`.
Types were also checked for every document, not just the sample.

## Index experiment

Query: `{movie_id: 1}`, returning full documents without a projection or hint.
Index name: `movie_id_1`; definition: `{"v": 2, "key": [["movie_id", 1]]}`.
Both measurements used the same find command with `verbosity: executionStats`.
All pre-index definitions and the full explains are saved alongside this report.

| Condition | Winning plan stages | nReturned | totalDocsExamined | executionTimeMillis |
|---|---|---|---|---|
| Before index | COLLSCAN | 1 | 1297 | 0 |
| After index | FETCH, IXSCAN | 1 | 1 | 1 |

Observed index usage: **True**.
Query documents before and after indexing are identical:
**True**; both explain counts agree with
the retrieved documents: **True**.
Documents examined decreased by **1296** (a negative value means an increase).

## Q5

The observed pre-index stages were COLLSCAN; post-index
stages were FETCH, IXSCAN. Documents examined changed from
1297 to 1 while the same
1 documents were returned. A selective index can locate matching
keys instead of checking the whole collection; FETCH may still retrieve full documents.
Whether that happened here is determined by the actual stages and counters above.
Elapsed times of 0 ms and 1 ms
are single observations on a small, recently imported collection: millisecond
resolution, warm caches, and variability prevent treating tiny differences as
proof of general performance improvement. COLLSCAN before indexing and IXSCAN
with possible FETCH afterward are expectations, not substituted measurements.

Warnings: []
