#!/usr/bin/env python3
"""Import the Spark movie summary and measure MongoDB indexing (HW3 E3)."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import sys

from bson import json_util
from pymongo import ASCENDING, MongoClient
from pymongo.errors import ConnectionFailure


ROOT = Path(__file__).resolve().parents[1]
DATABASE = "bdsp_hw3"
COLLECTION = "movie_stats"
URI = "mongodb://localhost:27017"
QUERY = {"movie_id": 1}
SOURCE = "hdfs://localhost:9000/user/bdsp/hw3/curated/movie_stats_json"
FIELDS = {"movie_id", "title", "rating_count", "avg_rating"}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def load_records(directory):
    """Validate the entire small input before modifying the collection."""
    files = sorted(path for path in directory.glob("part-*.json") if path.is_file())
    require(files, f"No part-*.json files found in {directory}")
    documents, seen, manifest = [], set(), []
    for path in files:
        manifest.append({"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
        with path.open(encoding="utf-8") as source:
            for number, line in enumerate(source, 1):
                try:
                    doc = json.loads(line)
                    require(isinstance(doc, dict) and set(doc) == FIELDS, "Unexpected fields")
                    for field in ("movie_id", "rating_count"):
                        require(type(doc[field]) is int and 0 < doc[field] < 2**63,
                                f"{field} must be a positive BSON integer")
                    require(doc["rating_count"] >= 20, "rating_count must be >= 20")
                    require(isinstance(doc["title"], str) and doc["title"].strip(), "Invalid title")
                    value = doc["avg_rating"]
                    require(type(value) in (int, float) and math.isfinite(value)
                            and 0.5 <= value <= 5.0, "Invalid avg_rating")
                    # Even JSON numbers such as 4 are stored as BSON doubles.
                    doc["avg_rating"] = float(value)
                    require(doc["movie_id"] not in seen, "Duplicate movie_id")
                    seen.add(doc["movie_id"])
                    documents.append(doc)
                except (ValueError, TypeError, OverflowError) as error:
                    raise ValueError(f"{path}:{number}: {error}") from error
    require(documents, "Input part files contain no documents")
    require(1 in seen, "Input lacks movie_id 1 required for the selective query")
    return documents, manifest


def stage_names(value):
    """Handle nested classic plans, SBE queryPlan wrappers, and arrays."""
    found = set()
    if isinstance(value, dict):
        if isinstance(value.get("stage"), str):
            found.add(value["stage"])
        for child in value.values():
            found.update(stage_names(child))
    elif isinstance(value, list):
        for child in value:
            found.update(stage_names(child))
    return sorted(found)


def plan_uses_index(value, index_name):
    if isinstance(value, dict):
        return (value.get("indexName") == index_name
                or any(plan_uses_index(child, index_name) for child in value.values()))
    if isinstance(value, list):
        return any(plan_uses_index(child, index_name) for child in value)
    return False


def explain_metrics(explain):
    plan = explain["queryPlanner"]["winningPlan"]
    stats = explain["executionStats"]
    return {
        "winning_plan": plan,
        "plan_stages": stage_names(plan),
        "execution_stages": stage_names(stats.get("executionStages", {})),
        **{field: stats[field] for field in
           ("nReturned", "totalDocsExamined", "executionTimeMillis")},
        "totalKeysExamined": stats.get("totalKeysExamined"),
    }


def write_json(path, value):
    # Extended JSON preserves ObjectIds and other BSON types in full explains.
    path.write_text(json_util.dumps(value, indent=2) + "\n", encoding="utf-8")


def write_report(result, directory):
    before, after = result["before_index"], result["after_index"]
    removed = before["totalDocsExamined"] - after["totalDocsExamined"]
    rows = "\n".join(
        f"| {label} | {', '.join(data['plan_stages'])} | {data['nReturned']} | "
        f"{data['totalDocsExamined']} | {data['executionTimeMillis']} |"
        for label, data in (("Before index", before), ("After index", after))
    )
    text = f"""# MongoDB HW3 Part 4 / E3

Run: `{result['run_id']}`; UTC: {result['timestamp']}.
MongoDB version: {result['mongodb_version']}.

## Transfer, import, and validation

The launcher used `hdfs dfs -get` to copy `{SOURCE}` into a fresh timestamped
directory under `data/mongodb/`. Only `part-*.json` files were parsed;
Spark metadata, hidden files, and `_SUCCESS` were ignored. Source file paths
and SHA-256 hashes are recorded in `benchmark_results.json`.
All source records were validated before resetting only `{DATABASE}.{COLLECTION}`.
Dropping and recreating this assignment collection removes old documents and indexes;
batch insertion makes reruns repeatable. No HDFS files were modified.

Imported documents: **{result['document_count']}**.
All imported values match the source: **{result['validation']['source_values_match']}**.

Sample document (Extended JSON):

```json
{json_util.dumps(result['sample_document'], indent=2)}
```

Sample BSON types from `$type`: `{json.dumps(result['sample_bson_types'])}`.
Types were also checked for every document, not just the sample.

## Index experiment

Query: `{{movie_id: 1}}`, returning full documents without a projection or hint.
Index name: `{result['index_name']}`; definition: `{json_util.dumps(result['index_information'])}`.
Both measurements used the same find command with `verbosity: executionStats`.
All pre-index definitions and the full explains are saved alongside this report.

| Condition | Winning plan stages | nReturned | totalDocsExamined | executionTimeMillis |
|---|---|---|---|---|
{rows}

Observed index usage: **{result['index_used']}**.
Query documents before and after indexing are identical:
**{result['validation']['query_results_equal']}**; both explain counts agree with
the retrieved documents: **{result['validation']['nReturned_consistent']}**.
Documents examined decreased by **{removed}** (a negative value means an increase).

## Q5

The observed pre-index stages were {', '.join(before['plan_stages'])}; post-index
stages were {', '.join(after['plan_stages'])}. Documents examined changed from
{before['totalDocsExamined']} to {after['totalDocsExamined']} while the same
{after['nReturned']} documents were returned. A selective index can locate matching
keys instead of checking the whole collection; FETCH may still retrieve full documents.
Whether that happened here is determined by the actual stages and counters above.
Elapsed times of {before['executionTimeMillis']} ms and {after['executionTimeMillis']} ms
are single observations on a small, recently imported collection: millisecond
resolution, warm caches, and variability prevent treating tiny differences as
proof of general performance improvement. COLLSCAN before indexing and IXSCAN
with possible FETCH afterward are expectations, not substituted measurements.

Warnings: {json.dumps(result['warnings'])}
"""
    (directory / "results.md").write_text(text, encoding="utf-8")


def run(args):
    with MongoClient(URI, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000,
                     socketTimeoutMS=30000) as client:
        client.admin.command("ping")
        version = client.server_info()["version"]
        documents, manifest = load_records(args.input_dir)
        print(f"Validated source: {len(documents)} documents from {len(manifest)} part files", flush=True)
        expected = sorted(documents, key=lambda doc: doc["movie_id"])
        database = client[DATABASE]
        collection = database[COLLECTION]
        archive = ROOT / "report/mongodb/runs" / args.run_id
        archive.mkdir(parents=True, exist_ok=False)
        print(f"Resetting only {DATABASE}.{COLLECTION}", flush=True)
        collection.drop()
        for offset in range(0, len(documents), 500):
            # insert_many adds _id to its input, so pass copies to retain the source.
            collection.insert_many([dict(doc) for doc in documents[offset:offset + 500]])
        count = collection.count_documents({})
        actual = list(collection.find({}, {"_id": 0}).sort("movie_id", ASCENDING))
        require(count == len(expected) and actual == expected, "Imported values differ from source")
        type_projection = {field: {"$type": f"${field}"}
                           for field in ("movie_id", "rating_count", "avg_rating")}
        type_rows = list(collection.aggregate([
            {"$project": {"_id": 0, **type_projection}},
            {"$group": {"_id": {field: f"${field}" for field in type_projection},
                        "count": {"$sum": 1}}},
        ]))
        for row in type_rows:
            types = row["_id"]
            require(types["movie_id"] in ("int", "long")
                    and types["rating_count"] in ("int", "long")
                    and types["avg_rating"] == "double", "Unexpected stored BSON types")
        require(sum(row["count"] for row in type_rows) == count, "Type validation count differs")
        sample = collection.find_one(sort=[("movie_id", ASCENDING)])
        sample_types = next(collection.aggregate([
            {"$match": {"_id": sample["_id"]}},
            {"$project": {"_id": 0, **type_projection}},
        ]))
        indexes_before = collection.index_information()
        require(set(indexes_before) == {"_id_"}, "Unexpected index before baseline measurement")
        command = {"find": COLLECTION, "filter": QUERY}
        before = database.command("explain", command, verbosity="executionStats")
        write_json(archive / "explain_before_index.json", before)
        before_docs = list(collection.find(QUERY).sort("_id", ASCENDING))
        index_name = collection.create_index([("movie_id", ASCENDING)])
        index_information = collection.index_information()[index_name]
        after = database.command("explain", command, verbosity="executionStats")
        write_json(archive / "explain_after_index.json", after)
        after_docs = list(collection.find(QUERY).sort("_id", ASCENDING))
        before_stats, after_stats = explain_metrics(before), explain_metrics(after)
        require(before_docs == after_docs, "Query results changed after indexing")
        require(before_stats["nReturned"] == after_stats["nReturned"] == len(before_docs),
                "Explain nReturned differs from actual query results")
        source_query = [doc for doc in expected if doc["movie_id"] == 1]
        require([{key: value for key, value in doc.items() if key != "_id"}
                 for doc in after_docs] == source_query, "Query differs from source")
        index_used = plan_uses_index(after_stats["winning_plan"], index_name)
        warnings = []
        if "COLLSCAN" not in before_stats["plan_stages"]:
            warnings.append("Baseline winning plan did not include COLLSCAN; inspect full explain.")
        if not index_used:
            warnings.append("Winning plan did not reference the new index; inspect full explain.")
        if after_stats["totalDocsExamined"] >= before_stats["totalDocsExamined"]:
            warnings.append("Documents examined did not decrease.")
        result = {
            "run_id": args.run_id, "timestamp": datetime.now(timezone.utc).isoformat(),
            "mongodb_version": version, "uri": URI, "database": DATABASE,
            "collection": COLLECTION, "source_hdfs": SOURCE, "source_files": manifest,
            "document_count": count, "sample_document": sample,
            "sample_bson_types": sample_types, "all_document_bson_types": type_rows,
            "indexes_before": indexes_before, "index_name": index_name,
            "index_information": index_information, "query": QUERY,
            "before_index": before_stats, "after_index": after_stats, "index_used": index_used,
            "query_results_before": before_docs, "query_results_after": after_docs,
            "validation": {"source_values_match": True, "query_results_equal": True,
                           "nReturned_consistent": True, "query_matches_source": True},
            "warnings": warnings,
        }
        write_json(archive / "benchmark_results.json", result)
        write_report(result, archive)
        # Latest results are published only after all validation succeeds.
        for name in ("explain_before_index.json", "explain_after_index.json",
                     "benchmark_results.json", "results.md"):
            (ROOT / "report/mongodb" / name).write_bytes((archive / name).read_bytes())
        print(json_util.dumps(result, indent=2), flush=True)
        print(f"Results: {ROOT / 'report/mongodb'}; archive: {archive}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=ROOT / "data/mongodb/movie_stats_json")
    parser.add_argument("--run-id", default=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"))
    args = parser.parse_args()
    try:
        require(re.fullmatch(r"[A-Za-z0-9_-]+", args.run_id), "Invalid run ID")
        run(args)
    except ConnectionFailure as error:
        print(f"MongoDB is unreachable: {type(error).__name__}: {error}", file=sys.stderr)
        print("Start/check MongoDB manually, then confirm it is reachable before retrying.",
              file=sys.stderr)
        return 1
    except Exception as error:
        print(f"MongoDB workflow failed: {type(error).__name__}: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
