#!/usr/bin/env python3
"""Verify Part 5 through Presto's HTTP API and read-only PyMongo queries."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys
import time
from urllib.request import Request, urlopen
from urllib.parse import urlsplit
from urllib.error import HTTPError

from bson import json_util
from pymongo import MongoClient

ROOT = Path(__file__).resolve().parents[1]
FIELDS = ["movie_id", "title", "rating_count", "avg_rating"]
Q6 = (
    "PrestoDB is a distributed SQL query engine; this homework uses one coordinator "
    "that also executes work. MongoDB remains the persistent data source. The MongoDB "
    "connector translates metadata access and supported SQL operations into MongoDB "
    "requests, while Presto executes the remaining query work. Presto reads through "
    "the connector and does not permanently copy the entire collection into its own storage."
)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def query(server, sql, evidence, warnings=None):
    evidence.mkdir()
    request = Request(server.rstrip("/") + "/v1/statement", data=sql.rstrip(";\n ").encode(),
                      headers={"X-Presto-User": "bdsp_hw3", "Content-Type": "text/plain"})
    rows, columns = [], None
    deadline = time.monotonic() + 120
    page = 0
    while True:
        require(time.monotonic() < deadline, "Presto query exceeded 120 seconds")
        try:
            with urlopen(request, timeout=20) as response:
                result = json.load(response)
        except HTTPError as error:
            detail = {"http_status": error.code, "body": error.read().decode("utf-8", errors="replace")}
            (evidence / f"{page:04d}-http-error.json").write_text(json.dumps(detail, indent=2) + "\n")
            raise RuntimeError(f"Presto HTTP error: {json.dumps(detail)}") from error
        (evidence / f"{page:04d}.json").write_text(json.dumps(result, indent=2) + "\n")
        page += 1
        for warning in result.get("warnings", []):
            if warnings is not None and warning not in warnings:
                warnings.append(warning)
                print("Presto warning:", json.dumps(warning), flush=True)
        if "error" in result:
            raise RuntimeError(json.dumps(result["error"]))
        if "columns" in result:
            columns = [column["name"] for column in result["columns"]]
        for values in result.get("data", []):
            require(columns is not None and len(values) == len(columns), "Invalid Presto row")
            rows.append(dict(zip(columns, values)))
        if not result.get("nextUri"):
            break
        next_url = result["nextUri"]
        # Follow only the configured coordinator, including when accessed on localhost.
        require(urlsplit(next_url).netloc == urlsplit(server).netloc,
                f"Coordinator returned a different host: {next_url}")
        request = Request(next_url, headers={"X-Presto-User": "bdsp_hw3"})
    print(sql.strip(), json.dumps(rows, ensure_ascii=False), sep="\n", flush=True)
    return rows


def compare_rows(presto_rows, mongo_rows):
    require(len(presto_rows) == 10 and len(mongo_rows) == 10, "Expected ten rows from each source")
    require(all(set(FIELDS).issubset(row) for row in presto_rows + mongo_rows),
            "A comparison row is missing required fields")
    return [{"rank": rank, "movie_id": left.get("movie_id"),
             "fields_match": {field: left.get(field) == right.get(field) for field in FIELDS}}
            for rank, (left, right) in enumerate(zip(presto_rows, mongo_rows), 1)]


def fingerprint(collection):
    documents = list(collection.find().sort("_id", 1))
    return hashlib.sha256(json_util.dumps(documents, sort_keys=True).encode()).hexdigest()


def verify_metadata(database):
    # Without this definition, even DESCRIBE may create MongoDB metadata.
    metadata = database["_schema"].find_one({"table": "movie_stats"})
    require(metadata is not None,
            "Existing bdsp_hw3._schema metadata for movie_stats is required. "
            "Stopping before SQL to avoid connector metadata writes; see presto/README.md.")
    expected = {"movie_id": "bigint", "title": "varchar", "rating_count": "bigint", "avg_rating": "double"}
    fields = {field["name"]: field["type"] for field in metadata.get("fields", [])}
    require(all(fields.get(name) == datatype for name, datatype in expected.items()),
            f"Existing connector metadata has unexpected field types: {fields}; no changes were made")


def run(args, archive):
    require(urlsplit(args.server).scheme in ("http", "https"), "Invalid coordinator URL")
    # Fail before sending SQL if the already-running coordinator is unavailable.
    with urlopen(args.server.rstrip("/") + "/v1/info", timeout=10) as response:
        info = json.load(response)
    (archive / "presto_info.json").write_text(json.dumps(info, indent=2) + "\n")
    version = info.get("nodeVersion", {}).get("version")
    require(isinstance(version, str) and version, "Coordinator did not report a Presto version")
    require(not info.get("starting", False), "Presto is still starting; retry after readiness")
    warnings = []
    if version.split("-", 1)[0] != "0.298.1":
        warnings.append({"message": f"Coordinator version {version} differs from configured 0.298.1"})
    print("Presto version:", version, flush=True)
    with MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=5000,
                     socketTimeoutMS=30000) as client:
        client.admin.command("ping")
        collection = client.bdsp_hw3.movie_stats
        require(collection.count_documents({}) > 0, "MongoDB movie_stats is empty")
        verify_metadata(client.bdsp_hw3)
        before = fingerprint(collection)
        queries = [statement.strip() for statement in
                   (ROOT / "presto/sql/verify.sql").read_text().split(";") if statement.strip()]
        require(len(queries) == 3, "verify.sql must contain exactly the three discovery statements")
        outputs = {"catalogs": query(args.server, "SHOW CATALOGS", archive / "catalogs", warnings)}
        require(any("mongodb" in row.values() for row in outputs["catalogs"]), "MongoDB catalog missing")
        for label, sql in zip(("schemas", "tables", "describe"), queries):
            outputs[label] = query(args.server, sql, archive / label, warnings)
        require(any("bdsp_hw3" in row.values() for row in outputs["schemas"]), "Schema missing")
        require(any("movie_stats" in row.values() for row in outputs["tables"]), "Table missing")
        require(set(FIELDS).issubset({row.get("Column") for row in outputs["describe"]}),
                "DESCRIBE is missing required fields")
        types = {row["Column"]: row["Type"] for row in outputs["describe"]}
        require(all(types[field] == expected for field, expected in
                    (("movie_id", "bigint"), ("title", "varchar"),
                     ("rating_count", "bigint"), ("avg_rating", "double"))),
                f"Unexpected Presto column types: {types}")
        sql = (ROOT / "presto/sql/top10.sql").read_text()
        top10 = query(args.server, sql, archive / "top10", warnings)
        mongo_top10 = list(collection.find({"rating_count": {"$gte": 20}}, {"_id": 0})
                           .sort([("avg_rating", -1), ("rating_count", -1), ("movie_id", 1)]).limit(10))
        comparisons = compare_rows(top10, mongo_top10)
        matches = all(all(item["fields_match"].values()) for item in comparisons)
        top_three_match = all(all(item["fields_match"].values()) for item in comparisons[:3])
        unchanged = before == fingerprint(collection)
        result = {"run_id": args.run_id, "timestamp": datetime.now(timezone.utc).isoformat(),
                  "presto_info": info, "presto_version": version, "server": args.server,
                  "mongodb_version": client.server_info()["version"], "verification": outputs,
                  "sql": sql, "top10": top10, "mongo_top10": mongo_top10,
                  "comparisons": comparisons, "all_four_fields_match_all_ten": matches,
                  "top_three_all_fields_match": top_three_match,
                  "checks": {"catalog_visible": True, "schema_visible": True,
                             "table_visible": True, "schema_types_match": True},
                  "warnings": warnings, "errors": [],
                  "movie_stats_unchanged": unchanged, "q6": Q6}
        (archive / "verification_results.json").write_text(json_util.dumps(result, indent=2) + "\n")
        require(matches, "Presto/MongoDB top-ten fields or ordering differ; see run evidence")
        require(unchanged, "MongoDB movie_stats changed during verification")
        table = "\n".join(
            f"| {r['movie_id']} | {r['title'].replace('|', '&#124;')} | {r['rating_count']} | {r['avg_rating']} |"
            for r in top10)
        comparison_table = "\n".join(
            "| " + str(row["rank"]) + " | " + str(row["movie_id"]) + " | "
            + " | ".join(str(row["fields_match"][field]) for field in FIELDS) + " |"
            for row in comparisons)
        schema_table = "\n".join(f"| {field} | {types[field]} |" for field in FIELDS)
        markdown = f"""# Part 5 results

Status: **verified**. Run: `{args.run_id}`.
Presto version: **{version}**. Presto info: `{json.dumps(info)}`.
MongoDB version: {result['mongodb_version']}.
Catalog, schema, table and required columns were verified. Full SQL responses
are archived under `report/presto/runs/{args.run_id}/`.
All four fields match MongoDB for all ten ranked rows, including the top three.
The movie_stats document fingerprint is unchanged.

## Table schema

| Column | Presto type |
|---|---|
{schema_table}

## Executed query and actual top ten

```sql
{sql.strip()}
```

| movie_id | title | rating_count | avg_rating |
|---|---|---|---|
{table}

## MongoDB comparison

Each row is compared against the corresponding position in the equivalently
filtered and sorted MongoDB result. Full values from both sources are saved in
`verification_results.json`. All top-three fields match: **{top_three_match}**.

| Rank | movie_id | movie_id matches | title matches | rating_count matches | avg_rating matches |
|---|---|---|---|---|---|
{comparison_table}

## Errors and warnings

Errors: none. Warnings: `{json.dumps(warnings)}`.

## Q6

{Q6}
"""
        (archive / "results.md").write_text(markdown)
        for name in ("verification_results.json", "results.md"):
            (ROOT / "report/presto" / name).write_bytes((archive / name).read_bytes())
        print(f"Part 5 verified; results: {ROOT / 'report/presto'}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", default="http://localhost:8080")
    parser.add_argument("--run-id", default=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"))
    args = parser.parse_args()
    require(re.fullmatch(r"[A-Za-z0-9_-]+", args.run_id), "Invalid run ID")
    archive = ROOT / "report/presto/runs" / args.run_id
    archive.mkdir(parents=True, exist_ok=False)
    try:
        run(args, archive)
    except Exception as error:
        message = f"Part 5 verification failed: {type(error).__name__}: {error}"
        print(message, file=sys.stderr)
        (archive / "error.txt").write_text(message + "\n")
        (archive / "status.json").write_text(json.dumps({
            "status": "failed", "run_id": args.run_id,
            "timestamp": datetime.now(timezone.utc).isoformat(), "errors": [message],
        }, indent=2) + "\n")
        print("Check the services manually; this script does not start services.", file=sys.stderr)
        return 1
    (archive / "status.json").write_text(json.dumps({
        "status": "verified", "run_id": args.run_id,
        "timestamp": datetime.now(timezone.utc).isoformat(), "errors": [],
    }, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
