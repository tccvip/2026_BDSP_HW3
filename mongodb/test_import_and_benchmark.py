"""Offline tests; synthetic plans are fixtures, never experiment evidence."""

import json
import contextlib
import io
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

import import_and_benchmark as workflow
from pymongo.errors import ConnectionFailure

from import_and_benchmark import explain_metrics, load_records, plan_uses_index, stage_names


class ImportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name)
        self.document = {"movie_id": 1, "title": "Test Movie", "rating_count": 30, "avg_rating": 4}

    def write_parts(self, documents):
        (self.folder / "part-00000.json").write_text(
            "".join(json.dumps(doc) + "\n" for doc in documents), encoding="utf-8")

    def test_metadata_ignored_and_double_conversion(self):
        self.write_parts([self.document])
        for name in ("_SUCCESS", ".part-hidden.json", "metadata.json"):
            (self.folder / name).write_text("not JSON")
        docs, manifest = load_records(self.folder)
        self.assertEqual(len(docs), 1)
        self.assertIs(type(docs[0]["avg_rating"]), float)
        self.assertEqual(len(manifest), 1)
        self.assertEqual(len(manifest[0]["sha256"]), 64)

    def test_multiple_part_files(self):
        self.write_parts([self.document])
        other = dict(self.document, movie_id=2)
        (self.folder / "part-00001.json").write_text(json.dumps(other) + "\n")
        self.assertEqual(len(load_records(self.folder)[0]), 2)

    def test_missing_parts(self):
        with self.assertRaisesRegex(ValueError, "No part"):
            load_records(self.folder)

    def test_invalid_records_rejected(self):
        invalid = [dict(self.document, movie_id=True), dict(self.document, movie_id=2**63),
                   dict(self.document, rating_count=19), dict(self.document, rating_count=30.0),
                   dict(self.document, avg_rating=float("nan")),
                   dict(self.document, avg_rating=True), dict(self.document, title=""),
                   dict(self.document, unexpected=1)]
        for document in invalid:
            with self.subTest(document=document):
                self.write_parts([document])
                with self.assertRaises(ValueError):
                    load_records(self.folder)

    def test_duplicate_id_and_missing_query_id(self):
        self.write_parts([self.document, self.document])
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            load_records(self.folder)
        self.write_parts([dict(self.document, movie_id=2)])
        with self.assertRaisesRegex(ValueError, "lacks movie_id 1"):
            load_records(self.folder)

    def test_invalid_json_has_file_and_line(self):
        (self.folder / "part-00000.json").write_text("invalid\n")
        with self.assertRaisesRegex(ValueError, "part-00000.json:1"):
            load_records(self.folder)

    def test_nested_sbe_and_classic_stages(self):
        fixture = {"queryPlanner": {
            "winningPlan": {"queryPlan": {"stage": "FETCH", "inputStage": {
                "stage": "IXSCAN", "indexName": "movie_id_1"}}},
            "rejectedPlans": [{"stage": "COLLSCAN"}]},
            "executionStats": {"nReturned": 1, "totalDocsExamined": 1,
                               "executionTimeMillis": 0,
                               "executionStages": {"stage": "nlj", "children": [{"stage": "ixseek"}]}}}
        metrics = explain_metrics(fixture)
        self.assertEqual(metrics["plan_stages"], ["FETCH", "IXSCAN"])
        self.assertEqual(metrics["execution_stages"], ["ixseek", "nlj"])
        self.assertTrue(plan_uses_index(metrics["winning_plan"], "movie_id_1"))
        self.assertFalse(plan_uses_index(metrics["winning_plan"], "other_index"))
        self.assertEqual(stage_names({"stage": "COLLSCAN"}), ["COLLSCAN"])

    def test_ping_failure_stops_before_input_or_database_changes(self):
        client = Mock()
        client.admin.command.side_effect = ConnectionFailure("offline test failure")
        with patch.object(workflow, "MongoClient") as factory, patch.object(workflow, "load_records") as load:
            factory.return_value.__enter__.return_value = client
            with self.assertRaises(ConnectionFailure):
                workflow.run(SimpleNamespace(input_dir=self.folder, run_id="test"))
        client.admin.command.assert_called_once_with("ping")
        client.server_info.assert_not_called()
        self.assertEqual(client.method_calls, [call.admin.command("ping")])
        load.assert_not_called()

    def test_connection_failure_message_requests_manual_resolution(self):
        output = io.StringIO()
        with patch.object(workflow, "run", side_effect=ConnectionFailure("offline test failure")), \
                patch("sys.argv", ["import_and_benchmark.py"]), contextlib.redirect_stderr(output):
            self.assertEqual(workflow.main(), 1)
        self.assertIn("MongoDB is unreachable", output.getvalue())
        self.assertIn("Start/check MongoDB manually", output.getvalue())


if __name__ == "__main__":
    unittest.main()
