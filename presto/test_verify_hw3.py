"""Offline fixtures only: no service connections or measured report artifacts."""

import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch
from types import SimpleNamespace
from urllib.error import HTTPError

import verify_hw3 as workflow
from verify_hw3 import compare_rows, query, verify_metadata


class VerificationTests(unittest.TestCase):
    def test_all_four_fields_match_and_detect_mismatch(self):
        rows = [{"movie_id": i, "title": f"Fixture {i}", "rating_count": 20,
                 "avg_rating": 4.0} for i in range(10)]
        comparisons = compare_rows(rows, rows)
        self.assertTrue(all(all(row["fields_match"].values()) for row in comparisons))
        other = [dict(row) for row in rows]
        other[0]["avg_rating"] = 3.5
        self.assertFalse(compare_rows(rows, other)[0]["fields_match"]["avg_rating"])

    def test_wrong_result_count_rejected(self):
        with self.assertRaisesRegex(ValueError, "ten rows"):
            compare_rows([], [])

    def test_missing_fields_are_not_treated_as_matching_nulls(self):
        with self.assertRaisesRegex(ValueError, "missing required fields"):
            compare_rows([{} for _ in range(10)], [{} for _ in range(10)])

    def test_existing_metadata_required_without_writes(self):
        database = MagicMock()
        database.__getitem__.return_value.find_one.return_value = None
        with self.assertRaisesRegex(ValueError, "Stopping before SQL"):
            verify_metadata(database)
        self.assertEqual(database.__getitem__.return_value.method_calls,
                         [unittest.mock.call.find_one({"table": "movie_stats"})])

    def test_http_pagination_and_raw_evidence(self):
        pages = [
            {"id": "fixture", "nextUri": "http://localhost:8080/v1/next"},
            {"columns": [{"name": "Catalog"}], "data": [["mongodb"]]},
        ]
        responses = [io.BytesIO(json.dumps(page).encode()) for page in pages]
        with tempfile.TemporaryDirectory() as folder, \
                patch("verify_hw3.urlopen", side_effect=responses) as fetch, patch("builtins.print"):
            evidence = Path(folder) / "pages"
            self.assertEqual(query("http://localhost:8080", "SHOW CATALOGS;", evidence),
                             [{"Catalog": "mongodb"}])
            self.assertEqual(fetch.call_count, 2)
            self.assertEqual(len(list(evidence.glob('*.json'))), 2)
            self.assertEqual(fetch.call_args_list[0].args[0].data, b'SHOW CATALOGS')

    def test_sql_error_saved_and_rejected(self):
        response = io.BytesIO(json.dumps({"error": {"message": "fixture error"}}).encode())
        with tempfile.TemporaryDirectory() as folder, patch("verify_hw3.urlopen", return_value=response):
            evidence = Path(folder) / "pages"
            with self.assertRaisesRegex(RuntimeError, "fixture error"):
                query("http://localhost:8080", "SELECT 1", evidence)
            self.assertTrue((evidence / '0000.json').exists())

    def test_different_pagination_host_rejected(self):
        response = io.BytesIO(json.dumps({"nextUri": "http://other:8080/v1/next"}).encode())
        with tempfile.TemporaryDirectory() as folder, patch("verify_hw3.urlopen", return_value=response):
            with self.assertRaisesRegex(ValueError, "different host"):
                query("http://localhost:8080", "SELECT 1", Path(folder) / 'pages')

    def test_http_error_body_saved(self):
        error = HTTPError("http://localhost:8080", 503, "Unavailable", {}, io.BytesIO(b"fixture unavailable"))
        with tempfile.TemporaryDirectory() as folder, patch("verify_hw3.urlopen", side_effect=error):
            evidence = Path(folder) / "pages"
            with self.assertRaisesRegex(RuntimeError, "fixture unavailable"):
                query("http://localhost:8080", "SELECT 1", evidence)
            self.assertEqual(json.loads((evidence / '0000-http-error.json').read_text())["http_status"], 503)

    def test_warnings_collected(self):
        warning = {"warningCode": {"name": "FIXTURE"}, "message": "offline fixture"}
        response = io.BytesIO(json.dumps({"warnings": [warning]}).encode())
        warnings = []
        with tempfile.TemporaryDirectory() as folder, patch("verify_hw3.urlopen", return_value=response), \
                patch("builtins.print"):
            query("http://localhost:8080", "SELECT 1", Path(folder) / "pages", warnings)
        self.assertEqual(warnings, [warning])

    def test_mocked_full_workflow_report(self):
        # All generated outputs stay in a temporary root and are discarded.
        rows = [{"movie_id": i + 1, "title": f"Fixture {i}", "rating_count": 20,
                 "avg_rating": 4.0} for i in range(10)]
        fields = {"movie_id": "bigint", "title": "varchar", "rating_count": "bigint", "avg_rating": "double"}
        outputs = {
            "catalogs": [{"Catalog": "mongodb"}], "schemas": [{"Schema": "bdsp_hw3"}],
            "tables": [{"Table": "movie_stats"}],
            "describe": [{"Column": key, "Type": value} for key, value in fields.items()], "top10": rows,
        }
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / 'presto/sql').mkdir(parents=True)
            for name in ['verify.sql', 'top10.sql']:
                (root / 'presto/sql' / name).write_bytes((workflow.ROOT / 'presto/sql' / name).read_bytes())
            archive = root / 'report/presto/runs/fixture'
            archive.mkdir(parents=True)
            client = MagicMock()
            database = client.bdsp_hw3
            database.__getitem__.return_value.find_one.return_value = {
                "fields": [{"name": key, "type": value} for key, value in fields.items()]}
            client.server_info.return_value = {"version": "fixture-server"}
            collection = database.movie_stats
            collection.count_documents.return_value = 10
            collection.find.return_value.sort.return_value.limit.return_value = rows
            info = io.BytesIO(json.dumps({"nodeVersion": {"version": "0.298.1-9e1b45f"}, "starting": False}).encode())
            with patch.object(workflow, 'ROOT', root), patch.object(workflow, 'MongoClient') as factory, \
                    patch.object(workflow, 'urlopen', return_value=info), \
                    patch.object(workflow, 'fingerprint', return_value='fixture-hash'), \
                    patch.object(workflow, 'query', side_effect=lambda server, sql, path, warnings: outputs[path.name]), \
                    patch('builtins.print'):
                factory.return_value.__enter__.return_value = client
                workflow.run(SimpleNamespace(server='http://localhost:8080', run_id='fixture'), archive)
            result = json.loads((archive / 'verification_results.json').read_text())
            self.assertEqual(result['presto_version'], '0.298.1-9e1b45f')
            self.assertEqual(result['warnings'], [])
            self.assertTrue(result['top_three_all_fields_match'])
            self.assertTrue(all(result['checks'].values()))
            self.assertEqual(result['errors'], [])
            self.assertEqual(len(result['comparisons']), 10)
            self.assertIn('MongoDB comparison', (archive / 'results.md').read_text())
            self.assertEqual((archive / 'results.md').read_bytes(), (root / 'report/presto/results.md').read_bytes())

    def test_failed_run_status_and_no_success_report(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(workflow, 'ROOT', Path(folder)), \
                patch('sys.argv', ['verify_hw3.py', '--run-id', 'fixture']), \
                patch.object(workflow, 'run', side_effect=RuntimeError('offline failure')), \
                patch('sys.stderr', new_callable=io.StringIO):
            self.assertEqual(workflow.main(), 1)
            archive = Path(folder) / 'report/presto/runs/fixture'
            status = json.loads((archive / 'status.json').read_text())
            self.assertEqual(status['status'], 'failed')
            self.assertIn('offline failure', status['errors'][0])
            self.assertFalse((Path(folder) / 'report/presto/results.md').exists())


if __name__ == "__main__":
    unittest.main()
