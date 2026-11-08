# Part 5 report status: verified

Actual live verification completed at 2026-11-07T10:20:40.872305+00:00.
Run: `20261107T102039993884991-51086`. Presto: `0.298.1-9e1b45f`.
MongoDB: `7.0.45`. Exit code: **0**.

- MongoDB catalog, bdsp_hw3 schema, movie_stats table, and column types verified.
- Required top-ten SQL executed; all four fields match MongoDB for all ten rows.
- All top-three fields match, and the movie_stats document fingerprint is unchanged.
- Final verification recorded no errors or warnings.

See [results.md](results.md) for actual rows, comparisons and Q6, and
[verification_results.json](verification_results.json) for machine-readable evidence.
Full HTTP responses and status are archived in `runs/20261107T102039993884991-51086/`.
The matching log is `logs/presto/20261107T102039993884991-51086.log` at the project root.

Reproduce against the existing services from the project root:

```bash
bash scripts/run_presto_hw3.sh --server http://localhost:8080
```

No Docker commands or MongoDB writes are executed by this workflow.
Earlier run archives are retained; use the run ID above for the final results.
