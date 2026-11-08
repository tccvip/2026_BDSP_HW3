# Part 5 results

Status: **verified**. Run: `20261107T102039993884991-51086`.
Presto version: **0.298.1-9e1b45f**. Presto info: `{"nodeVersion": {"version": "0.298.1-9e1b45f"}, "environment": "bdsp_hw3", "coordinator": true, "starting": false, "uptime": "6.24m"}`.
MongoDB version: 7.0.45.
Catalog, schema, table and required columns were verified. Full SQL responses
are archived under `report/presto/runs/20261107T102039993884991-51086/`.
All four fields match MongoDB for all ten ranked rows, including the top three.
The movie_stats document fingerprint is unchanged.

## Table schema

| Column | Presto type |
|---|---|
| movie_id | bigint |
| title | varchar |
| rating_count | bigint |
| avg_rating | double |

## Executed query and actual top ten

```sql
SELECT movie_id, title, rating_count, avg_rating
FROM mongodb.bdsp_hw3.movie_stats
WHERE rating_count >= 20
ORDER BY avg_rating DESC, rating_count DESC, movie_id ASC
LIMIT 10;
```

| movie_id | title | rating_count | avg_rating |
|---|---|---|---|
| 1104 | Streetcar Named Desire, A (1951) | 20 | 4.475 |
| 318 | Shawshank Redemption, The (1994) | 317 | 4.429 |
| 922 | Sunset Blvd. (a.k.a. Sunset Boulevard) (1950) | 27 | 4.3333 |
| 898 | Philadelphia Story, The (1940) | 29 | 4.3103 |
| 1204 | Lawrence of Arabia (1962) | 45 | 4.3 |
| 475 | In the Name of the Father (1993) | 25 | 4.3 |
| 246 | Hoop Dreams (1994) | 29 | 4.2931 |
| 858 | Godfather, The (1972) | 192 | 4.2891 |
| 1235 | Harold and Maude (1971) | 26 | 4.2885 |
| 168252 | Logan (2017) | 25 | 4.28 |

## MongoDB comparison

Each row is compared against the corresponding position in the equivalently
filtered and sorted MongoDB result. Full values from both sources are saved in
`verification_results.json`. All top-three fields match: **True**.

| Rank | movie_id | movie_id matches | title matches | rating_count matches | avg_rating matches |
|---|---|---|---|---|---|
| 1 | 1104 | True | True | True | True |
| 2 | 318 | True | True | True | True |
| 3 | 922 | True | True | True | True |
| 4 | 898 | True | True | True | True |
| 5 | 1204 | True | True | True | True |
| 6 | 475 | True | True | True | True |
| 7 | 246 | True | True | True | True |
| 8 | 858 | True | True | True | True |
| 9 | 1235 | True | True | True | True |
| 10 | 168252 | True | True | True | True |

## Errors and warnings

Errors: none. Warnings: `[]`.

## Q6

PrestoDB is a distributed SQL query engine; this homework uses one coordinator that also executes work. MongoDB remains the persistent data source. The MongoDB connector translates metadata access and supported SQL operations into MongoDB requests, while Presto executes the remaining query work. Presto reads through the connector and does not permanently copy the entire collection into its own storage.
