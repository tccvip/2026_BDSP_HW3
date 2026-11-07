# HW3 - Big Data Storage and Processing

## Project Overview

Implement a small data pipeline using:
- HDFS for raw data storage
- Kafka for ratings event ingestion
- Spark for batch processing
- MongoDB for serving derived data
- PrestoDB for interactive SQL queries

Dataset: MovieLens Latest Small.

This is an individual academic assignment.
Prioritize correctness, reproducibility and experimental analysis
over production-level complexity.

## Requirements

Follow the original homework specification.

Important constraints:
- Use the official MovieLens Latest Small dataset.
- Keep original CSV files unchanged.
- HDFS replication factor = 1.
- Kafka: one KRaft broker, three partitions, replication factor = 1.
- Spark: local mode.
- MongoDB: single instance.
- PrestoDB: MongoDB connector.
- Kafka-to-Spark streaming is optional, not required.

Do not introduce unnecessary infrastructure.

## Development Workflow

Work incrementally, one homework part at a time.

Before implementation:
1. Inspect the current environment.
2. Check installed software and versions.
3. Understand existing configurations.
4. Propose an implementation plan.

After implementation:
1. Execute relevant commands.
2. Verify outputs and data consistency.
3. Save logs and experimental measurements.
4. Explain observed behavior.

Do not proceed to the next major phase without
verifying the current phase.

## Experimental Requirements

Three required experiments:

E1: Kafka producer throughput
- 5,000 records
- 30,000 records
- Record elapsed time, throughput and actual count.

E2: Spark CSV vs Parquet
- Same aggregation query.
- One warm-up run per format.
- Three timed runs per format.
- Compare median runtime and verify matching results.

E3: MongoDB indexing
- Run explain("executionStats") before indexing.
- Create index on movie_id.
- Run the same query again.
- Compare execution plans and documents examined.

Never fabricate experimental results.

## Coding Guidelines

- Prefer simple, readable implementations.
- Avoid unnecessary dependencies.
- Keep configuration separate from application code.
- Make data processing steps reproducible.
- Handle errors explicitly.
- Preserve existing user data.

## Reporting

Keep experimental evidence in logs and structured tables.

The final report must contain:
- Environment and dataset information.
- Implementation overview.
- E1-E3 experiment results.
- Answers to Q1-Q8.
- Reproducibility instructions.

Use actual observations from the local environment.
Never invent performance measurements.

## Docker Execution Policy

- Docker commands must only be executed manually by the user.
- Never execute `docker`, `docker compose`, or `sudo docker` commands.
- You may create and modify Dockerfiles and docker-compose.yml files.
- You may inspect Docker configuration files.
- When Docker operations are required, provide the exact commands
  and explain their purpose.
- Wait for the user to execute the commands and provide the output
  before proceeding.