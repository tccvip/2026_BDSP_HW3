#!/usr/bin/env python3
"""Publish the first N MovieLens ratings and measure E1 client throughput."""

import argparse
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time

from kafka import KafkaProducer


RATINGS_PATH = Path(__file__).resolve().parents[1] / "data/ml-latest-small/ratings.csv"
LOG_DIR = Path(__file__).resolve().parents[1] / "logs/e1"


def positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def save_results(results, summary, errors):
    """Preserve prior runs and write the same summary shown in the terminal."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    # Keep filenames inside logs/e1 even if an invalid topic contains a separator.
    topic = results["topic"].replace("/", "_").replace("\\", "_")
    stem = topic
    suffix = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    attempt = 0
    while True:
        text_path = LOG_DIR / f"{stem}.txt"
        json_path = LOG_DIR / f"{stem}.json"
        if not text_path.exists() and not json_path.exists():
            break
        attempt += 1
        stem = f"{topic}-{suffix}-{attempt}"

    # Exclusive creation also prevents overwriting files created concurrently.
    with text_path.open("x", encoding="utf-8") as text_file:
        text_file.write(summary + "\n")
        for error in errors:
            text_file.write(error + "\n")
    with json_path.open("x", encoding="utf-8") as json_file:
        json.dump({**results, "errors": errors}, json_file, indent=2)
        json_file.write("\n")
    print(f"Text log: {text_path}")
    print(f"JSON results: {json_path}")


def publish(args):
    timestamp = datetime.now(timezone.utc).isoformat()
    with RATINGS_PATH.open(newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames != ["userId", "movieId", "rating", "timestamp"]:
            raise ValueError("ratings.csv has an unexpected header")

        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_server,
            acks="all",
        )
        try:
            futures = []
            failed = 0
            errors = []
            interrupted = False
            start = time.perf_counter()
            try:
                for index in range(args.num_records):
                    row = next(reader, None)
                    if row is None:
                        raise ValueError(f"CSV ended after {index} records")
                    if None in row or any(value is None for value in row.values()):
                        raise ValueError(f"Malformed CSV record {index + 1}")
                    key = row["userId"].encode("utf-8")
                    value = json.dumps(row).encode("utf-8")
                    try:
                        futures.append(producer.send(args.topic, key=key, value=value))
                    except Exception:
                        # This record was attempted but no delivery future was returned.
                        failed += 1
                        raise
            except (Exception, KeyboardInterrupt) as error:
                errors.append(f"Publishing stopped: {type(error).__name__}: {error}")
                interrupted = isinstance(error, KeyboardInterrupt)
            finally:
                try:
                    producer.flush()
                except (Exception, KeyboardInterrupt) as error:
                    errors.append(f"Flush failed: {type(error).__name__}: {error}")
                    interrupted |= isinstance(error, KeyboardInterrupt)
                finally:
                    elapsed = time.perf_counter() - start

            # flush() waits for delivery completion, but does not establish that
            # every send succeeded. Inspect each future outside the timed section.
            successful = 0
            unresolved = 0
            not_submitted = args.num_records - len(futures) - failed
            for future in futures:
                if not future.is_done:
                    unresolved += 1
                    continue
                try:
                    future.get(timeout=0)
                    successful += 1
                except Exception as error:
                    failed += 1
                    if not errors:
                        errors.append(f"Delivery failed: {type(error).__name__}: {error}")

            throughput = successful / elapsed if elapsed > 0 else 0.0
            results = {
                "topic": args.topic,
                "requested_records": args.num_records,
                "successful_records": successful,
                "failed_records": failed,
                "not_submitted_records": not_submitted,
                "unresolved_records": unresolved,
                # Match the precision displayed in the terminal summary.
                "elapsed_seconds": round(elapsed, 6),
                "throughput_records_per_second": round(throughput, 2),
                "timestamp": timestamp,
                "bootstrap_server": args.bootstrap_server,
            }
            summary = "\n".join([
                f"Topic: {results['topic']}",
                f"Requested records: {results['requested_records']}",
                f"Successful records: {results['successful_records']}",
                f"Failed records: {results['failed_records']}",
                f"Not submitted records: {results['not_submitted_records']}",
                f"Unresolved records: {results['unresolved_records']}",
                f"Elapsed seconds: {results['elapsed_seconds']:.6f}",
                "Throughput (successful records/second): "
                f"{results['throughput_records_per_second']:.2f}",
                f"Timestamp: {results['timestamp']}",
                f"Bootstrap server: {results['bootstrap_server']}",
            ])
            print(summary)
            for error in errors:
                print(error, file=sys.stderr)
            save_results(results, summary, errors)
            if interrupted:
                return 130
            return 0 if successful == args.num_records and not errors else 1
        finally:
            producer.close(timeout=10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--num-records", required=True, type=positive_int)
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    args = parser.parse_args()
    try:
        return publish(args)
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)
        return 130
    except Exception as error:
        print(f"Producer error: {type(error).__name__}: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
