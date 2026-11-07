# HW3 Kafka broker

This configuration runs one Apache Kafka 3.9.1 node with both broker and
controller roles in KRaft mode. It does not require ZooKeeper.

## Configuration

- Compose project: `bdsp-hw3`; service: `kafka`; container: `bdsp-hw3-kafka`.
- Host Python clients use `bootstrap_servers="localhost:9092"`. Port 9092 is
  published only on host loopback. The broker advertises this same address so
  clients can reach it after discovering broker metadata.
- The controller uses container port 9093, which is not published to the host.
  Its single quorum voter is node 1 at `localhost:9093` inside the container.
- `PLAINTEXT` carries broker traffic; `CONTROLLER` carries KRaft traffic. Both
  use unencrypted PLAINTEXT, suitable for this local homework setup.
- New topics default to three partitions and replication factor one. Consumer
  offsets and transaction state topics also use replication factor one; both
  transaction-state and ordinary minimum in-sync replica counts are one.
  This single-node configuration provides no broker fault tolerance.
- `CLUSTER_ID` is a fixed KRaft initialization identifier. Keep it and node ID 1
  unchanged when reusing the data volume.
- Kafka logs (including KRaft metadata by default) persist in
  `/var/lib/kafka/data`, backed by named volume `bdsp-hw3-kafka-data`.
- JVM heap starts at 256 MiB and is capped at 512 MiB. The container memory
  limit is 1 GiB, leaving room beyond the heap for native memory and other
  allocations. These are initial limits for the small homework workload,
  not measured capacity guarantees. Stop Kafka after E1 to free memory.

This listener configuration supports clients on the host and CLI tools inside
the Kafka container. Clients in other containers need a separate advertised
listener; their `localhost` addresses do not refer to this broker.

## Manual commands

Run these commands yourself from the repository root. Docker daemon access is
required; discovery previously returned permission denied for the Docker socket.
If that remains unresolved, report the error before continuing.

Start the broker (Compose pulls the image if necessary):

```bash
docker compose -f kafka/docker-compose.yml up -d
```

Inspect container status and recent logs:

```bash
docker compose -f kafka/docker-compose.yml ps
docker compose -f kafka/docker-compose.yml logs --tail=100 kafka
```

Follow startup logs if needed; Ctrl+C stops log following, leaving Kafka running:

```bash
docker compose -f kafka/docker-compose.yml logs -f kafka
```

Once startup completes, verify that the broker answers Kafka protocol requests
and inspect its KRaft quorum:

```bash
docker compose -f kafka/docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092
docker compose -f kafka/docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status
docker compose -f kafka/docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

An empty topic list is normal for a fresh broker. A running container alone
does not establish broker readiness. The API command should return supported
APIs; the quorum output should identify leader ID 1 and the configured cluster.
These commands check access inside the container; the host Python producer
connection will be verified separately during E1.

When ready to prepare the two E1 topics, explicitly set the assignment values:

```bash
docker compose -f kafka/docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic ratings-5k --partitions 3 --replication-factor 1
docker compose -f kafka/docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic ratings-30k --partitions 3 --replication-factor 1
docker compose -f kafka/docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic ratings-5k
docker compose -f kafka/docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic ratings-30k
```

Each description should show three partitions, replication factor one, and
leader/replica/ISR node 1. Topic creation reports an error if the topic already
exists; inspect it before reusing it. Existing records remain across restarts,
so do not assume a repeated producer run starts from empty topics.

Stop the broker while keeping the container and persistent data:

```bash
docker compose -f kafka/docker-compose.yml stop kafka
```

Alternatively, remove the container and Compose network while retaining data:

```bash
docker compose -f kafka/docker-compose.yml down
```

Run `up -d` again to resume using the same named volume. Do not add `-v` to
`down`: that would delete the persistent Kafka data volume.

## Validation status

The files were inspected statically. No Docker commands were executed by the
assistant, so image startup, volume permissions, memory sufficiency, and broker
operation remain unverified. Provide the startup logs and verification output
before proceeding to the producer experiment.
