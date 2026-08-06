# Distributed Systems Project 2026
![Java](https://img.shields.io/badge/Java-ED8B00?style=flat-square&logo=java&logoColor=white)
![Akka](https://img.shields.io/badge/Akka-15A9CE?style=flat-square&logo=akka&logoColor=white)
![Gradle](https://img.shields.io/badge/Gradle-Build-02303A?style=flat-square&logo=gradle&logoColor=white)

Repository for the **Distributed Systems** project assigned in the academic year **2025–2026**.  
The project is implemented in **Java** using **Akka Actors**.

## Requirements

- Java 8+ (with Akka 2.6)
- Gradle 9.2.1 (or use the included wrapper)

## How to Use

The project is an Akka system of **replicas** (nodes sharing replicated memory) and **clients**
that issue read/write requests against a specific replica, which forwards operations to a
**coordinator** replica.

Run the demo application (main class `it.unitn.ds.Main`):

```bash
./gradlew run
```

Run automated tests:

```bash
./gradlew test
```

Build the project:

```bash
./gradlew build
```

## Features

- **Replicated shared memory**: each replica stores an array of `POSITIONS_LIST_LENGTH` positions;
  reads can be served locally by any replica.
- **Two-phase commit (2PC)**: the coordinator sequences every write with an `<epoch, seqNum>` clock,
  proposes it to the other replicas, and commits once a majority quorum (`floor(N/2) + 1`) of ACKs
  is reached. Writes are committed in order and then made durable.
- **Coordinator fault detection**: the coordinator sends periodic heartbeats; replicas reset a
  staggered timeout on each heartbeat and start the election protocol when it fires.
- **Election protocol**: a ring-based election picks the replica with the most up-to-date clock
  (ties broken by highest node ID). The winner becomes the new coordinator and starts a new epoch.
- **State synchronization**: after election, the new coordinator merges the update history of all
  replicas, brings every replica up to date, and re-sends any client writes left unacknowledged.
- **Client timeouts**: reads and writes have per-request timers and are reported if they time out.
