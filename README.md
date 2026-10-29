# Distributed Systems Project 2026
![Java](https://img.shields.io/badge/Java-ED8B00?style=flat-square&logo=java&logoColor=white)
![Akka](https://img.shields.io/badge/Akka-15A9CE?style=flat-square&logo=akka&logoColor=white)
![Gradle](https://img.shields.io/badge/Gradle-Build-02303A?style=flat-square&logo=gradle&logoColor=white)

Repository for the **Distributed Systems** project assigned in the academic year **2025–2026**. 
The project implements a quorum-based total order broadcast protocol in **Java** using **Akka Actors** to securely manage a distributed intelligence database of geographical positions.

## Requirements
- Java 8+ (with Akka 2.6)
- Gradle 9.2.1 (or use the included wrapper)

## How to Use

Run the demo application (main class `it.unitn.ds.Main`), which executes a sequence of reads, writes, and crashes to stress-test system failures:
```bash
./gradlew run
```

Run the automated test suite (validating corner cases like crashes during update broadcasts, write affirmations, and mid-election phases):
```bash
./gradlew test
```

Build the project:
```bash
./gradlew build
```

## Project Structure
```text
main/
├── src/main/java/it/unitn/ds/
│   ├── Main.java               # Stress test to check the system response
│   ├── Replica.java            # Replica implementation (2PC, heartbeats, election, sync)
│   ├── Client.java             # Client implementation (read and write requests)
│   ├── Messages.java           # All inter-node message types + Helper classes
│   └── ReplicaManagers/
│       ├── ElectionManager.java  # Handles ring-based election protocol
│       ├── HeartbeatManager.java # Manages Heartbeat to check coordinator liveness
│       ├── SyncManager.java      # Post-election state synchronization
│       └── UpdateManager.java    # Handles update requests from clients
└── src/test/java/it/unitn/ds/
    └── PersonalTests.java      # Personalized tests to check edge cases
```


### System Architecture
The system consists of Akka actors acting as either **clients** or **replicas**. Replicas hold a replicated list of geographical positions (serialized as integers) and operate in one of three states: `NORMAL`, `ELECTION`, or `CRASHED`. To ensure data consistency even in the presence of node crashes, the system enforces uniform agreement, total order, and sequential consistency through a robust two-phase broadcast mechanism and an automated leader election protocol. 

### Core Features
- **Local Reads & Coordinated Writes**: Clients can contact any replica. Read requests are served locally and immediately by the contacted replica. Update (write) requests are forwarded to a central coordinator.
- **Two-Phase Broadcast (2PC) with Quorum**: The coordinator sequences every update using an incrementally assigned `<epoch, seqNum>` clock. It broadcasts the update and waits for a majority quorum ($\lfloor N/2\rfloor+1$) of Acknowledgments (ACKs) before broadcasting a `WriteOk` message to finalize the commit.
- **Strict Ordering**: The use of unique coordinator clocks and reliable FIFO channels guarantees total ordering. Operations requested by a client to a specific replica are sequentially consistent and observed in the same order they were sent.
- **Fault Detection & Staggered Heartbeats**: The coordinator sends periodic heartbeats to signal its liveness. Replicas utilize staggered heartbeat timers to avoid starting an election simultaneously if the coordinator fails.
- **Ring-Based Leader Election**: If the coordinator crashes, replicas enter the `ELECTION` state and route election messages through a ring topology. The replica containing the most up-to-date clock is elected as the new coordinator, with ties broken by selecting the lowest node ID.
- **Synchronization Phase**: To guarantee uniform agreement after a crash, the newly elected coordinator reconstructs the complete update history and synchronizes all replicas before starting the next epoch. Any incomplete client requests left unacknowledged are re-sent and processed.
