package it.unitn.ds;

import akka.testkit.javadsl.TestKit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.japi.Predicate;
import it.unitn.ds.AbstractClient.ReadResult;
import it.unitn.ds.AbstractClient.WriteResult;
import it.unitn.ds.AbstractReplica.CoordinatorElected;
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.AbstractReplica.ElectionStarted;
import it.unitn.ds.AbstractReplica.UpdateApplied;
import it.unitn.ds.TestsCommons.TestsSystemWrapper;

class PersonalTests {

    private static final int N_NODES = 7;

    // =========================================================================
    // Crash-simulation tests where the crash happens DURING a protocol phase
    // =========================================================================

    /**
     * The coordinator crashes while it is broadcasting the Update message of a
     * write. The in-flight write may be lost: what MUST hold is that the system
     * elects a new coordinator and remains fully available (write + read) for
     * the following requests.
     */
    @Test
    void coordinatorCrashDuringUpdateBroadcast() throws InterruptedException {
        final int COORDINATOR_ID = 0;
        final int TARGET_REPLICA_ID = N_NODES - 1; // 6, never crashed
        final int RECOVERY_VALUE = 20;

        final TestsSystemWrapper sys = TestsCommons.createTestSystem(
                "crashDuringUpdateBroadcast_" + COORDINATOR_ID, N_NODES, COORDINATOR_ID);

        // Client 1 issues the write that gets interrupted by the crash.
        ClientHandle firstClient = createClient(sys, "client", TARGET_REPLICA_ID);
        firstClient.client().tell(
                new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE),
                Actor.noSender());

        TestKit stateProbe = new TestKit(sys.system);
        int quorum = (N_NODES / 2) + 1;
        awaitState(sys, stateProbe, COORDINATOR_ID,
                s -> s.seqNum == 1 && !s.ackQuorumReached && s.latestAckCount < quorum);
        crash(sys, COORDINATOR_ID);

        // The system must elect a new coordinator and resume normal operation.
        awaitCoordinatorElected(sys, Collections.singleton(COORDINATOR_ID));
        Thread.sleep(TestsCommons.getMaxUpdateDelay(sys));

        // A fresh client (own probe) proves the system is available again.
        ClientHandle recoveryClient = createClient(sys, "recoveryClient", TARGET_REPLICA_ID);
        recoveryClient.client().tell(
                new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, RECOVERY_VALUE),
                Actor.noSender());
        WriteResult wr = (WriteResult) recoveryClient.probe().fishForMessage(
                Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)),
                "WriteResult",
                m -> m instanceof WriteResult);
        assertEquals(new WriteResult(true, TestsCommons.TEST_INDEX, RECOVERY_VALUE, TARGET_REPLICA_ID), wr);

        recoveryClient.client().tell(new AbstractClient.ReadRequest(TestsCommons.TEST_INDEX), Actor.noSender());
        ReadResult rr = (ReadResult) recoveryClient.probe().fishForMessage(
                Duration.ofMillis(TestsCommons.getLatencyPlusEpsilon(sys)),
                "ReadResult",
                m -> m instanceof ReadResult);
        assertEquals(new ReadResult(true, TestsCommons.TEST_INDEX, RECOVERY_VALUE, TARGET_REPLICA_ID), rr);

        sys.system.terminate();
    }


    /**
     * The coordinator commits a write (quorum of Acks reached) and crashes while
     * the WriteOk messages are being disseminated. The system must elect a new
     * coordinator. The original client must eventually receive the WriteResult
     * for its write (via post-crash synchronization), and the system must stay
     * available for the following write/read.
     */
    @Test
    void coordinatorCrashDuringWriteOkDissemination() throws InterruptedException {
        final int COORDINATOR_ID = 0;
        final int TARGET_REPLICA_ID = N_NODES - 1;
        final int RECOVERY_VALUE = 20;

        final TestsSystemWrapper sys = TestsCommons.createTestSystem(
                "crashDuringWriteOkDissemination", N_NODES, COORDINATOR_ID);

        ClientHandle firstClient = createClient(sys, "client", TARGET_REPLICA_ID);
        firstClient.client().tell(
                new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE),
                Actor.noSender());

        // commitToStorage fires callbackOnUpdateApplied just before the WriteOk
        // broadcast: observing this on the coordinator is the deterministic
        // "commit happened, WriteOk dissemination in progress" signal.
        sys.probes.get(COORDINATOR_ID).fishForMessage(
                Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)),
                "coordinatorCommit",
                m -> m instanceof UpdateApplied ua
                        && ua.replicaId == COORDINATOR_ID
                        && ua.index == TestsCommons.TEST_INDEX
                        && ua.value == TestsCommons.TEST_VALUE);

        // Crash the coordinator while the WriteOk messages are in flight.
        crash(sys, COORDINATOR_ID);

        awaitCoordinatorElected(sys, Collections.singleton(COORDINATOR_ID));
        Thread.sleep(TestsCommons.getMaxUpdateDelay(sys));

        // The original client must receive its WriteResult after the new
        // coordinator finishes synchronization. This catches the bug where
        // handleSynchronization applies the committed update but never calls
        // notifyClient for the originating client.
        WriteResult firstWr = (WriteResult) firstClient.probe().fishForMessage(
                Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)),
                "WriteResult",
                m -> m instanceof WriteResult);
        assertEquals(
                new WriteResult(true, TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE, TARGET_REPLICA_ID),
                firstWr);

        ClientHandle recoveryClient = createClient(sys, "recoveryClient", TARGET_REPLICA_ID);
        recoveryClient.client().tell(
                new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, RECOVERY_VALUE),
                Actor.noSender());
        WriteResult wr = (WriteResult) recoveryClient.probe().fishForMessage(
                Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)),
                "WriteResult",
                m -> m instanceof WriteResult);
        assertEquals(new WriteResult(true, TestsCommons.TEST_INDEX, RECOVERY_VALUE, TARGET_REPLICA_ID), wr);

        recoveryClient.client().tell(new AbstractClient.ReadRequest(TestsCommons.TEST_INDEX), Actor.noSender());
        ReadResult rr = (ReadResult) recoveryClient.probe().fishForMessage(
                Duration.ofMillis(TestsCommons.getLatencyPlusEpsilon(sys)),
                "ReadResult",
                m -> m instanceof ReadResult);
        assertEquals(new ReadResult(true, TestsCommons.TEST_INDEX, RECOVERY_VALUE, TARGET_REPLICA_ID), rr);

        sys.system.terminate();
    }

    // /**
    //  * A non-coordinator replica R crashes in the middle of the 2PC of a client
    //  * write: R has already forwarded the UpdateRequest to the coordinator (so the
    //  * write is in flight) but crashes BEFORE the coordinator reaches the quorum.
    //  * The remaining replicas still provide enough Acks, so the coordinator
    //  * commits the write and disseminates the WriteOk: the change becomes
    //  * persistent on the surviving replicas. R is dead and will never commit nor
    //  * notify the client, so C can only receive its WriteResult if the system
    //  * answers it directly (e.g. the coordinator, which knows both the client and
    //  * the committed clock, sends the WriteResult back to it).
    //  */
    // @Test
    // void replicaCrashBeforeQuorumClientStillReceivesWriteResult() throws InterruptedException {
    //     final int COORDINATOR_ID = 0;
    //     final int TARGET_REPLICA_ID = N_NODES - 1; // 6: the replica that crashes
    //     final int SURVIVOR_REPLICA_ID = 1;         // commits the write after the WriteOk
    //
    //     final TestsSystemWrapper sys = TestsCommons.createTestSystem(
    //             "replicaCrashBeforeQuorum", N_NODES, COORDINATOR_ID);
    //
    //     // C sends the write to R; R forwards it to the coordinator.
    //     ClientHandle client = createClient(sys, "client", TARGET_REPLICA_ID);
    //
    //     // ==== Make the crash deterministic ====
    //     // The whole 2PC uses direct ActorRef tells (no network channel), so it
    //     // completes in a few milliseconds: without any intervention the coordinator
    //     // can broadcast the WriteOk, and R can process it (committing and notifying
    //     // C itself), BEFORE the test's Crash message reaches R. To guarantee that R
    //     // crashes before the quorum is met, delay the Acks of the OTHER replicas by
    //     // flooding their mailboxes with StateInfoRequest: their Acks arrive at the
    //     // coordinator (and therefore the WriteOk dissemination) tens of milliseconds
    //     // later, while R forwards the request immediately. R is then crashed way
    //     // before the WriteOk can exist, so it can never commit nor notify C.
    //     // (The flood is safe: every delayed replica only arms its WriteOk timeout
    //     // AFTER its own Ack, and the heartbeat timeout is 2 * beat interval, so no
    //     // spurious election is triggered.)
    //     TestKit floodProbe = new TestKit(sys.system);
    //     Messages.StateInfoRequest flood = new Messages.StateInfoRequest();
    //     for (int i = 0; i < N_NODES; i++) {
    //         if (i != COORDINATOR_ID && i != TARGET_REPLICA_ID) {
    //             for (int j = 0; j < 120; j++) {
    //                 sys.actors.get(i).tell(flood, floodProbe.getRef());
    //             }
    //         }
    //     }
    //
    //     client.client().tell(
    //             new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE),
    //             Actor.noSender());
    //
    //     // Wait until R has forwarded the request to the coordinator and is still
    //     // waiting for the outcome of the 2PC, i.e. it has NOT committed/notified
    //     // the client yet:
    //     // - pendingUpdateRequestsSize >= 1 -> forwarded, Update not arrived yet
    //     // - writeOkTimersSize >= 1         -> acked, WriteOk not arrived yet
    //     // (This is checked on R itself on purpose. Checking the coordinator is NOT
    //     // reliable here: after the commit the coordinator removes the clock from
    //     // ackCounters, so ackQuorumReached/latestAckCount look the same as before
    //     // the quorum.)
    //     TestKit stateProbe = new TestKit(sys.system);
    //     awaitState(sys, stateProbe, TARGET_REPLICA_ID,
    //             s -> s.pendingUpdateRequestsSize >= 1 || s.writeOkTimersSize >= 1);
    //
    //     // Crash R before the WriteOk can reach it: it will never commit nor notify C.
    //     crash(sys, TARGET_REPLICA_ID);
    //
    //     // The survivors + the coordinator still form a quorum: the coordinator
    //     // commits and broadcasts the WriteOk, and the surviving replicas persist
    //     // the change.
    //     sys.probes.get(SURVIVOR_REPLICA_ID).fishForMessage(
    //             Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)),
    //             "UpdateAppliedOnSurvivor",
    //             m -> m instanceof UpdateApplied ua
    //                     && ua.index == TestsCommons.TEST_INDEX
    //                     && ua.value == TestsCommons.TEST_VALUE);
    //
    //     // C must receive its WriteResult even though the replica it wrote to
    //     // crashed before the commit. This assertion currently fails: nobody
    //     // answers C (the coordinator commits but does not notify the client).
    //     // Fix: the coordinator sends the WriteResult back to the client when it
    //     // commits the write.
    //     WriteResult wr = (WriteResult) client.probe().fishForMessage(
    //             Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)),
    //             "WriteResult",
    //             m -> m instanceof WriteResult);
    //     // fromReplica is the coordinator: it is the only node that can answer C
    //     // once R is gone.
    //     assertEquals(
    //             new WriteResult(true, TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE, COORDINATOR_ID), wr);
    //
    //     sys.system.terminate();
    // }

    /**
     * A replica crashes while the coordinator election is in progress (after the
     * coordinator crash triggered it). The ring election and the synchronization
     * must tolerate the extra crash and still converge on a coordinator, after
     * which the system is available again.
     */
    @Test
    void replicaCrashDuringCoordinatorElection() throws InterruptedException {
        final int COORDINATOR_ID = 0;
        final int CRASHED_DURING_ELECTION = 2;
        final int TARGET_REPLICA_ID = N_NODES - 1;

        final TestsSystemWrapper sys = TestsCommons.createTestSystem(
                "crashDuringElection", N_NODES, COORDINATOR_ID);

        // Trigger the election by crashing the coordinator.
        crash(sys, COORDINATOR_ID);

        // Wait until the election is actually in progress, then crash another
        // replica while the ring is circulating.
        awaitElectionStarted(sys, Collections.singleton(COORDINATOR_ID));
        crash(sys, CRASHED_DURING_ELECTION);

        // The election must still complete among the surviving replicas.
        awaitCoordinatorElected(sys, Set.of(COORDINATOR_ID, CRASHED_DURING_ELECTION));
        Thread.sleep(TestsCommons.getMaxUpdateDelay(sys));

        ClientHandle c = createClient(sys, "client", TARGET_REPLICA_ID);
        c.client().tell(
                new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE),
                Actor.noSender());
        WriteResult wr = (WriteResult) c.probe().fishForMessage(
                Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)),
                "WriteResult",
                m -> m instanceof WriteResult);
        assertEquals(new WriteResult(true, TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE, TARGET_REPLICA_ID), wr);

        c.client().tell(new AbstractClient.ReadRequest(TestsCommons.TEST_INDEX), Actor.noSender());
        ReadResult rr = (ReadResult) c.probe().fishForMessage(
                Duration.ofMillis(TestsCommons.getLatencyPlusEpsilon(sys)),
                "ReadResult",
                m -> m instanceof ReadResult);
        assertEquals(new ReadResult(true, TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE, TARGET_REPLICA_ID), rr);

        sys.system.terminate();
    }

    // =========================================================================
    // Preexisting tests (kept as-is, only the invalid ActorSystem name was fixed)
    // =========================================================================

    @ParameterizedTest(name = "some non-coordinator replicas crash, client writes, waits and reads => coordinator {0}, nodes {1}")
    @CsvSource({
            "7",
            "22",
    })
    void crashDuringUpdateBroadcast(int n_nodes) throws InterruptedException {
        final int COORDINATOR_ID = 0;
        final TestsSystemWrapper sys = TestsCommons
                .createTestSystem("crashDuringUpdateBroadcast_" + COORDINATOR_ID, n_nodes,
                        COORDINATOR_ID);
        TestKit probe = new TestKit(sys.system);
        int targetReplicaID = n_nodes - 1;

        ActorRef client = sys.system.actorOf(
                Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
                        Optional.ofNullable(sys.actors.get(targetReplicaID)), probe.getRef()),
                "client");

        // client send write request to the target replica (6)
        client.tell(new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE),
                Actor.noSender());

        Thread.sleep(TestsCommons.getMaxUpdateDelay(sys));

        // crash coordinator
        sys.actors.get(0).tell(new Crash(Crash.Type.Now, 0), Actor.noSender());

        WriteResult wr = (WriteResult) probe.fishForMessage(
                Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)),
                "WriteResult",
                msg -> msg instanceof WriteResult);
        assertEquals(
                new WriteResult(true, TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE, targetReplicaID), wr);

        Thread.sleep(TestsCommons.getMaxUpdateDelay(sys));

        client.tell(new AbstractClient.ReadRequest(TestsCommons.TEST_INDEX), Actor.noSender());
        ReadResult rr = (ReadResult) probe.fishForMessage(
                Duration.ofMillis(sys.client_read_timeout),
                "ReadResult",
                msg -> msg instanceof ReadResult);
        assertEquals(
                new ReadResult(true, TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE, targetReplicaID), rr);

        sys.system.terminate();
    }

    @ParameterizedTest(name = "coordinator and other 2 replicas crash, client writes, waits and reads => coordinator {0}, nodes {1}")
    @CsvSource({
            "1,7",
            "0,22",
    })
    void coordinatorCrashClientWritesWaitsReads(int coordinator, int n_nodes) throws InterruptedException {
        final TestsSystemWrapper sys = TestsCommons
                .createTestSystem("coordinatorCrashClientWritesWaitsReads_" + coordinator, n_nodes, coordinator);
        TestKit probe = new TestKit(sys.system);
        int targetReplicaID = n_nodes - 1;
        ActorRef client = sys.system.actorOf(
                Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
                        Optional.ofNullable(sys.actors.get(targetReplicaID)), probe.getRef()),
                "client");

        for (int i = 0; i < 3; i++) {
            sys.actors.get(i).tell(new Crash(Crash.Type.Now, 0), Actor.noSender());
        }

        client.tell(new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE),
                Actor.noSender());
        WriteResult wr = (WriteResult) probe.fishForMessage(
                Duration.ofMillis(TestsCommons.getElectionMaxDelay(sys) + TestsCommons.getMaxUpdateDelay(sys)),
                "WriteResult",
                msg -> msg instanceof WriteResult);
        assertEquals(
                new WriteResult(true, TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE, targetReplicaID), wr);

        client.tell(new AbstractClient.ReadRequest(TestsCommons.TEST_INDEX), Actor.noSender());
        ReadResult rr = (ReadResult) probe.fishForMessage(
                Duration.ofMillis(TestsCommons.getLatencyPlusEpsilon(sys)),
                "ReadResult",
                msg -> msg instanceof ReadResult);
        assertEquals(
                new ReadResult(true, TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE, targetReplicaID), rr);

        sys.system.terminate();
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private record ClientHandle(ActorRef client, TestKit probe) {
    }

    private ClientHandle createClient(TestsSystemWrapper sys, String name, int targetReplicaId) {
        TestKit probe = new TestKit(sys.system);
        ActorRef client = sys.system.actorOf(
                Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
                        Optional.ofNullable(sys.actors.get(targetReplicaId)), probe.getRef()),
                name);
        return new ClientHandle(client, probe);
    }

    private void crash(TestsSystemWrapper sys, int id) {
        sys.actors.get(id).tell(new Crash(Crash.Type.Now, 0), Actor.noSender());
    }

    private void awaitElectionStarted(TestsSystemWrapper sys, Set<Integer> skipReplicas) {
        long window = TestsCommons.getElectionMaxDelay(sys);
        for (int i = 0; i < sys.getNNodes(); i++) {
            if (skipReplicas.contains(i)) {
                continue;
            }
            try {
                sys.probes.get(i).fishForMessage(
                        Duration.ofMillis(window), "ElectionStarted", m -> m instanceof ElectionStarted);
                return; // the election is in progress
            } catch (AssertionError ignored) {
                // try the next replica
            }
        }
        fail("No replica reported that the election started");
    }

    private void awaitCoordinatorElected(TestsSystemWrapper sys, Set<Integer> skipReplicas) {
        long window = TestsCommons.getElectionMaxDelay(sys);
        int quorum = (sys.getNNodes() / 2) + 1;

        List<CoordinatorElected> received = new ArrayList<>();
        for (int i = 0; i < sys.getNNodes(); i++) {
            if (skipReplicas.contains(i)) {
                continue;
            }
            try {
                CoordinatorElected ce = (CoordinatorElected) sys.probes.get(i).fishForMessage(
                        Duration.ofMillis(window),
                        "CoordinatorElected",
                        m -> m instanceof CoordinatorElected);
                received.add(ce);
            } catch (AssertionError ignored) {
                // Probe timed out without seeing the callback — tolerated as long as
                // quorum-1 replicas fire it.
            }
        }

        assertTrue(received.size() >= quorum - 1,
                "At least a quorum of surviving replicas must report CoordinatorElected. "
                        + String.format("Received %d but quorum is %d", received.size(), quorum - 1));

        int elected = received.get(0).newCoordinatorId;
        for (CoordinatorElected ce : received) {
            assertEquals(elected, ce.newCoordinatorId, "All replicas must agree on the same new coordinator");
        }
        assertTrue(!skipReplicas.contains(elected),
                "The new coordinator must be a surviving replica, got " + elected);
    }

    private Messages.StateInfoResponse awaitState(TestsSystemWrapper sys, TestKit probe, int replicaId,
            Predicate<Messages.StateInfoResponse> condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + TestsCommons.getMaxUpdateDelay(sys);
        while (System.currentTimeMillis() < deadline) {
            sys.actors.get(replicaId).tell(new Messages.StateInfoRequest(), probe.getRef());
            Messages.StateInfoResponse s = probe.expectMsgClass(Duration.ofMillis(1000),
                    Messages.StateInfoResponse.class);
            if (condition.test(s))
                return s;
            Thread.sleep(2);
        }
        fail("Timeout waiting for state condition on replica " + replicaId);
        return null;
    }
}
