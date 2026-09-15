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
     * A non-coordinator replica receives (and acknowledges) an Update and crashes
     * immediately after, before the write is committed. The quorum is unaffected,
     * so the write still completes and the crashed replica stays out of the
     * system.
     */
    @Test
    void nonCoordinatorCrashAfterReceivingUpdate() throws InterruptedException {
        final int COORDINATOR_ID = 0;
        final int CRASHED_REPLICA_ID = 1;
        final int TARGET_REPLICA_ID = N_NODES - 1;

        final TestsSystemWrapper sys = TestsCommons.createTestSystem(
                "crashAfterReceivingUpdate", N_NODES, COORDINATOR_ID);

        ClientHandle c = createClient(sys, "client", TARGET_REPLICA_ID);
        c.client().tell(
                new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE),
                Actor.noSender());

        TestKit stateProbe = new TestKit(sys.system);
        awaitState(sys, stateProbe, CRASHED_REPLICA_ID,
                s -> s.writeOkTimersSize >= 1 && s.commitHistorySize == 1);
        crash(sys, CRASHED_REPLICA_ID);

        // The crashed replica must report its crash (skipping any UpdateApplied if
        // the write happened to commit before the crash landed).
        sys.probes.get(CRASHED_REPLICA_ID).fishForMessage(
                Duration.ofMillis(300), "Crash", m -> m instanceof Crash);

        // Quorum is unaffected, so the write completes.
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

    /**
     * The coordinator commits a write (quorum of Acks reached) and crashes while
     * the WriteOk messages are being disseminated. The system must elect a new
     * coordinator and stay available for the following write/read.
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
