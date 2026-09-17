package it.unitn.ds;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import it.unitn.ds.AbstractReplica.InitSystem;
import it.unitn.ds.Messages.StateInfoResponse;

public class Main {

    public static void main(String[] args) throws Exception {
        System.out.println("========================================");
        System.out.println("START: nonCoordinatorCrashAfterReceivingUpdate");
        System.out.println("========================================\n");

        final int N_REPLICAS = 7;
        final int COORDINATOR_ID = 0;
        final int CRASHED_REPLICA_ID = 1;
        final int TARGET_REPLICA_ID = N_REPLICAS - 1; // 6

        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);

        final ActorSystem system = ActorSystem.create("crashAfterReceivingUpdate");

        // --- Create replicas (same as TestsCommons.createTestSystem) ---
        Map<Integer, ActorRef> replicas = new HashMap<>(N_REPLICAS);
        for (int i = 0; i < N_REPLICAS; i++) {
            replicas.put(i, system.actorOf(
                    Replica.props(i, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY,
                            AbstractReplica.COORDINATOR_BEAT_INTERVAL),
                    "Replica_" + i));
        }

        InitSystem initMsg = new InitSystem(replicas, COORDINATOR_ID);
        for (Map.Entry<Integer, ActorRef> entry : replicas.entrySet()) {
            entry.getValue().tell(initMsg, ActorRef.noSender());
        }

        Thread.sleep(500); // let initSystem run + coordinator start heartbeats

        // --- Create client targeting replica N-1 (same as test) ---
        long clientReadTimeout = (long) AbstractReplica.MAX_LATENCY * N_REPLICAS * 8;
        long electionMaxDelay = (long) (AbstractReplica.COORDINATOR_BEAT_INTERVAL * 3.0)
                + ((long) AbstractReplica.MAX_LATENCY * N_REPLICAS * 2);
        long ringHops = (long) N_REPLICAS * AbstractReplica.MAX_LATENCY * 2;
        long clientWriteTimeout = clientReadTimeout + (electionMaxDelay + ringHops) * 5;

        ActorRef client = system.actorOf(
                Client.props(clientReadTimeout, clientWriteTimeout,
                        Optional.of(replicas.get(TARGET_REPLICA_ID))),
                "client");

        // --- Client writes (same as test) ---
        System.out.println(">>> Client write (index=0, value=10) to replica " + TARGET_REPLICA_ID);
        client.tell(new AbstractClient.WriteRequest(0, 10), ActorRef.noSender());

        // --- Wait until replica 1 received the Update but has NOT concluded the
        //     write yet (same condition as awaitState:
        //     writeOkTimersSize >= 1 && commitHistorySize == 1) ---
        long deadline = System.currentTimeMillis()
                + (6 * AbstractReplica.MAX_LATENCY + N_REPLICAS + 200); // getMaxUpdateDelay
        boolean conditionMet = false;
        while (System.currentTimeMillis() < deadline) {
            StateInfoResponse s = (StateInfoResponse) Patterns.ask(
                    replicas.get(CRASHED_REPLICA_ID),
                    new Messages.StateInfoRequest(),
                    Duration.ofMillis(1000))
                    .toCompletableFuture().get();
            System.out.println(">>> Replica " + s.replicaId
                    + " state: writeOkTimers=" + s.writeOkTimersSize
                    + " commitHistory=" + s.commitHistorySize
                    + " coordinator=" + s.coordinatorId);
            if (s.writeOkTimersSize >= 1 && s.commitHistorySize == 1) {
                conditionMet = true;
                break;
            }
            Thread.sleep(2);
        }
        if (!conditionMet) {
            System.out.println(">>> WARNING: state condition not met before deadline, crashing anyway");
        }

        // --- Crash non-coordinator replica 1 (same as test) ---
        System.out.println(">>> Crashing non-coordinator replica " + CRASHED_REPLICA_ID);
        replicas.get(CRASHED_REPLICA_ID).tell(
                new AbstractReplica.Crash(AbstractReplica.Crash.Type.Now, 0), ActorRef.noSender());

        // --- Let the crash land (300ms in the test) ---
        Thread.sleep(300);

        // --- Wait for write to complete (same window as getMaxUpdateDelay) ---
        long writeWait = 6 * AbstractReplica.MAX_LATENCY + N_REPLICAS + 200;
        System.out.println(">>> Waiting " + writeWait + "ms for write to complete...");
        Thread.sleep(writeWait);

        // --- Client reads (same as test) ---
        System.out.println(">>> Client read (index=0) to replica " + TARGET_REPLICA_ID);
        client.tell(new AbstractClient.ReadRequest(0), ActorRef.noSender());

        long readWait = (long) AbstractReplica.MAX_LATENCY * N_REPLICAS; // getLatencyPlusEpsilon
        System.out.println(">>> Waiting " + readWait + "ms for read to complete...");
        Thread.sleep(readWait);

        System.out.println(">>> Terminating system.");
        system.terminate();

        System.out.println("\n========================================");
        System.out.println("END");
        System.out.println("========================================\n");
    }

}