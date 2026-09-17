package it.unitn.ds;

import java.util.HashMap;
import java.util.Map;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds.AbstractReplica.InitSystem;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("========================================");
        System.out.println("START: callbackOnCoordinatorElectedAllAgree (coordinator=0, n_nodes=5)");
        System.out.println("========================================\n");

        final int N_REPLICAS = 5;
        final int COORDINATOR_ID = 0;

        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);

        final ActorSystem system = ActorSystem.create("callbackCoordElected_0_5");

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

        // --- Crash coordinator (same as test) ---
        System.out.println(">>> Crashing coordinator " + COORDINATOR_ID);
        replicas.get(COORDINATOR_ID).tell(
                new AbstractReplica.Crash(AbstractReplica.Crash.Type.Now, 0), ActorRef.noSender());

        // --- Wait for election (same window as TestsCommons.getElectionMaxDelay) ---
        long electionMaxDelay = (long) (AbstractReplica.COORDINATOR_BEAT_INTERVAL * 3.0)
                + ((long) AbstractReplica.MAX_LATENCY * N_REPLICAS * 2);
        long ringHops = (long) N_REPLICAS * AbstractReplica.MAX_LATENCY * 2;
        long window = (electionMaxDelay + ringHops) * 5;

        System.out.println(">>> Waiting " + window + "ms for election to complete...");
        Thread.sleep(window);

        System.out.println(">>> Election window elapsed. Terminating system.");
        system.terminate();

        System.out.println("\n========================================");
        System.out.println("END");
        System.out.println("========================================\n");
    }

}
