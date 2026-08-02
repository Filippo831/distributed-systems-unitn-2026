package it.unitn.ds;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds.AbstractReplica.InitSystem;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("========================================");
        System.out.println("START");
        System.out.println("========================================\n");
        final int N_REPLICAS = 22;
        final int COORDINATOR_ID = 0;   // same variants as the test: (1,7) or (0,22)
        final int TARGET_REPLICA_ID = N_REPLICAS - 1;

        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);

        final ActorSystem system = ActorSystem.create("DebugCoordinatorCrash");

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

        ActorRef client = system.actorOf(
                Client.props(AbstractReplica.MAX_LATENCY * N_REPLICAS * 8, 30000,
                        Optional.of(replicas.get(TARGET_REPLICA_ID))),
                "client");

        // STEP 1: crash coordinator + 2 other replicas (same as the test)
        System.out.println(">>> STEP 1: crashing replicas 0,1,2 (coordinator " + COORDINATOR_ID + " included)");
        for (int i = 0; i < 3; i++) {
            replicas.get(i).tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.Now, 0), ActorRef.noSender());
        }
        Thread.sleep(200);

        // STEP 2: client writes to replica N-1 -> forwarded to dead coordinator
        //         -> UpdateTimeout -> election -> sync -> pending resend -> commit
        System.out.println(">>> STEP 2: client write (index=0, value=10) to replica " + TARGET_REPLICA_ID);
        client.tell(new AbstractClient.WriteRequest(0, 10), ActorRef.noSender());

        // STEP 3: wait for crash detection (~2s heartbeat / 45ms UpdateTimeout) + election + sync + resend
        Thread.sleep(8000);

        // STEP 4: read back from the same replica
        System.out.println(">>> STEP 4: client read (index=0) to replica " + TARGET_REPLICA_ID);
        client.tell(new AbstractClient.ReadRequest(0), ActorRef.noSender());

        Thread.sleep(3000);
        system.terminate();

        System.out.println("\n========================================");
        System.out.println("END");
        System.out.println("========================================\n");
    }

}
