package it.unitn.ds;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds.AbstractReplica.InitSystem;

public class Main {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("START");
        System.out.println("========================================\n");

        // final int N_REPLICAS = 5;
        // final int COORDINATOR_ID = 0;
        // final ActorSystem system = ActorSystem.create("TestMain");
        //
        //
        // Logger.setDestinationStdout();
        // Logger.setDebugEnabled(true);
        //
        // Map<Integer, ActorRef> replicas = new HashMap<>(N_REPLICAS);
        // for (int i = 0; i < N_REPLICAS; i++) {
        //     replicas.put(i,
        //         system.actorOf(
        //             Replica.props(i, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL),
        //             "Replica_" + i
        //         )
        //     );
        // }
        //
        // InitSystem initMsg = new InitSystem(replicas, COORDINATOR_ID);
        // for (Map.Entry<Integer, ActorRef> entry : replicas.entrySet()) {
        //     entry.getValue().tell(initMsg, ActorRef.noSender());
        // }
        //
        // // TODO: Create your clients
        // ActorRef testClient = system.actorOf(Client.props(2000, 2000, Optional.of(replicas.get(0))), "Client_1");
        //
        // // TODO: Implement your main logic
        // // write a value and read it back from the same replica
        // testClient.tell(new AbstractClient.WriteRequest(1, 100, replicas.get(0)), ActorRef.noSender());
        // // wait for a while to let the write complete
        // try {
        //     Thread.sleep(1000);
        // } catch (InterruptedException e) {
        //     e.printStackTrace();
        // }
        // testClient.tell(new AbstractClient.ReadRequest(1, replicas.get(0)), ActorRef.noSender());
        //
        //
        // system.terminate();

		// final int NUM_WRITES = 5;
		// final int NUM_READS_PER_CLIENT = 15;
		// final int READ_INTERVAL_MS = TestsCommons.getMaxUpdateDelay(sys) / 2;
		//
		// // Create write client with its own probe
		// ActorRef writeClient = sys.system.actorOf(
		// 		Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout, Optional.ofNullable(sys.actors.get(0)), null),
		// 		"clientWrite");
		//
		// // Create read clients with read probe
		// List<ActorRef> readClients = new ArrayList<>();
		// for (int i = 0; i < 22; i++) {
		// 	ActorRef readClient = sys.system.actorOf(
		// 			Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout, Optional.ofNullable(sys.actors.get(i)), readProbe.getRef()),
		// 			"clientRead_replica_" + i);
		// 	readClients.add(readClient);
		// }
		//
		// // Start all read clients in parallel
		// CountDownLatch startLatch = new CountDownLatch(1);
		// List<Thread> readThreads = new ArrayList<>();
		//
		// for (ActorRef readClient : readClients) {
		// 	Thread readThread = new Thread(() -> {
		// 		try {
		// 			startLatch.await();
		// 			for (int i = 0; i < NUM_READS_PER_CLIENT; i++) {
		// 				readClient.tell(new AbstractClient.ReadRequest(TestsCommons.TEST_INDEX), Actor.noSender());
		// 				Thread.sleep(READ_INTERVAL_MS);
		// 			}
		// 		} catch (InterruptedException e) {
		// 			Thread.currentThread().interrupt();
		// 		}
		// 	});
		// 	readThread.start();
		// 	readThreads.add(readThread);
		// }
		//
		// // Send all writes
		// for (int val = 0; val < NUM_WRITES; val++) {
		// 	writeClient.tell(new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, val), Actor.noSender());
		// }
		//
		// // Start all read requests simultaneously
		// startLatch.countDown();
		//
		// // Wait for all read threads to complete
		// for (Thread thread : readThreads) {
		// 	thread.join();
		// }
		//
		// // Collect and verify sequential consistency
		// Map<Integer, List<Integer>> replicaValues = new HashMap<>();
		// int totalExpectedReads = readClients.size() * NUM_READS_PER_CLIENT;
		//
		// for (int i = 0; i < totalExpectedReads; i++) {
		// 	ReadResult result = readProbe.expectMsgClass(ReadResult.class);
		// 	replicaValues.computeIfAbsent(result.fromReplica, k -> new ArrayList<>()).add(result.value);
		// }
		//
		// sys.system.terminate();
		//
		// // Verify sequential consistency
		// System.out.println("- TEST OUTCOME (coordinator: " + coordinator + ") -");
		// for (Map.Entry<Integer, List<Integer>> entry : replicaValues.entrySet()) {
		// 	int replicaId = entry.getKey();
		// 	List<Integer> values = entry.getValue();
		//
		// 	System.out.println("Replica " + replicaId + " values: " + values);
		//
		// 	for (int i = 1; i < values.size(); i++) {
		// 		assertTrue(
		// 				values.get(i - 1) == null || values.get(i) >= values.get(i - 1),
		// 				String.format(
		// 						"Sequential consistency violated for replica %d: value at index %d (%d) is less than previous value (%d)",
		// 						replicaId, i, values.get(i), values.get(i - 1)));
		// 	}
		// }
        System.out.println("\n========================================");
        System.out.println("END");
        System.out.println("========================================\n");
    }

}
