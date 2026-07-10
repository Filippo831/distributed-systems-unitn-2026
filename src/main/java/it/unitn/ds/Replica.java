package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props; // from doc: something that can be cancelled, with method .cancel()
import scala.concurrent.duration.Duration;
import java.util.concurrent.TimeUnit;

public class Replica extends AbstractReplica {
    private Map<Integer, ActorRef> group;
    private int coordinatorId;

    private int epoch;
    private int seqNum;
    private HashMap<Messages.NodeClock, Integer> ackCounters;

    // TODO: create a storage systems with pending updates
    private Map<Messages.NodeClock, Messages.UpdateData> commitHistory;
    private int[] storage = new int[POSITIONS_LIST_LENGTH];

    private TreeMap<Messages.NodeClock, Messages.UpdateData> toCommitQueue;
    private ArrayList<Messages.NodeClock> ackedList;

    private Map<ActorRef, Messages.NodeClock> myClients = new HashMap<>();
    private Map<Messages.NodeClock, ActorRef> updateClients = new HashMap<>();

    private Map<Messages.NodeClock, Messages.UpdateData> coordinatorProposals = new HashMap<>();

    private Cancellable heartbeatTimeout;

    // init timers to detect coordinator crashes
    private Cancellable heartbeatTimer = null; // wait for heartbeat message from coordinator, re-init once received
    private Cancellable updateTimer = null; // starts after forwarding write request to the coordinator, wait for UPDATE message from coordinator
    private Cancellable writeOkTimer = null; // starts after sending ACK in response to UPDATE message, wait for WRITEOK message from coordinator


    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL,
                Optional.empty());
    }

    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);

        // TODO: add all the initialization code you need here
        this.group = new HashMap<>();
        this.coordinatorId = -1;

        this.epoch = 0;
        this.seqNum = 0;

        this.ackCounters = new HashMap<>();
        this.commitHistory = new TreeMap<>();

        this.toCommitQueue = new TreeMap<>();
        this.ackedList = new ArrayList<>();
    }

    public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
        return Props.create(Replica.class,
                () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(int id, int minLatency, int maxLatency, int coordinatorBeatInterval,
            ActorRef listener) {
        return Props.create(Replica.class,
                () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
    }

    private final void handleUpdateRequest(Messages.UpdateRequest _msg) throws Exception {
        if (this.id == coordinatorId) {
            // debug("Replica " + this.id + " is the coordinator and received UpdateRequest
            // from client " + _msg.client);
            // THIS IS THE COORDINATOR
            // - forward to other replicas UPDATE MESSAGE
            // - save client in myClients and updateClients with current clock
            //

            this.seqNum++;
            Messages.NodeClock updateClock = new Messages.NodeClock(this.epoch, this.seqNum);
            Messages.UpdateData updateData = new Messages.UpdateData(_msg.index, _msg.value);

            // save the value of the update to later make it persistent for coordinator
            this.coordinatorProposals.put(updateClock, updateData);

            updateClients.put(new Messages.NodeClock(this.epoch, this.seqNum), _msg.client);

            if (!_msg.fromReplica) {
                myClients.put(_msg.client, new Messages.NodeClock(this.epoch, this.seqNum));
            }

            this.ackCounters.put(updateClock, 1);
            this.toCommitQueue.put(updateClock, updateData);

            // if is the coordinator who received the updateRequest, send an UPDATE to the
            // replicas
            for (Map.Entry<Integer, ActorRef> entry : group.entrySet()) {
                if (entry.getKey() != this.id) {
                    entry.getValue()
                            .tell(new Messages.Update(_msg.index, _msg.value,
                                    new Messages.NodeClock(this.epoch, this.seqNum), _msg.client),
                                    getSelf());
                }
            }
        } else {
            // THIS IS NOT THE COORDINATOR
            // - forward the request to the coordinator
            // - add client to the list of clients without clock
            // debug("Replica " + this.id + " forwarding UpdateRequest to coordinator " +
            // coordinatorId);
            Messages.UpdateRequest forwardMsg = new Messages.UpdateRequest(_msg.index, _msg.value,
                    _msg.client, true);
            group.get(coordinatorId).tell(forwardMsg, getSelf());

            myClients.put(_msg.client, null);
        }
        // TODO: handle timeout (I guess)
    }

    private final void handleUpdate(Messages.Update _msg) throws Exception {
        // debug("Replica " + this.id + " received Update from coordinator " +
        // coordinatorId + " with clock "
        // + _msg.clock);
        this.toCommitQueue.put(_msg.clock, new Messages.UpdateData(_msg.index, _msg.value));

        updateClients.put(_msg.clock, _msg.client);

        if (myClients.containsKey(_msg.client) && myClients.get(_msg.client) == null) {
            myClients.put(_msg.client, _msg.clock);
        }

        // send ACK back to the coordinator
        group.get(coordinatorId).tell(new Messages.Ack(_msg.clock), getSelf());

        // TODO: handle timeout (I guess)
    }

    private final void handleAck(Messages.Ack _msg) throws Exception {
        // debug("Replica " + this.id + " received Ack from replica " + getSender() + "
        // for clock " + _msg.clock);
        // incerment number of received ack for the _msg.NodeClock
        int currentCount = this.ackCounters.getOrDefault(_msg.clock, 0);
        currentCount++;
        this.ackCounters.put(_msg.clock, currentCount);

        // if number of ack received >= (N/2 + 1) [quorum] add the clock to the
        // ackdeList
        if (this.ackCounters.get(_msg.clock) >= (Math.floor(group.size() / 2) + 1)) {
            // keep track of the acked clocks to later commit them in order
            if (!this.ackedList.contains(_msg.clock)) {
                this.ackedList.add(_msg.clock);
                this.ackedList.sort((a, b) -> a.compareTo(b));
            }
            // iterate until the smallest clock in the ackedList is not the first in the
            // toCommitQueue.
            while (!this.ackedList.isEmpty() && !this.toCommitQueue.isEmpty()
                    && this.ackedList.get(0).equals(this.toCommitQueue.firstKey())) {

                Messages.NodeClock clockToCommit = this.ackedList.remove(0);
                Messages.UpdateData dataToCommit = this.toCommitQueue.remove(clockToCommit);

                if (dataToCommit != null) {
                    this.commitHistory.put(clockToCommit, dataToCommit);
                    this.storage[dataToCommit.index] = dataToCommit.value;

                    callbackOnUpdateApplied(dataToCommit.index, dataToCommit.value);

                    // send the writeOk to all the others
                    for (Map.Entry<Integer, ActorRef> entry : group.entrySet()) {
                        if (entry.getKey() != this.id) {
                            entry.getValue().tell(new Messages.WriteOk(clockToCommit), getSelf());
                        }
                    }

                    ActorRef client = updateClients.remove(clockToCommit);
                    if (client != null) {
                        Messages.NodeClock expectedClock = myClients.get(client);
                        if (expectedClock != null && expectedClock.equals(clockToCommit)) {
                            Messages.UpdateData clientData = commitHistory.get(clockToCommit);
                            tell(new AbstractClient.WriteResult(true, clientData.index, clientData.value, this.id),
                                    client);
                            myClients.remove(client);
                        }
                    }

                    this.ackCounters.remove(clockToCommit);
                }
            }
        }
        // TODO: handle timeout (I guess)
    }

    private final void handleWriteOk(Messages.WriteOk _msg) throws Exception {
        // debug("Replica " + this.id + " received WriteOk from replica " + getSender()
        // + " for clock " + _msg.clock);
        // update internal state with the new values
        Messages.UpdateData toCommitData = toCommitQueue.remove(_msg.clock);
        if (toCommitData != null) {
            commitHistory.put(_msg.clock, toCommitData);
            storage[toCommitData.index] = toCommitData.value;

            // trigger testing function
            callbackOnUpdateApplied(toCommitData.index, toCommitData.value);
        }

        ActorRef client = updateClients.remove(_msg.clock);

        if (client != null) {
            Messages.NodeClock expectedClock = myClients.get(client);
            if (expectedClock != null && expectedClock.equals(_msg.clock)) {
                Messages.UpdateData data = commitHistory.get(_msg.clock);
                tell(new AbstractClient.WriteResult(true, data.index, data.value, this.id), client);
                myClients.remove(client); // Clean up
            }
        }
    }

    private final void handleReadRequest(Messages.ReadRequest _msg) {
        int value = storage[_msg.index];
        // _msg.client.tell(new Messages.ReadResponse(_msg.index, value, this.id),
        // getSelf());
        tell(new AbstractClient.ReadResult(true, _msg.index, value, this.id), _msg.client);
    }

    private final void handleHeartbeat(Messages.Heartbeat _msg) {
        for (Map.Entry<Integer, ActorRef> entry : group.entrySet()) {
            if (entry.getKey() != this.id) {
                entry.getValue().tell(new Messages.Heartbeat(), getSelf());
            }
        }
    }

    public final void startCoordinatorHeartbeat() {
        // debug("Replica " + this.id + " starting coordinator heartbeat");
        getContext().getSystem().scheduler().scheduleAtFixedRate(
                Duration.create(getCoordinatorBeatInterval(), TimeUnit.MILLISECONDS),
                Duration.create(getCoordinatorBeatInterval(), TimeUnit.MILLISECONDS),
                getSelf(),
                new Messages.Heartbeat(),
                getContext().dispatcher(),
                getSelf());
    }

    private void resetHeartbeatTimeout() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'resetHeartbeatTimeout'");
    }

    @Override
    public int getSystemNumberOfActors() {
        return group.size();
    }

    @Override
    public void crash(AbstractReplica.Crash how_to_crash) {
        // TODO: implement
    }

    @Override
    public void initSystem(InitSystem sysInit) {
        this.group = sysInit.group;
        this.coordinatorId = sysInit.coordinator_id;
        // TODO: Start heartbeat scheduler if I am the coordinator
        if (this.id == this.coordinatorId) {
            startCoordinatorHeartbeat();
        } else {
            resetHeartbeatTimeout();
        }

    }

    // This methods handle message reception in different situations: NORMAL, ELECTION, CRASHED
    // .match() filters messages the replica can receive/handle in each state

    // NORMAL state: the replica is wroking normally, no crash detected
    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(AbstractReplica.InitSystem.class, this::initSystem)
                .match(Messages.UpdateRequest.class, this::handleUpdateRequest)
                .match(Messages.Update.class, this::handleUpdate)
                .match(Messages.Ack.class, this::handleAck)
                .match(Messages.WriteOk.class, this::handleWriteOk)

                // Also handle the timers
                .match(Messages.HeartbeatTimeout.class, this::handleHeartbeatTimeout)
                .match(Messages.UpdateTimeout.class, this::handleUpdateTimeout)
                .match(Messages.WriteOkTimeout.class, this::handleWriteOkTimeout)
                .match(Messages.ReadRequest.class, this::handleReadRequest)
                .match(Messages.Heartbeat.class, this::handleHeartbeat)
                .build();
    }

    // ELECTION state: coordinator crash detected, switched to election state, where we want to handle only the election messages, ignoring updates
    public final Receive createElectionReceive() {
        return createBaseReceiveBuilder()
                // handle election messages
                .build();
    }

    // CRASHED state: replica crashed, simulate this by ignoring all messages
    public final Receive createCrashedReceive() {
        return createBaseReceiveBuilder()
                .build();
    }

    // Method to handle timeouts
    public final void handleHeartbeatTimeout(Messages.HeartbeatTimeout _msg) throws Exception {


    }

    public final void handleUpdateTimeout(Messages.UpdateTimeout _msg) throws Exception {

    }

    public final void handleWriteOkTimeout(Messages.WriteOkTimeout _msg) throws Exception {

    }

}
