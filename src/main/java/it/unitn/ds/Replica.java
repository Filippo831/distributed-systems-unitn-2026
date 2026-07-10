package it.unitn.ds;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props; // from doc: something that can be cancelled, with method .cancel()

public class Replica extends AbstractReplica {
    private Map<Integer, ActorRef> group;
    private int coordinatorId;

    private int epoch;
    private int seqNum;
    private HashMap<Messages.NodeClock, Integer> ackCounters;

    // TODO: create a storage systems with pending updates
    private Map<Messages.NodeClock, Messages.UpdateData> commitHistory;
    private int[] storage = new int[POSITIONS_LIST_LENGTH];

    private Messages.NodeClock pendingUpdateClock;
    private Messages.UpdateData pendingUpdateData;

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

        this.pendingUpdateClock = null;
        this.pendingUpdateData = null;

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
        System.out.println("received UpdateRequest from client");
        if (this.id == coordinatorId) {
            // if is the coordinator who received the updateRequest, send an UPDATE to the
            // replicas
            for (Map.Entry<Integer, ActorRef> entry : group.entrySet()) {
                if (entry.getKey() != this.id) {
                    entry.getValue()
                            .tell(new Messages.Update(_msg.index, _msg.value,
                                    new Messages.NodeClock(this.epoch, this.seqNum)),
                                    getSelf());
                }
            }
        } else {
            // if not the coordinator, forward to the coordinator
            group.get(coordinatorId).tell(_msg, getSelf());
        }
        // TODO: handle timeout (I guess)
    }

    private final void handleUpdate(Messages.Update _msg) throws Exception {
        this.pendingUpdateClock = _msg.clock;
        this.pendingUpdateData = new Messages.UpdateData(_msg.index, _msg.value);

        // send ACK back to the coordinator
        _msg.clock.incrementSeqNum();
        group.get(coordinatorId).tell(new Messages.Ack(_msg.clock), getSelf());

        // TODO: handle timeout (I guess)
    }

    private final void handleAck(Messages.Ack _msg) throws Exception {
        // incerment number of received ack for the _msg.NodeClock
        this.ackCounters.putIfAbsent(_msg.clock, 1);
        this.ackCounters.put(_msg.clock, this.ackCounters.get(_msg.clock) + 1);

        // if number of ack received > (N/2 + 1) [quorum]
        if (this.ackCounters.get(_msg.clock) > (Math.floor(group.size() / 2) + 1)) {
            // send the writeOk to all the others
            _msg.clock.incrementSeqNum();
            for (Map.Entry<Integer, ActorRef> entry : group.entrySet()) {
                if (entry.getKey() != this.id) {
                    entry.getValue().tell(new Messages.WriteOk(_msg.clock), getSelf());
                }
            }
        }

        // TODO: handle timeout (I guess)
    }

    private final void handleWriteOk(Messages.WriteOk _msg) throws Exception {
        // update internal state with the new values
        if (pendingUpdateClock != null && pendingUpdateClock.equals(_msg.clock)) {
            commitHistory.put(pendingUpdateClock, pendingUpdateData);
            storage[pendingUpdateData.index] = pendingUpdateData.value;

            // trigger the testing functions
            callbackOnUpdateApplied(pendingUpdateData.index, pendingUpdateData.value);

            pendingUpdateClock = null;
            pendingUpdateData = null;
        }
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
        // TODO: implement
        this.group = sysInit.group;
        this.coordinatorId = sysInit.coordinator_id;
        System.out.println("replica init");
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
