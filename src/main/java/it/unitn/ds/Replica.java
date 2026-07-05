package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

public class Replica extends AbstractReplica {
    private Map<Integer, ActorRef> group;
    private int coordinatorId;

    private Messages.NodeClock clock;
    private int epoch;
    private int seqNum;
    private HashMap<Messages.NodeClock, Integer> ackCounters;

    // TODO: create a storage systems with pending updates
    private Map<Messages.NodeClock, Messages.UpdateData> commitHistory;
    private int[] storage = new int[POSITIONS_LIST_LENGTH];

    private Messages.NodeClock pendingUpdateClock;
    private Messages.UpdateData pendingUpdateData;

    private Map<ActorRef, Messages.NodeClock> myClients = new HashMap<>();
    private Map<Messages.NodeClock, ActorRef> updateClients = new HashMap<>();

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
        debug("Received UPDATE_REQUEST from client " + getSender().path().name() + " for index: " + _msg.index
                + " and value: " + _msg.value);
        if (this.id == coordinatorId) {
            // THIS IS THE COORDINATOR
            // - forward to other replicas UPDATE MESSAGE
            // - save client in myClients and updateClients with current clock
            
            this.seqNum++;
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
            updateClients.put(new Messages.NodeClock(this.epoch, this.seqNum), _msg.client);
            myClients.put(_msg.client, new Messages.NodeClock(this.epoch, this.seqNum));
        } else {
            // THIS IS NOT THE COORDINATOR
            // - forward the request to the coordinator
            // - add client to the list of clients without clock
            group.get(coordinatorId).tell(_msg, getSelf());

            myClients.put(_msg.client, null);
        }
        // TODO: handle timeout (I guess)
    }

    private final void handleUpdate(Messages.Update _msg) throws Exception {
        debug("Received UPDATE from coordinator " + getSender().path().name() + " for clock: " + _msg.clock.toString());
        this.pendingUpdateClock = _msg.clock;
        this.pendingUpdateData = new Messages.UpdateData(_msg.index, _msg.value);

        updateClients.put(_msg.clock, _msg.client);

        if (myClients.containsKey(_msg.client) && myClients.get(_msg.client) == null) {
            myClients.put(_msg.client, new Messages.NodeClock(this.epoch, this.seqNum));
        }

        // send ACK back to the coordinator
        group.get(coordinatorId).tell(new Messages.Ack(_msg.clock), getSelf());

        // TODO: handle timeout (I guess)
    }

    private final void handleAck(Messages.Ack _msg) throws Exception {
        debug("Received ACK from replica " + getSender().path().name() + " for clock: " + _msg.clock.toString());
        // incerment number of received ack for the _msg.NodeClock
        this.ackCounters.putIfAbsent(_msg.clock, 1);
        this.ackCounters.put(_msg.clock, this.ackCounters.get(_msg.clock) + 1);
        // print the ackCounters for debug purposes
        debug("Ack counters: " + this.ackCounters.toString());

        // if number of ack received >= (N/2 + 1) [quorum]
        if (this.ackCounters.get(_msg.clock) >= (Math.floor(group.size() / 2) + 1)) {
            // apply the changes to local storage
            // send the writeOk to all the others
            for (Map.Entry<Integer, ActorRef> entry : group.entrySet()) {
                if (entry.getKey() != this.id) {
                    entry.getValue().tell(new Messages.WriteOk(_msg.clock), getSelf());
                }
            }
            ActorRef client = updateClients.remove(_msg.clock);
            if (client != null) {
                Messages.NodeClock expectedClock = myClients.get(client);
                if (expectedClock != null && expectedClock.equals(_msg.clock)) {
                    Messages.UpdateData clientData = commitHistory.get(_msg.clock);
                    tell(new AbstractClient.WriteResult(true, clientData.index, clientData.value, this.id), client);
                    myClients.remove(client);
                }
            }

            this.ackCounters.remove(_msg.clock);
        }

        // TODO: handle timeout (I guess)
    }

    private final void handleWriteOk(Messages.WriteOk _msg) throws Exception {
        debug("Received WRITE_OK from replica " + getSender().path().name() + " for clock: " + _msg.clock.toString());
        // update internal state with the new values
        if (pendingUpdateClock != null && pendingUpdateClock.equals(_msg.clock)) {
            commitHistory.put(pendingUpdateClock, pendingUpdateData);
            storage[pendingUpdateData.index] = pendingUpdateData.value;

            // trigger the testing functions
            callbackOnUpdateApplied(pendingUpdateData.index, pendingUpdateData.value);

            pendingUpdateClock = null;
            pendingUpdateData = null;
        }
        ActorRef client = updateClients.remove(_msg.clock);

        if (client != null) {
            Messages.NodeClock expectedClock = myClients.get(client);
            if (expectedClock != null && expectedClock.equals(clock)) {
                Messages.UpdateData data = commitHistory.get(clock);
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
        // TODO: Start heartbeat scheduler if I am the coordinator
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                // TODO: add your message handlers here .match(, )
                .match(AbstractReplica.InitSystem.class, this::initSystem)
                .match(Messages.UpdateRequest.class, this::handleUpdateRequest)
                .match(Messages.Update.class, this::handleUpdate)
                .match(Messages.Ack.class, this::handleAck)
                .match(Messages.WriteOk.class, this::handleWriteOk)
                .match(Messages.ReadRequest.class, this::handleReadRequest)
                .build();
    }

}
