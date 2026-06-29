package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class Replica extends AbstractReplica {
    private Map<Integer, ActorRef> group;
    private int coordinatorId;

    private int epoch;
    private int seqNum;

    private HashMap<Messages.NodeClock, Integer> ackCounters;

    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL,
                Optional.empty());
    }

    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
        this.group = new HashMap<>();
        this.coordinatorId = -1;
        this.epoch = 0;
        this.seqNum = 0;
        this.ackCounters = new HashMap<>();

        // TODO: add all the initialization code you need here
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
        // TODO: update internal state with the new values
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

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                // TODO: add your message handlers here .match(, )
                .match(AbstractReplica.InitSystem.class, this::initSystem)
                .match(Messages.UpdateRequest.class, this::handleUpdateRequest)
                .match(Messages.Update.class, this::handleUpdate)
                .match(Messages.Ack.class, this::handleAck)
                .match(Messages.WriteOk.class, this::handleWriteOk)
                .build();
    }

}
