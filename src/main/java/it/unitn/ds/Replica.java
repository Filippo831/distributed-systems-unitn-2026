package it.unitn.ds;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.ds.ReplicaManagers.ElectionManager;
import it.unitn.ds.ReplicaManagers.HeartbeatManager;
import it.unitn.ds.ReplicaManagers.SyncManager;
import it.unitn.ds.ReplicaManagers.UpdateManager;
import scala.concurrent.duration.Duration;

public class Replica extends AbstractReplica {

    // =================================================================================
    // Shared state with the managers
    // =================================================================================

    // maps node Id and node "address", contains all replicas, also coordinator
    public Map<Integer, ActorRef> group;
    public int coordinatorId;

    public int epoch;
    public int seqNum;

    // persistent storage of applied updates, kept in commit order
    public TreeMap<Messages.NodeClock, Messages.UpdateData> commitHistory;
    public int[] storage = new int[POSITIONS_LIST_LENGTH];

    // updates still waiting to be committed (in coordinator-assigned order)
    public TreeMap<Messages.NodeClock, Messages.UpdateData> toCommitQueue;

    // replicas that are known to have crashed
    public Set<Integer> crashedReplicas = new HashSet<>();

    public int timerDuration;
    
    // keep track if there is a crash requested
    public Crash pendingCrash = null;
    public int n_messages_of_type = 0;

    // =================================================================================
    // Managers
    // =================================================================================
    private final UpdateManager updateManager;
    private final ElectionManager electionManager;
    private final SyncManager syncManager;
    private final HeartbeatManager heartbeatManager;

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

        this.commitHistory = new TreeMap<>();
        this.toCommitQueue = new TreeMap<>();

        this.updateManager = new UpdateManager(this);
        this.heartbeatManager = new HeartbeatManager(this);
        this.electionManager = new ElectionManager(this);
        this.syncManager = new SyncManager(this);
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

    // =================================================================================
    // Actor accessors (exposed so the managers can use actor internals)
    // =================================================================================

    public ActorRef getSelfRef() {
        return getSelf();
    }

    public ActorRef getSenderRef() {
        return getSender();
    }

    public ActorContext actorContext() {
        return getContext();
    }

    // Public wrappers around the package-private helpers of AbstractReplica,
    // so the managers (which live in the ReplicaManagers package) can use them
    public int getId() {
        return id;
    }

    public void sendTo(java.io.Serializable m, ActorRef dst) {
        tell(m, dst);
    }

    public void logInfo(String msg) {
        log(msg);
    }

    public void debugInfo(String msg) {
        debug(msg);
    }

    public void onCoordinatorElected(int newCoordinatorId) {
        callbackOnCoordinatorElected(newCoordinatorId);
    }

    public void onUpdateApplied(int index, int value) {
        callbackOnUpdateApplied(index, value);
    }

    public void onElectionStarted(int crashedCoordinatorId) {
        callbackOnElectionStarted(crashedCoordinatorId);
    }

    // Manager getters, used by the managers to cooperate (e.g. sync -> update)
    public UpdateManager updateManager() {
        return updateManager;
    }

    public ElectionManager electionManager() {
        return electionManager;
    }

    public SyncManager syncManager() {
        return syncManager;
    }

    public HeartbeatManager heartbeatManager() {
        return heartbeatManager;
    }

    // Helper function to broadcast a message to all replicas in the group except
    // itself
    public final void broadcast(Object message) {
        for (Map.Entry<Integer, ActorRef> entry : group.entrySet()) {
            if (entry.getKey() != this.id) {
                sendTo((java.io.Serializable) message, entry.getValue());
            }
        }
    }

    // Helper function to crate an object timer
    public final Cancellable createTimer(Object message, long duration) {
        return getContext().system().scheduler().scheduleOnce(
                Duration.create(duration, TimeUnit.MILLISECONDS), // timer duration
                getSelf(), // destination (self)
                message, // message that will be received
                getContext().dispatcher(), // dispatcher
                getSelf() // sender (self)
        );
    }

    // utility function to cancel all timers
    public final void cancelAllTimers() {
        heartbeatManager.cancelTimers();
        electionManager.cancelTimers();
        updateManager.cancelTimers();
    }

    // =================================================================================
    // Read path
    // =================================================================================

    private final void handleReadRequest(Messages.ReadRequest _msg) {
        int value = storage[_msg.index];

        tell(new AbstractClient.ReadResult(true, _msg.index, value, this.id), _msg.client);
    }

    @Override
    public int getSystemNumberOfActors() {
        return group.size();
    }

    @Override
    public void crash(AbstractReplica.Crash how_to_crash) {
        if (how_to_crash.type == Crash.Type.Now) {
            // change state: NORMAL/ELECTION -> CRASH
            // this is done by changing the node behaviour using the "message filter"
            // defined in createCrashedReceive
            // this way it stops handling messages
            getContext().become(createCrashedReceive());
            return;
        } 
        this.pendingCrash = how_to_crash;
        this.n_messages_of_type = 0;
    }

    @Override
    public void initSystem(InitSystem sysInit) {
        this.group = sysInit.group;
        this.coordinatorId = sysInit.coordinator_id;
        this.timerDuration = getMaxLatencyPlusTolerance();
        if (this.id == this.coordinatorId) {
            // if this node is the coordinator, start sending heartbeat messages to the
            // other nodes
            heartbeatManager.startCoordinatorHeartbeat();
        } else {
            // initialize the heartbeat timer if this node is not the coordinator
            heartbeatManager.resetTimeout();
        }

    }

    // =================================================================================
    // Message handling
    // =================================================================================

    // this methods handle message reception in different situations: NORMAL,
    // ELECTION, CRASHED
    // .match() filters messages the replica can receive/handle in each state

    // NORMAL state: the replica is wroking normally, no crash detected
    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(AbstractReplica.InitSystem.class, this::initSystem)
                .match(Messages.UpdateRequest.class, updateManager::handleUpdateRequest)
                .match(Messages.Update.class, updateManager::handleUpdate)
                .match(Messages.Ack.class, updateManager::handleAck)
                .match(Messages.WriteOk.class, updateManager::handleWriteOk)
                .match(Messages.Heartbeat.class, heartbeatManager::handleHeartbeat) // handle heartbeat

                .match(Messages.Election.class, electionManager::handleElection)

                // also handle the timeouts
                .match(Messages.HeartbeatTimeout.class, this::handleHeartbeatTimeout)
                .match(Messages.UpdateTimeout.class, this::handleUpdateTimeout)
                .match(Messages.WriteOkTimeout.class, this::handleWriteOkTimeout)
                .match(Messages.ReadRequest.class, this::handleReadRequest)
                .build();
    }

    // ELECTION state: coordinator crash detected, switched to election state, where
    // we want to handle only the election messages, ignoring updates
    public final Receive createElectionReceive() {
        return createBaseReceiveBuilder()
                // handle election messages, synch messages and acks
                .match(Messages.Election.class, electionManager::handleElection)
                .match(Messages.ElectionAck.class, electionManager::handleElectionAck)
                .match(Messages.Synchronization.class, syncManager::handleSynchronization)

                // .match(Messages.UpdateSyncRequest.class, syncManager::handleUpdateSyncRequest)
                // .match(Messages.UpdateSyncResponse.class, syncManager::handleUpdateSyncResponse)

                // also handle the timeouts
                .match(Messages.ElectionTimeout.class, electionManager::handleElectionTimeout)
                .match(Messages.ElectionAckTimeout.class, electionManager::handleElectionAckTimeout)

                //.match(Messages.UpdateSyncTimeout.class, syncManager::handleUpdateSyncTimeout)
                .build();
    }

    // CRASHED state: replica crashed, simulate this by ignoring all messages
    public final Receive createCrashedReceive() {
        return createBaseReceiveBuilder()
                .build();
    }

    // =================================================================================
    // Timeouts management
    // == (all of them just trigger the election protocol)
    // =================================================================================

    // handle Update message timeout
    public final void handleUpdateTimeout(Messages.UpdateTimeout _msg) {
        electionManager.startElection();
    }

    // handle WriteOk message timeout
    public final void handleWriteOkTimeout(Messages.WriteOkTimeout _msg) {
        electionManager.startElection();
    }

    // handle heartbeat message timeout -> coordinator crashed!
    public final void handleHeartbeatTimeout(Messages.HeartbeatTimeout _msg) {
        electionManager.startElection();
    }


    // =================================================================================
    // Public delegators preserved for external/test API
    // =================================================================================

    public int getNextNodeId() {
        return electionManager.getNextNodeId();
    }

    public int getBestId(Messages.Election _msg) {
        return electionManager.getBestId(_msg);
    }
}
