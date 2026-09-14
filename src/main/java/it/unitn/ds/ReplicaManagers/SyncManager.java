package it.unitn.ds.ReplicaManagers;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import it.unitn.ds.Messages;
import it.unitn.ds.Replica;

/**
 * Handles the post-election synchronization: the new coordinator gathers the
 * update histories from the surviving replicas, builds the complete history and
 * propagates it via Synchronization messages to bring everyone up to date.
 */
public class SyncManager {
    private final Replica replica;

    // complete up to date history gathered from all replicas
    private Map<Messages.NodeClock, Messages.UpdateData> completeHistory = new TreeMap<>();

    private Cancellable updateSyncTimer = null;

    // ids of the replicas that replied to the UpdateSyncRequest
    private Set<Integer> updateSyncResponses = new HashSet<>();

    public SyncManager(Replica _replica) {
        this.replica = _replica;
    }

    // this node was elected as the new coordinator: it requests the update
    // histories from all the other replicas to build the complete history
    public void startSynchronization() {
        // clean variables
        updateSyncResponses.clear();
        completeHistory.clear();

        replica.debugInfo("Replica " + replica.getId() + " elected coordinator");

        // cleanup of timers + setup new cooridnator
        replica.cancelAllTimers();

        // add rertrival of complete upate history from replicas
        for (Map.Entry<Integer, ActorRef> node : replica.group.entrySet()) {
            if (!replica.crashedReplicas.contains(node.getKey()) && node.getKey() != replica.getId()) {
                node.getValue().tell(new Messages.UpdateSyncRequest(), replica.getSelfRef());
            }
        }

        // setup timer for the receiver ack
        updateSyncTimer = replica.createTimer(new Messages.UpdateSyncTimeout(), replica.timerDuration);
    }

    private void finishSynchronization() throws Exception {
        // prepare synchronization message with the id of the new coordinator and the up
        // to date message history
        Messages.Synchronization synchMsg = new Messages.Synchronization();
        synchMsg.newCoordId = replica.getId();

        replica.coordinatorId = replica.getId();
        replica.onCoordinatorElected(replica.coordinatorId);

        // add to complte history the coordinator history, now it is complete
        completeHistory.putAll(replica.commitHistory);
        completeHistory.putAll(replica.toCommitQueue);

        // complete history contains commited and still uncommitted updates
        synchMsg.coordHistory = completeHistory;

        // the new coordinator can start new epoch an reset the sequence number
        replica.epoch++;
        replica.seqNum = 0;

        // got to NORMAL state
        replica.actorContext().become(replica.createReceive());

        // send synchronization message in broadcast to the other replicas
        for (Map.Entry<Integer, ActorRef> node : replica.group.entrySet()) {
            if (!replica.crashedReplicas.contains(node.getKey()) && node.getKey() != replica.getId()) {
                node.getValue().tell(synchMsg, replica.getSelfRef());
            }
        }

        // allow coordinator to commit what's left
        // put it here so in each coord-replica channel we have synch message before
        this.handleSynchronization(synchMsg);
    }

    // this function brings all replicas up to date with the updates (it is called
    // also by the coordinator itself to commit what was left in the toCommitQueue
    // before the election)
    public void handleSynchronization(Messages.Synchronization _msg) throws Exception {
        // set new coordinator
        if (replica.coordinatorId != _msg.newCoordId) {
            replica.coordinatorId = _msg.newCoordId;
            replica.onCoordinatorElected(_msg.newCoordId);
        }

        // get up to date with updates -> these still have clock in the old view
        for (Map.Entry<Messages.NodeClock, Messages.UpdateData> entry : _msg.coordHistory.entrySet()) {
            Messages.NodeClock clock = entry.getKey();
            Messages.UpdateData data = entry.getValue();

            // check history against most up to date history (from coordinator)
            if (!replica.commitHistory.containsKey(clock)) {
                // if the node is missing some updates apply them
                replica.storage[data.index] = data.value;
                replica.commitHistory.put(clock, data);
                replica.onUpdateApplied(data.index, data.value);
            }
        }

        // now that everything is commit we can clear the queues
        replica.updateManager().clearCommitState();

        // from ELECTION state back to NORMAL state
        replica.electionManager().exitElection();
        replica.cancelAllTimers();
        replica.actorContext().become(replica.createReceive());

        // reset heartbeat timer
        if (replica.getId() != replica.coordinatorId) {
            replica.heartbeatManager().resetTimeout();
        } else {
            replica.heartbeatManager().startCoordinatorHeartbeat();
        }

        // these will have a clock in the new view
        replica.updateManager().resendPendingUpdateRequests();
    }

    public void handleUpdateSyncRequest(Messages.UpdateSyncRequest _msg) {
        Map<Messages.NodeClock, Messages.UpdateData> history = new TreeMap<>(replica.commitHistory);
        history.putAll(replica.toCommitQueue);

        // now history contains toCommitQueue and commitHistory of the replica and can
        // send it back to the cooridnator
        replica.getSenderRef().tell(new Messages.UpdateSyncResponse(replica.getId(), history), replica.getSelfRef());
    }

    public void handleUpdateSyncResponse(Messages.UpdateSyncResponse _msg) {
        // add history of the replica to the complete history
        completeHistory.putAll(_msg.updateHistory);

        updateSyncResponses.add(_msg.getId());

        if (updateSyncResponses.size() == replica.group.size() - 1 - replica.crashedReplicas.size()) {
            updateSyncTimer.cancel();
            try {
                finishSynchronization();
            } catch (Exception ex) {
                replica.logInfo("Update sync failed (handleUpdateSyncResponse)");
            }
        }
    }

    public void handleUpdateSyncTimeout(Messages.UpdateSyncTimeout _msg) {
        for (Integer id : replica.group.keySet()) {
            if (id != replica.getId() && !replica.crashedReplicas.contains(id) && !updateSyncResponses.contains(id)) {
                replica.crashedReplicas.add(id);
            }
        }
        try {
            finishSynchronization();
        } catch (Exception ex) {
            replica.logInfo("Update sync failed (handleUpdateSyncTimeout)");
        }
    }

    public void cancelTimers() {
        if (updateSyncTimer != null)
            updateSyncTimer.cancel();
    }
}
