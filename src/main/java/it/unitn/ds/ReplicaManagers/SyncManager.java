package it.unitn.ds.ReplicaManagers;

import java.util.HashMap;
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

    public SyncManager(Replica replica) {
        this.replica = replica;
    }

    // this node was elected as the new coordinator: it requests the update
    // histories from all the other replicas to build the complete history
    public void startSynchronization(Messages.Election _msg) {
        Replica r = replica;

        // clean variables
        updateSyncResponses.clear();
        completeHistory.clear();

        r.debugInfo("Replica " + r.getId() + " elected coordinator");

        // cleanup of timers + setup new cooridnator
        r.cancelAllTimers();

        // add rertrival of complete upate history from replicas
        for (Map.Entry<Integer, ActorRef> node : r.group.entrySet()) {
            if (!r.crashedReplicas.contains(node.getKey()) && node.getKey() != r.getId()) {
                node.getValue().tell(new Messages.UpdateSyncRequest(), r.getSelfRef());
            }
        }

        // setup timer for the receiver ack
        updateSyncTimer = r.createTimer(new Messages.UpdateSyncTimeout(), r.timerDuration);
    }

    private void finishSynchronization() throws Exception {
        Replica r = replica;
        // prepare synchronization message with the id of the new coordinator and the up
        // to date message history
        Messages.Synchronization synchMsg = new Messages.Synchronization();
        synchMsg.newCoordId = r.getId();

        r.coordinatorId = r.getId();
        r.onCoordinatorElected(r.coordinatorId);

        // add to complte history the coordinator history, now it is complete
        completeHistory.putAll(r.commitHistory);
        completeHistory.putAll(r.toCommitQueue);

        // complete history contains commited and still uncommitted updates
        synchMsg.coordHistory = completeHistory;

        // the new coordinator can start new epoch an reset the sequence number
        r.epoch++;
        r.seqNum = 0;

        // got to NORMAL state
        r.actorContext().become(r.createReceive());

        // send synchronization message in broadcast to the other replicas
        for (Map.Entry<Integer, ActorRef> node : r.group.entrySet()) {
            if (!r.crashedReplicas.contains(node.getKey()) && node.getKey() != r.getId()) {
                node.getValue().tell(synchMsg, r.getSelfRef());
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
        Replica r = replica;

        // set new coordinator
        if (r.coordinatorId != _msg.newCoordId) {
            r.coordinatorId = _msg.newCoordId;
            r.onCoordinatorElected(_msg.newCoordId);
        }

        // get up to date with updates -> these still have clock in the old view
        for (Map.Entry<Messages.NodeClock, Messages.UpdateData> entry : _msg.coordHistory.entrySet()) {
            Messages.NodeClock clock = entry.getKey();
            Messages.UpdateData data = entry.getValue();

            // check history against most up to date history (from coordinator)
            if (!r.commitHistory.containsKey(clock)) {
                // if the node is missing some updates apply them
                r.storage[data.index] = data.value;
                r.commitHistory.put(clock, data);
                r.onUpdateApplied(data.index, data.value);
            }
        }

        // now that everything is commit we can clear the queues
        r.updateManager().clearCommitState();

        // from ELECTION state back to NORMAL state
        r.electionManager().exitElection();
        r.cancelAllTimers();
        r.actorContext().become(r.createReceive());

        // reset heartbeat timer
        if (r.getId() != r.coordinatorId) {
            r.heartbeatManager().resetTimeout();
        } else {
            r.heartbeatManager().startCoordinatorHeartbeat();
        }

        // these will have a clock in the new view
        r.updateManager().resendPendingUpdateRequests();
    }

    public void handleUpdateSyncRequest(Messages.UpdateSyncRequest _msg) {
        Replica r = replica;

        Map<Messages.NodeClock, Messages.UpdateData> history = new TreeMap<>(r.commitHistory);
        history.putAll(r.toCommitQueue);

        // now history contains toCommitQueue and commitHistory of the replica and can
        // send it back to the cooridnator
        r.getSenderRef().tell(new Messages.UpdateSyncResponse(r.getId(), history), r.getSelfRef());
    }

    public void handleUpdateSyncResponse(Messages.UpdateSyncResponse _msg) {
        Replica r = replica;
        // add history of the replica to the complete history
        completeHistory.putAll(_msg.updateHistory);

        updateSyncResponses.add(_msg.getId());

        if (updateSyncResponses.size() == r.group.size() - 1 - r.crashedReplicas.size()) {
            updateSyncTimer.cancel();
            try {
                finishSynchronization();
            } catch (Exception ex) {
                r.logInfo("Update sync failed (handleUpdateSyncResponse)");
            }
        }
    }

    public void handleUpdateSyncTimeout(Messages.UpdateSyncTimeout _msg) {
        Replica r = replica;

        for (Integer id : r.group.keySet()) {
            if (id != r.getId() && !r.crashedReplicas.contains(id) && !updateSyncResponses.contains(id)) {
                r.crashedReplicas.add(id);
            }
        }
        try {
            finishSynchronization();
        } catch (Exception ex) {
            r.logInfo("Update sync failed (handleUpdateSyncTimeout)");
        }
    }

    public void cancelTimers() {
        if (updateSyncTimer != null)
            updateSyncTimer.cancel();
    }
}