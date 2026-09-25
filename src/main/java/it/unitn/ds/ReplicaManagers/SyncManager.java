package it.unitn.ds.ReplicaManagers;

import java.util.Map;
import java.util.TreeMap;

import akka.actor.ActorRef;
import it.unitn.ds.Messages;
import it.unitn.ds.Replica;

/**
 * Handles the post-election synchronization: the new coordinator brings the other replicas 
 * up-to-date with its own update history by propagating it via
 * Synchronization messages.
 */
public class SyncManager {
    private final Replica replica;


    public SyncManager(Replica _replica) {
        this.replica = _replica;
    }

    // this node was elected as the new coordinator: it now will bring all replicas up-to-date
    public void startSynchronization() {
        // clean variables
        //completeHistory.clear();

        replica.debugInfo("Replica " + replica.getId() + " elected coordinator starts synchronization phase");

        // cleanup of timers
        replica.cancelAllTimers();

        try{
            // elect as coordinator
            replica.coordinatorId = replica.getId();

            // callback on election
            replica.onCoordinatorElected(replica.coordinatorId);

            // build complete history from commited and to commit updates of the new coordinator (most up-to-date replica)
            Map<Messages.NodeClock, Messages.UpdateData> completeHistory = new TreeMap<>(replica.commitHistory);
            completeHistory.putAll(replica.toCommitQueue);

            // build synchronization message with the id of the new coordinator and the complete update history
            Messages.Synchronization synchMsg = new Messages.Synchronization(replica.getId(), completeHistory);

            // the new coordinator can start new epoch an reset the sequence number
            replica.epoch++;
            replica.seqNum = 0;

            // return to NORMAL state
            replica.actorContext().become(replica.createReceive());

            // send synchronization message in broadcast to the other replicas to bring them up-to-date
            for (Map.Entry<Integer, ActorRef> node : replica.group.entrySet()) {
                if (!replica.crashedReplicas.contains(node.getKey()) && node.getKey() != replica.getId()) {
                    replica.sendTo((java.io.Serializable) synchMsg, node.getValue());
                }
            }

            // allow coordinator to commit what's left
            // put it here, after broadcast of the synch message, so in each coord-replica channel we have synch message before every other message
            this.handleSynchronization(synchMsg);

        } catch (Exception ex) {
            System.getLogger(SyncManager.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
        }
    }

    // this function brings all replicas up to date with the updates 
    // it is called also by the coordinator itself to commit what was left in the toCommitQueue before the election
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

        // now that everything is committed we can clear the queues
        replica.updateManager().clearCommitState();

        // from ELECTION state back to NORMAL state and cancel timers
        replica.electionManager().exitElection(); // inElection = false
        replica.cancelAllTimers();
        replica.actorContext().become(replica.createReceive());

        // reset heartbeat timer management
        if (replica.getId() != replica.coordinatorId) {
            replica.heartbeatManager().resetTimeout();
        } else {
            replica.heartbeatManager().startCoordinatorHeartbeat();
        }

        // these will have a clock in the new view
        replica.updateManager().resendPendingUpdateRequests();
    }

}
