package it.unitn.ds.ReplicaManagers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import it.unitn.ds.AbstractClient;
import it.unitn.ds.Messages;
import it.unitn.ds.Replica;

/**
 * Handles the write protocol: UpdateRequest (coordinator + replica branches),
 * Update, Ack and WriteOk messages, together with the commit logic and the
 * client notification.
 */
public class UpdateManager {
    private final Replica replica;

    // number of Acks received per clock (coordinator side)
    private HashMap<Messages.NodeClock, Integer> ackCounters = new HashMap<>();

    // clocks that reached the quorum and are ready to be committed in order
    private ArrayList<Messages.NodeClock> ackedList = new ArrayList<>();

    // this node's clients -> clocks of their pending updates
    private Map<ActorRef, Set<Messages.NodeClock>> myClients = new HashMap<>();

    // clock -> client that issued the update
    private Map<Messages.NodeClock, ActorRef> updateClients = new HashMap<>();

    // messages waiting for the Update response, indexed by request id
    private Map<String, Messages.UpdateRequest> pendingUpdateRequests = new HashMap<>();

    // clocks that received the WriteOk and are ready to be committed
    private TreeSet<Messages.NodeClock> readyToCommit = new TreeSet<>();

    // timer per clock, armed after sending Ack, cancelled on WriteOk
    private Map<Messages.NodeClock, Cancellable> writeOkTimers = new TreeMap<>();

    // timer per request id, armed after forwarding the UpdateRequest
    private Map<String, Cancellable> updateTimers = new HashMap<>();

    // GETTERS FOR TESTING PURPOSES
    public int ackCountersSize() {
        return ackCounters.size();
    }

    public int ackCountOf(Messages.NodeClock clock) {
        return ackCounters.getOrDefault(clock, -1);
    }

    public boolean ackQuorumReached(Messages.NodeClock clock) {
        return ackCountOf(clock) >= (Math.floor(replica.group.size() / 2) + 1);
    }

    public int writeOkTimersSize() {
        return writeOkTimers.size();
    }

    public int updateTimersSize() {
        return updateTimers.size();
    }

    public int pendingUpdateRequestsSize() {
        return pendingUpdateRequests.size();
    }

    public UpdateManager(Replica _replica) {
        this.replica = _replica;
    }

    // handling of the UpdateRequest message
    // - coordinator actions -> handle request + forward Update message
    // - replica actions -> forward message to coordinator + handle Update timer
    public void handleUpdateRequest(Messages.UpdateRequest _msg) throws Exception {
        if (replica.getId() == replica.coordinatorId) {
            // THIS IS THE COORDINATOR
            // - forward to other replicas UPDATE MESSAGE
            // - save client in myClients and updateClients with current clock
            replica.seqNum++;

            // define node clock -> each update the coordinator sends is identified by a
            // pair <e, i>
            Messages.NodeClock updateClock = new Messages.NodeClock(replica.epoch, replica.seqNum);

            Messages.UpdateData updateData = new Messages.UpdateData(_msg.index, _msg.value);

            updateClients.put(new Messages.NodeClock(replica.epoch, replica.seqNum), _msg.client);

            // CHECK: removed to know who the coordinator has to respond to
            if (!_msg.fromReplica) {
                myClients.computeIfAbsent(_msg.client, k -> new HashSet<>()).add(updateClock);
            } else if (myClients.containsKey(_msg.client)) {
                myClients.get(_msg.client).add(updateClock);
            }

            this.ackCounters.put(updateClock, 1);
            replica.toCommitQueue.put(updateClock, updateData);

            String requestId;
            if (_msg.id == null) {
                requestId = replica.getId() + "-" + UUID.randomUUID();
            } else {
                requestId = _msg.id;
            }

            pendingUpdateRequests.remove(requestId);

            // if is the coordinator who received the updateRequest, send an UPDATE to the
            // replicas
            replica.broadcast(
                    new Messages.Update(_msg.index, _msg.value, new Messages.NodeClock(replica.epoch, replica.seqNum),
                            _msg.client, requestId));
        } else {
            // THIS IS NOT THE COORDINATOR
            // - forward the request to the coordinator
            // - add client to the list of clients without clock

            // add client ot the list of node clients
            // myClients -> <ActorRef, Messages.NodeClock> -> NodeClock is null, will be
            // assigned by coordinator
            this.myClients.computeIfAbsent(_msg.client, k -> new HashSet<>());

            // create a unique ID for the request
            String requestId = replica.getId() + "-" + UUID.randomUUID();

            // prepare update request
            Messages.UpdateRequest forwardMsg = new Messages.UpdateRequest(_msg.index, _msg.value, _msg.client, true,
                    requestId);

            // add to pending requests
            pendingUpdateRequests.put(requestId, forwardMsg);

            // send to coordinator
            replica.group.get(replica.coordinatorId).tell(forwardMsg, replica.getSelfRef());

            // when the node sends UpdateRequest to the coordinator it starts waiting for
            // the Update message, so the updateTimer is started
            Cancellable timer = replica.createTimer(new Messages.UpdateTimeout(), replica.timerDuration);

            // create update timer, associated with the request ID
            updateTimers.put(requestId, timer);
        }
    }

    // handle Update message (nodes)
    public void handleUpdate(Messages.Update _msg) throws Exception {
        // Cancel the timer associeted with that request
        Cancellable removedTimer = updateTimers.remove(_msg.id);
        if (removedTimer != null)
            removedTimer.cancel();

        // remove pending UpdateRequest
        pendingUpdateRequests.remove(_msg.id);

        // get node clock assigned by coordinator _msg.clock
        replica.toCommitQueue.put(_msg.clock, new Messages.UpdateData(_msg.index, _msg.value));

        updateClients.put(_msg.clock, _msg.client);

        // if client is this node's client and the NodeClock associated with the message
        // is still null, must be initialized now that it has the clock value assigned
        // by the coordionator
        if (myClients.containsKey(_msg.client)) {
            myClients.get(_msg.client).add(_msg.clock);
        }

        // send ACK back to the coordinator
        replica.group.get(replica.coordinatorId).tell(new Messages.Ack(_msg.clock), replica.getSelfRef());

        // when the node sends ACK to the coordinator it starts waiting for the WriteOk
        // message, so the writeOkTimer is started
        Cancellable newTimer = replica.createTimer(new Messages.WriteOkTimeout(), replica.timerDuration);

        writeOkTimers.put(_msg.clock, newTimer);
    }

    // coordinator: count Acks and commit in order once the quorum is reached
    public void handleAck(Messages.Ack _msg) throws Exception {
        // incerment number of received ack for the _msg.NodeClock
        int currentCount = this.ackCounters.getOrDefault(_msg.clock, 0);
        currentCount++;
        this.ackCounters.put(_msg.clock, currentCount);

        // if number of ack received >= (N/2 + 1) [quorum] add the clock to the
        // ackdeList
        if (this.ackCounters.get(_msg.clock) >= (Math.floor(replica.group.size() / 2) + 1)) {
            // keep track of the acked clocks to later commit them in order
            if (!this.ackedList.contains(_msg.clock)) {
                this.ackedList.add(_msg.clock);
                this.ackedList.sort((a, b) -> a.compareTo(b));
            }
            // iterate until the smallest clock in the ackedList is not the first in the
            // toCommitQueue.
            while (!this.ackedList.isEmpty() && !replica.toCommitQueue.isEmpty()
                    && this.ackedList.get(0).equals(replica.toCommitQueue.firstKey())) {

                Messages.NodeClock clockToCommit = this.ackedList.remove(0);
                Messages.UpdateData dataToCommit = replica.toCommitQueue.remove(clockToCommit);

                if (dataToCommit != null) {
                    // persist the values on the storage
                    commitToStorage(clockToCommit, dataToCommit);

                    // send the writeOk to all the others
                    replica.broadcast(new Messages.WriteOk(clockToCommit));

                    // send the writeOk to the client if it is this node's client
                    notifyClient(clockToCommit);

                    this.ackCounters.remove(clockToCommit);
                }
            }
        }
    }

    // replica: commit in order once the WriteOk for the oldest clock is received
    public void handleWriteOk(Messages.WriteOk _msg) throws Exception {
        // received WriteOk message, cancel the WriteOk timer!
        Cancellable timer = writeOkTimers.remove(_msg.clock); // remove entry from the map
        if (timer != null)
            timer.cancel();

        // now this message is ready to be committed
        readyToCommit.add(_msg.clock);

        // commit oldest message (readyToCommit and toCommitQueue are ordered!)
        while (!readyToCommit.isEmpty() && !replica.toCommitQueue.isEmpty()
                && readyToCommit.first().equals(replica.toCommitQueue.firstKey())) {
            // clock of the message to commit
            Messages.NodeClock clockToCommit = readyToCommit.first();

            // commit and remove from toCommitQueue
            Messages.UpdateData toCommitData = replica.toCommitQueue.remove(clockToCommit);

            // persist the values on the storage
            commitToStorage(clockToCommit, toCommitData);

            // remove from readyToCommit set
            readyToCommit.remove(clockToCommit);

            // send the writeOk to the client if it is this node's client
            notifyClient(clockToCommit);
        }
    }

    // persist an update on the local storage and trigger the callback
    private void commitToStorage(Messages.NodeClock clock, Messages.UpdateData data) {
        replica.commitHistory.put(clock, data);
        replica.storage[data.index] = data.value;
        replica.onUpdateApplied(data.index, data.value);
    }

    // notify the originating client (if it is one of this node's clients)
    private void notifyClient(Messages.NodeClock clock) {
        ActorRef client = updateClients.remove(clock);
        if (client != null) {
            Set<Messages.NodeClock> pending = myClients.get(client);
            if (pending != null && pending.remove(clock)) {
                Messages.UpdateData clientData = replica.commitHistory.get(clock);
                replica.sendTo(
                        new AbstractClient.WriteResult(true, clientData.index, clientData.value, replica.getId()),
                        client);
                if (pending.isEmpty())
                    myClients.remove(client);
            }
        }
    }

    // retry all the update requests that were pending when the coordinator crashed
    public void resendPendingUpdateRequests() {
        replica.debugInfo(
                "Replica " + replica.getId() +
                        " resend pending = " + pendingUpdateRequests.size());
        for (Map.Entry<String, Messages.UpdateRequest> entry : new HashMap<>(pendingUpdateRequests).entrySet()) {
            Messages.UpdateRequest pendingUpdateRequest = entry.getValue();
            Messages.UpdateRequest retry = new Messages.UpdateRequest(
                    pendingUpdateRequest.index,
                    pendingUpdateRequest.value,
                    pendingUpdateRequest.client,
                    pendingUpdateRequest.fromReplica,
                    entry.getKey());

            try {
                handleUpdateRequest(retry);
            } catch (Exception e) {
                replica.logInfo("Error in retry pending request: " + e.getMessage());
            }
        }
    }

    // reset the commit state once synchronization is completed
    public void clearCommitState() {
        ackedList.clear();
        replica.toCommitQueue.clear();
        readyToCommit.clear();
        ackCounters.clear();
    }

    public void cancelTimers() {
        if (writeOkTimers != null) {
            for (Cancellable timer : writeOkTimers.values()) {
                timer.cancel();
            }
            writeOkTimers.clear();
        }

        if (updateTimers != null) {
            for (Cancellable timer : updateTimers.values()) {
                timer.cancel();
            }
            updateTimers.clear();
        }
    }
}
