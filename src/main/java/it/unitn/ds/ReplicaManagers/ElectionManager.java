package it.unitn.ds.ReplicaManagers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.Messages;
import it.unitn.ds.Replica;

/**
 * Handles the ring-based election protocol: election messages, state switching
 * (NORMAL -> ELECTION), the election/ack timers and the ring traversal.
 */
public class ElectionManager {
    private final Replica replica;

    private Messages.Election election = null;

    // starts after forwarding election message to the next node in the ring, wait
    // for ACK message from receiver
    private Cancellable electionAckTimer = null;

    // starts after forwarding election message to the next node in the ring, wait
    // for the end of the election protocol
    private Cancellable electionTimer = null;

    // varibale to know if the replica is in election
    private boolean inElection = false;

    private int nextNodeId;

    // epoch of the election round this replica is in, the highest (newer) election epoch wins
    private int electionEpoch = -1;

    // keep track of the lowest election id initiator
    // on equal election epoch the lowest starter id wins
    private int electionStarterId = -1;

    // track outstanding election ack
    private int outstandingAckNode = -1;
    private int outstandingAckEpoch = -1;
    private int outstandingAckStarterId = -1;

    public ElectionManager(Replica _replica) {
        this.replica = _replica;
    }

    // function to get the next node in the ring (returns the ID)
    public int getNextNodeId() {
        // group hash map cannot be sorted directly -> convert group in an array list of
        // ids
        List<Integer> sortedGroupIds = new ArrayList<>(replica.group.keySet());

        // sort it
        Collections.sort(sortedGroupIds);

        // get index in the list of the current node
        int i = sortedGroupIds.indexOf(replica.getId());

        int nextId;
        do {
            // get next index (consider circularity)
            i = (i + 1) % sortedGroupIds.size();

            // get corresponding next id in the list
            nextId = sortedGroupIds.get(i);

            // if the next id is the same as this node id, it means that all other nodes are
            // crashed, so we can break the loop
            if (nextId == replica.getId()) {
                break;
            }

        } while (nextId == replica.coordinatorId || replica.crashedReplicas.contains(nextId)); // ignore coordinatorId
                                                                                               // and crashed
        // print the decision
        replica.debugInfo(
                "Replica " + replica.getId() +
                        " next node in the ring is " + nextId +
                        " (coordinator=" + replica.coordinatorId +
                        ", crashed=" + replica.crashedReplicas + ")");
        // return
        return nextId;
    }

    // get ID associated to the most recent update
    public int getBestId(Messages.Election _msg) {
        int bestId = Integer.MIN_VALUE;
        Messages.NodeClock bestClock = new Messages.NodeClock(Integer.MIN_VALUE, Integer.MIN_VALUE);

        // cycle on map entries
        for (Map.Entry<Integer, Messages.NodeClock> entry : _msg.candidates.entrySet()) {
            // check that the replica is not crashed
            if (!replica.crashedReplicas.contains(entry.getKey())) {
                // check if the entry clock is newer (compare epoch and seqNum)
                if (entry.getValue().compareTo(bestClock) > 0) {
                    bestClock = entry.getValue();
                    bestId = entry.getKey();
                }
                // if there is a tie, get highest ID
                else if (entry.getValue().compareTo(bestClock) == 0 && entry.getKey() > bestId) {
                    bestClock = entry.getValue();
                    bestId = entry.getKey();
                }
            }
        }
        return bestId;
    }

    // change state: NORMAL -> ELECTION
    public void enterElectionState() {
        if (this.inElection) {
            return;
        }

        // update election epoch here
        this.electionEpoch += 1;

        // callback
        replica.onElectionStarted(replica.coordinatorId);

        // add the crashed coordinator to the list of crashed replicas
        if (replica.coordinatorId != -1) {
            replica.crashedReplicas.add(replica.coordinatorId);
        }
        
        // ADDED: set coordinatorId at -1 as done in startElectionProtocol
        replica.coordinatorId = -1;

        // this is done by changing the node behaviour using the "message filter"
        // defined in createElectionReceive
        replica.actorContext().become(replica.createElectionReceive());
        this.inElection = true;

        // cancel all timers, not needed anymore
        replica.cancelAllTimers();

        // ADDED
        armElectionTimer();
    }

    // entry point for the timeout handlers: switch to election state and start the
    // protocol
    public void startElection() {
        enterElectionState();

        // start election protocol
        startElectionProtocol();
    }

    public void startElectionProtocol() {
        // reset election message and coordinator id
        // this.electionEpoch += 1; removed as fail-and-retry will increase it but we do
        // not want that
        this.electionStarterId = replica.getId();
        this.election = new Messages.Election(replica.getId(), this.electionEpoch, replica.epoch, new HashMap<>());
        replica.coordinatorId = -1;

        // append node id and last seen message
        this.election = addOwnCandidate(election);

        // forward message to next node in the ring
        nextNodeId = getNextNodeId();
        ActorRef nextNode = replica.group.get(nextNodeId);
        // nextNode.tell(this.election, replica.getSelfRef());
        replica.sendTo((java.io.Serializable) this.election, nextNode);

        // start timer for ack of the receiver
        armElectionAckTimer();

        // log info
        replica.logInfo("Election protocol started.");
    }

    public void handleElection(Messages.Election _msg) throws Exception {
        replica.logInfo(
                "Replica " + replica.getId() +
                        " received Election starter=" + _msg.starterId +
                        " sender=" + replica.getSenderRef() +
                        " electionEpoch=" + _msg.electionEpoch +
                        " candidates=" + _msg.candidates.keySet());

        // this allows a replica to ignore election message with lower node epoch (not election epoch)
        // basically acts as a guard against late election messages
        if (_msg.nodeEndingEpoch < replica.epoch) {
            replica.debugInfo(
                    "Replica " + replica.getId() +
                            " ignoring election message with lower node epoch " + _msg.nodeEndingEpoch +
                            " than current epoch " + replica.epoch);
            // still need to ACK sender so it knows this node is alive
            replica.sendTo((java.io.Serializable) new Messages.ElectionAck(_msg.electionEpoch, _msg.starterId), replica.getSenderRef());
            return;
        }

        // ADDED: save wheter the node was already in election or it's the first election message it receives (still in NORMAL)
        boolean wasInElection = inElection;

        // ADDED: if node still in NORMAL state, enter ELECTION state
        if (!wasInElection) {
            enterElectionState();
        }
        
        // ADDED: if the node was already in election we consider this part (filtering)
        // otherwise just consider this election message in all cases as it is the first one it sees
        if (wasInElection) {
            // this allows a replica to ignore election message with lower election epoch
            // this allows to ignore old election messages in case a newer election was started (e.g. due to electionTimer expiration)
            if (_msg.electionEpoch < this.electionEpoch) {
                replica.debugInfo(
                        "Replica " + replica.getId() +
                                " ignoring election message with lower election epoch " + _msg.electionEpoch +
                                " than current epoch " + this.electionEpoch);
                // still need to ACK sender so it knows this node is alive
                replica.sendTo((java.io.Serializable) new Messages.ElectionAck(_msg.electionEpoch, _msg.starterId), replica.getSenderRef());
                return;
            }

            // if the election epoch of the message is equal 
            // but the starter id is higher than the current one, ignore it
            else if (_msg.electionEpoch == this.electionEpoch && _msg.starterId > this.electionStarterId) {
                replica.debugInfo(
                        "Replica " + replica.getId() +
                                " ignoring election message with equal epoch " + _msg.electionEpoch +
                                " but higher starter id " + _msg.starterId +
                                " than current starter id " + this.electionStarterId);
                // still need to ACK sender so it knows this node is alive
                replica.sendTo((java.io.Serializable) new Messages.ElectionAck(_msg.electionEpoch, _msg.starterId), replica.getSenderRef());
                return;
            }
        }
        
        // election message is accepted: either first election message seen, current election or a new election with higher priority
        
        // ADDED: check if election state changed
        boolean electionChanged = _msg.electionEpoch != this.electionEpoch || _msg.starterId != this.electionStarterId;

        // save the election message content (candidates map changes every time) 
        this.election = _msg;

        // ADDED: only if the election state changed
        if (electionChanged) {
            this.electionEpoch = _msg.electionEpoch;
            this.electionStarterId = _msg.starterId;

            // ADDED: if the replica adopts a new election (electionEpoch or electionStarterId is different) then re-arm the timer 
            // - give the new "winning" election a try, without this in case of electionTimer expiration a new election is considered 
            // but the replicas still have the timer of the old election armed, leading to the cascading expiration of the older timers
            // - if the same election is looping electionChanged would be false and the timer would expire!
            if (wasInElection) {
                armElectionTimer();
            }
        }

        // ack sender
        replica.sendTo((java.io.Serializable) new Messages.ElectionAck(_msg.electionEpoch, _msg.starterId), replica.getSenderRef());

        // if the message contains my ID, that means it cycled back to me (travelled along all nodes)
        // otherwise, i never saw it and need to add myself as a candidate
        if (!_msg.candidates.containsKey(replica.getId())) {
            // add own data to the election message -> append node id and last seen message
            // (if all messages have been commited, take it from the commit history)
            this.election = addOwnCandidate(_msg);

            // get next node in the ring
            nextNodeId = getNextNodeId();

            // forward message to next node in the ring
            ActorRef nextNode = replica.group.get(nextNodeId);
            // nextNode.tell(this.election, replica.getSelfRef());
            replica.sendTo((java.io.Serializable) this.election, nextNode);

            replica.debugInfo(
                    "Replica " + replica.getId() +
                            " forwarding to " + nextNodeId);

            // setup timer for the receiver ack
            armElectionAckTimer();

        } else {
            // message cycled back, therefore it needs to check if it is the best candidate
            if (replica.getId() == getBestId(_msg)) {
                // if it is, elect it as coordinator
                replica.syncManager().startSynchronization();
            } else {
                // forward message to next node in the ring
                // addOwnCandidate(_msg); not needed, already there
                nextNodeId = getNextNodeId();
                ActorRef nextNode = replica.group.get(nextNodeId);
                // nextNode.tell(_msg, replica.getSelfRef());
                replica.sendTo((java.io.Serializable) this.election, nextNode);

                // create a timer
               armElectionAckTimer();
            }
        }

        if (replica.pendingCrash != null && replica.pendingCrash.type == Crash.Type.Election) {
            replica.n_messages_of_type++;
            if (replica.n_messages_of_type >= replica.pendingCrash.after_n_messages_of_type) {
                replica.n_messages_of_type = 0;
                replica.crashNow();
                return;
            }
        }
    }

    // append this node's id and last seen message clock to the election candidates
    private Messages.Election addOwnCandidate(Messages.Election _msg) {
        Map<Integer, Messages.NodeClock> candidates = new HashMap<>(_msg.candidates);

        if (!replica.toCommitQueue.isEmpty()) {
            candidates.put(replica.getId(), replica.toCommitQueue.lastKey()); // toCommitQueue is a tree map, so is
                                                                              // ordered by
            // NodeClock, get latest
        } else if (!replica.commitHistory.isEmpty()) {
            candidates.put(replica.getId(), replica.commitHistory.lastKey());
        } else {
            candidates.put(replica.getId(), new Messages.NodeClock(0, 0)); // if no updates have been made yet, use //
                                                                           // default
            // clock
        }

        return new Messages.Election(
                _msg.starterId,
                _msg.electionEpoch,
                _msg.nodeEndingEpoch,
                candidates);
    }

    public void handleElectionAck(Messages.ElectionAck _msg) throws Exception {
        // Only cancel if this ACK matches the current outstanding request (ignore stale ACKs)
        if (electionAckTimer != null &&
                _msg.electionEpoch == outstandingAckEpoch &&
                _msg.starterId == outstandingAckStarterId) {
            electionAckTimer.cancel();
            electionAckTimer = null;
        }
    }

    public void handleElectionTimeout(Messages.ElectionTimeout _msg) throws Exception {
        // soemthing went wrong during the election, retry
        replica.debugInfo(
                "Election timeout on replica " + replica.getId());
        this.electionEpoch += 1;

        // ADDED: re-arm election timer 
       armElectionTimer();

        // re-start election protocol
        startElectionProtocol();
    }

    public void handleElectionAckTimeout(Messages.ElectionAckTimeout _msg) throws Exception {
        replica.debugInfo(
                "Election ACK timeout on replica " + replica.getId() +
                        " waiting for ACK from " + nextNodeId);
        
        // ACK to an election message was not received, add node to crashedReplicas
        replica.crashedReplicas.add(nextNodeId);
        
        // ADDED: remove crashed node from the election (important because crashed could be the best and still be considered... message will loop until electionTimer expires)
        Map<Integer, Messages.NodeClock> candidates = new HashMap<>(election.candidates);
        candidates.remove(nextNodeId); 
        election = new Messages.Election(election.starterId, election.electionEpoch, election.nodeEndingEpoch, candidates);

        // retry now
        // forward message to next node in the ring (now skipping the crashed one)
        nextNodeId = getNextNodeId();
        ActorRef nextNode = replica.group.get(nextNodeId);
        // nextNode.tell(election, replica.getSelfRef());
        replica.sendTo((java.io.Serializable) election, nextNode);

        // setup timer for the receiver ack
        armElectionAckTimer();
    }

    // back to NORMAL state, invoked by the synchronization process
    public void exitElection() {
        this.inElection = false;
    }

    public void cancelTimers() {
        if (electionTimer != null)
            electionTimer.cancel();
        if (electionAckTimer != null)
            electionAckTimer.cancel();
        outstandingAckNode = -1;
        outstandingAckEpoch = -1;
        outstandingAckStarterId = -1;
    }

    private void armElectionAckTimer() {
        if (electionAckTimer != null) {
            electionAckTimer.cancel();
        }
        outstandingAckNode = nextNodeId;
        outstandingAckEpoch = this.electionEpoch;
        outstandingAckStarterId = this.electionStarterId;
        electionAckTimer = replica.createTimer(new Messages.ElectionAckTimeout(), replica.getElectionAckTimeoutDuration());
    }

    // ADDED armElectionTimer
    private void armElectionTimer() {
        if (electionTimer != null) electionTimer.cancel();
        electionTimer = replica.createTimer(new Messages.ElectionTimeout(), replica.getElectionTimeoutDuration());
    }

}
