package it.unitn.ds.ReplicaManagers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import it.unitn.ds.Messages;
import it.unitn.ds.Replica;

/**
 * Handles the ring-based election protocol: election messages, state switching
 * (NORMAL -> ELECTION), the election/ack timers and the ring traversal.
 */
public class ElectionManager {
    private final Replica replica;

    // election message, will contain coordinator candidates
    private Messages.Election election = new Messages.Election();

    // starts after forwarding election message to the next node in the ring, wait
    // for ACK message from receiver
    private Cancellable electionAckTimer = null;

    // starts after forwarding election message to the next node in the ring, wait
    // for the end of the election protocol
    private Cancellable electionTimer = null;

    // varibale to know if the replica is in election
    private boolean inElection = false;

    private int nextNodeId;

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

        } while (nextId == replica.coordinatorId || replica.crashedReplicas.contains(nextId)); // ignore coordinatorId and crashed
                                                                                    // replicas

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

        // callback
        replica.onElectionStarted(replica.coordinatorId);

        // add the crashed coordinator to the list of crashed replicas
        if (replica.coordinatorId != -1) {
            replica.crashedReplicas.add(replica.coordinatorId);
        }

        // this is done by changing the node behaviour using the "message filter"
        // defined in createElectionReceive
        replica.actorContext().become(replica.createElectionReceive());
        this.inElection = true;

        // cancel all timers, not needed anymore
        replica.cancelAllTimers();

        electionTimer = replica.createTimer(new Messages.ElectionTimeout(), replica.timerDuration * replica.group.size());
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
        election = new Messages.Election();
        replica.coordinatorId = -1;
        election.starterId = replica.getId();

        // append node id and last seen message
        addOwnCandidate(election);

        // forward message to next node in the ring
        nextNodeId = getNextNodeId();
        ActorRef nextNode = replica.group.get(nextNodeId);
        nextNode.tell(election, replica.getSelfRef());

        // start timer for ack of the receiver
        electionAckTimer = replica.createTimer(new Messages.ElectionAckTimeout(), replica.timerDuration);

        // log info
        replica.logInfo("Election protocol started.");
    }

    public void handleElection(Messages.Election _msg) throws Exception {
        replica.logInfo(
                "Replica " + replica.getId() +
                        " received Election starter=" + _msg.starterId +
                        " sender=" + replica.getSenderRef() +
                        " candidates=" + _msg.candidates.keySet());
        // save election message
        this.election = _msg;

        // if node still in NORMAL state, enter ELECTION state and handle election
        // message
        if (!inElection) {
            // go to election state
            enterElectionState();

            // ack sender
            replica.getSenderRef().tell(new Messages.ElectionAck(), replica.getSelfRef());

            // add own data to the election message -> append node id and last seen message
            // (if all messages have been commited, take it from the commit history)
            addOwnCandidate(_msg);

            // forward message to next node in the ring
            nextNodeId = getNextNodeId();
            ActorRef nextNode = replica.group.get(nextNodeId);
            nextNode.tell(_msg, replica.getSelfRef());

            replica.debugInfo(
                    "Replica " + replica.getId() +
                            " forwarding to " + nextNodeId);

            // setup timer for the receiver ack
            electionAckTimer = replica.createTimer(new Messages.ElectionAckTimeout(), replica.timerDuration);

        } else {
            replica.getSenderRef().tell(new Messages.ElectionAck(), replica.getSelfRef());
            // if the node was already in election, it means the message cycled back to it,
            // therefore it needs to check if it is the best candidate
            if (replica.getId() == getBestId(_msg)) {
                // if it is, elect it as coordinator
                replica.syncManager().startSynchronization();
            } else {
                // forward message to next node in the ring
                nextNodeId = getNextNodeId();
                ActorRef nextNode = replica.group.get(nextNodeId);
                nextNode.tell(_msg, replica.getSelfRef());
            }
        }
    }

    // append this node's id and last seen message clock to the election candidates
    private void addOwnCandidate(Messages.Election _msg) {
        if (!replica.toCommitQueue.isEmpty()) {
            _msg.candidates.put(replica.getId(), replica.toCommitQueue.lastKey()); // toCommitQueue is a tree map, so is ordered by
                                                                    // NodeClock, get latest
        } else if (!replica.commitHistory.isEmpty()) {
            _msg.candidates.put(replica.getId(), replica.commitHistory.lastKey());
        } else {
            _msg.candidates.put(replica.getId(), new Messages.NodeClock(0, 0)); // if no updates have been made yet, use default
                                                                       // clock
        }
    }

    public void handleElectionAck(Messages.ElectionAck _msg) throws Exception {
        // sender of election message can cancel the timer now that it received the ACK
        if (electionAckTimer != null)
            electionAckTimer.cancel();
    }

    public void handleElectionTimeout(Messages.ElectionTimeout _msg) throws Exception {
        // soemthing went wrong during the election, retry
        replica.debugInfo(
                "Election timeout on replica " + replica.getId());
        startElectionProtocol();
    }

    public void handleElectionAckTimeout(Messages.ElectionAckTimeout _msg) throws Exception {
        // ACK to an election message was not received, add node to crashedReplicas
        replica.crashedReplicas.add(nextNodeId);

        // retry now
        // forward message to next node in the ring (now skipping the crashed one)
        nextNodeId = getNextNodeId();
        ActorRef nextNode = replica.group.get(nextNodeId);
        nextNode.tell(election, replica.getSelfRef());

        // setup timer for the receiver ack
        electionAckTimer = replica.createTimer(new Messages.ElectionAckTimeout(), replica.timerDuration);
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
    }
}
