package it.unitn.ds;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import akka.actor.ActorRef;

public class Messages {
    // TODO: probabily this isn't the best place to declare this class, see if there
    // are better options
    public static class UpdateData {
        public final int index;
        public final int value;

        public UpdateData(int _index, int _value) {
            index = _index;
            value = _value;
        }
    }

    public static class NodeClock implements Comparable<NodeClock> {
        public final int epoch;
        public final int seqNum;

        public NodeClock(int _epoch, int _seqNum) {
            epoch = _epoch;
            seqNum = _seqNum;
        }

        // check if this UpdateId is newer than the other UpdateId, first compare the
        // epoch, if they are equal compare the id
        public boolean isNewerThan(NodeClock _other) {
            if (this.epoch != _other.epoch) {
                return this.epoch > _other.epoch;
            }
            return this.seqNum > _other.seqNum;
        }

        @Override
        public int compareTo(NodeClock _other) {
            if (this.epoch != _other.epoch) {
                return Integer.compare(this.epoch, _other.epoch);
            }
            return Integer.compare(this.seqNum, _other.seqNum);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            NodeClock updateId = (NodeClock) o;
            return epoch == updateId.epoch && seqNum == updateId.seqNum;
        }

        @Override
        public int hashCode() {
            return Objects.hash(epoch, seqNum);
        }
    }

    public static class UpdateRequest implements Serializable {
        public final int index;
        public final int value;
        public final boolean fromReplica;
        public final String id;

        // keep track on who sent the message
        public final ActorRef client;

        public UpdateRequest(int _index, int _value, ActorRef _client, boolean _fromReplica, String _id) {
            index = _index;
            value = _value;
            client = _client;
            fromReplica = _fromReplica;
            id = _id;
        }

        // This is needed by clients that don't have/need the request id
        public UpdateRequest(int index, int value, ActorRef client, boolean fromReplica) {
            this(index, value, client, fromReplica, null);
        }
    }

    public static class ReadRequest implements Serializable {
        public final int index;

        // keep track on who sent the message
        public final ActorRef client;

        public ReadRequest(int _index, ActorRef _client) {
            index = _index;
            client = _client;
        }

    }

    // public static class ReadResponse implements Serializable {
    // public final int index;
    // public final int value;

    // public final int sender;

    // public ReadResponse(int _index, int _value, int _sender) {
    // index = _index;
    // value = _value;
    // sender = _sender;
    // }
    // }

    public static class Update implements Serializable {
        public final int index;
        public final int value;

        public final NodeClock clock;
        public final ActorRef client;

        public final String id;

        public Update(int _index, int _value, NodeClock _clock, ActorRef _client, String _id) {
            index = _index;
            value = _value;
            clock = _clock;
            client = _client;
            id = _id;
        }
    }

    // TODO: check if the sender id is needed to avoid duplicates
    public static class Ack implements Serializable {
        public final NodeClock clock;

        public Ack(NodeClock _clock) {
            clock = _clock;
        }
    }

    public static class WriteOk implements Serializable {
        public final NodeClock clock;

        public WriteOk(NodeClock _clock) {
            clock = _clock;
        }
    }

    public static class Heartbeat implements Serializable {
        // empty, just a signal to check if the node is alive
    }

    public static class Election implements Serializable {
        // this will contain a map of node id and node clock, where node clock
        // represents last message seen by that node
        public final Map<Integer, Messages.NodeClock> candidates;
        // id of the node that started this election
        public final int starterId;
        public final int electionEpoch;
        public final int nodeEndingEpoch;

        public Election(int _starterId, int _electionEpoch, int _nodeEndingEpoch, Map<Integer, NodeClock> candidates) {
            starterId = _starterId;
            electionEpoch = _electionEpoch;
            nodeEndingEpoch = _nodeEndingEpoch;

            this.candidates = Collections.unmodifiableMap(new HashMap<>(candidates));
            // this.candidates = new HashMap<>(candidates);
        }
    }

    public static class ElectionAck implements Serializable {
        // empty, just a ack election message sender
    }

    public static class Synchronization implements Serializable {
        // new coordinator id
        public final int newCoordId;
        // this message will contain the coordinator history used by the nodes to get up
        // to date before starting the new epoch
        public final Map<Messages.NodeClock, Messages.UpdateData> coordHistory;

        public Synchronization(int newCoordId, Map<NodeClock, UpdateData> coordHistory) {
            this.newCoordId = newCoordId;
            this.coordHistory = Collections.unmodifiableMap(new TreeMap<>(coordHistory));
            // this.coordHistory =new TreeMap<>(coordHistory);
        }
    }

    // Create empty classes to handle timeouts
    // REPLICA timeouts
    public static class HeartbeatTimeout {
    }

    public static class UpdateTimeout {
    }

    public static class WriteOkTimeout {
    }

    public static class ElectionTimeout {
    }

    public static class ElectionAckTimeout {
    }
}
