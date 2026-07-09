package it.unitn.ds;

import java.io.Serializable;
import java.util.Objects;

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

    public static class NodeClock implements Comparable<NodeClock>{
        public int epoch;
        public int seqNum;

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

        public void incrementSeqNum() {
            this.seqNum++;
        }

        public void incrementEpoch() {
            // when changing epoch, reset the seqNum to 0
            this.epoch++;
            this.seqNum = 0;
        }
    }

    public static class UpdateRequest implements Serializable {
        public final int index;
        public final int value;
        public final boolean fromReplica;

        // keep track on who sent the message
        public final ActorRef client;

        public UpdateRequest(int _index, int _value, ActorRef _client, boolean _fromReplica) {
            index = _index;
            value = _value;
            client = _client;
            fromReplica = _fromReplica;
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

    public static class ReadResponse implements Serializable {
        public final int index;
        public final int value;

        public final int sender;

        public ReadResponse(int _index, int _value, int _sender) {
            index = _index;
            value = _value;
            sender = _sender;
        }
    }

    public static class Update implements Serializable {
        public final int index;
        public final int value;

        public final NodeClock clock;
        public final ActorRef client;

        public Update(int _index, int _value, NodeClock _clock, ActorRef _client) {
            index = _index;
            value = _value;
            clock = _clock;
            client = _client;
        }
    }

    // TODO: check if the sender id is needed to avoid duplicates
    public static class Ack implements Serializable {
        public NodeClock clock;

        public Ack(NodeClock _clock) {
            clock = _clock;
        }
    }

    public static class WriteOk implements Serializable {
        public NodeClock clock;

        public WriteOk(NodeClock _clock) {
            clock = _clock;
        }
    }

    public static class Heartbeat implements Serializable {
        // empty, just a signal to check if the coordinator is alive
    }

    public static class HeartbeatTimeout implements Serializable {
        // empty, just a signal to check when the timeout expires
    }
}
