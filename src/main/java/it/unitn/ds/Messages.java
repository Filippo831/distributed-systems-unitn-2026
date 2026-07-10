package it.unitn.ds;

import java.io.Serializable;

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

    public static class NodeClock {
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

        public boolean equals(NodeClock _other) {
            return (this.epoch == _other.epoch) && (this.seqNum == _other.seqNum);
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

        // keep track on who sent the message
        public final ActorRef client;

        public UpdateRequest(int _index, int _value, ActorRef _client) {
            index = _index;
            value = _value;
            client = _client;
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

    public static class Update implements Serializable {
        public final int index;
        public final int value;

        public final NodeClock clock;

        public Update(int _index, int _value, NodeClock _clock) {
            index = _index;
            value = _value;
            clock = _clock;
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

    // Create emty classes to handle timeouts
    public static class HeartbeatTimeout {}
    public static class UpdateTimeout {}
    public static class WriteOkTimeout {}



}
