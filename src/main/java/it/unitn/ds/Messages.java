package it.unitn.ds;

import java.io.Serializable;

import akka.actor.ActorRef;

public class Messages {
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
}
