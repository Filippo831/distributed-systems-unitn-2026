package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.Optional;

public class Client extends AbstractClient {

    Client(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica,
            Optional<ActorRef> listener) {
        super(readTimeoutDelay, writeTimeoutDelay, listener, defaultTargetReplica);
    }

    public static Props props(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica) {
        return Props.create(Client.class,
                () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(long readTimeoutDelay, long writeTimeoutDelay,
            Optional<ActorRef> defaultTargetReplica, ActorRef listener) {
        return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica,
                Optional.ofNullable(listener)));
    }

    private final void handleWriteRequest(AbstractClient.WriteRequest _msg) throws Exception {
        sendWrite(_msg.replica, _msg.index, _msg.value);
    }

    private final void handleReadRequest(AbstractClient.ReadRequest _msg) throws Exception {
        sendRead(_msg.replica, _msg.index);
        // TODO: handle timeout
    }

    private final void handleReadResult(AbstractClient.ReadResult _msg) throws Exception {
        callbackOnReadResult(_msg);
    }

    private final void handleWriteResult(AbstractClient.WriteResult _msg) throws Exception {
        callbackOnWriteResult(_msg);
    }

    @Override
    public void sendRead(ActorRef replica, int index) {
        // create a message type ReadRequest and forward it to the replica
        Messages.ReadRequest message = new Messages.ReadRequest(index, getSelf());
        replica.tell(message, getSelf());
    }

    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        // create a message type UpdateRequest and forward it to the replica
        Messages.UpdateRequest message = new Messages.UpdateRequest(index, value, getSelf());
        replica.tell(message, getSelf());
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                // TODO add your message handlers here .match(, )
                .match(AbstractClient.ReadRequest.class, this::handleReadRequest)
                .match(AbstractClient.WriteRequest.class, this::handleWriteRequest)
                .match(AbstractClient.ReadResult.class, this::handleReadResult)
                .match(AbstractClient.WriteResult.class, this::handleWriteResult)
                .build();
    }

}
