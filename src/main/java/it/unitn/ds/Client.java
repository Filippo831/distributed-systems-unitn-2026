package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;

import java.util.Map;
import java.util.Optional;

public class Client extends AbstractClient {
    public record IntPair(int index, int value) {}
    private Map<Integer, Cancellable> readTimer = null;
    // use both the index of the write and the value to identify the timer for a write operation
    private Map<IntPair, Cancellable> writeTimer = null;

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
    }

    private final void handleReadResult(AbstractClient.ReadResult _msg) throws Exception {
        callbackOnReadResult(_msg);
        // if (readTimer != null && !readTimer.isCancelled()) {
        //     readTimer.cancel();
        // }
        // remove the timer for this index from the map
        if (readTimer != null && readTimer.containsKey(_msg.index)) {
            Cancellable timer = readTimer.get(_msg.index);
            if (timer != null && !timer.isCancelled()) {
                timer.cancel();
            }
            readTimer.remove(_msg.index);
        }
    }

    private final void handleWriteResult(AbstractClient.WriteResult _msg) throws Exception {
        callbackOnWriteResult(_msg);
        // if (writeTimer != null && !writeTimer.isCancelled()) {
        //     writeTimer.cancel();
        // }
        // remove the timer for this index and value from the map
        if (writeTimer != null && writeTimer.containsKey(new IntPair(_msg.index, _msg.value))) {
            Cancellable timer = writeTimer.get(new IntPair(_msg.index, _msg.value));
            if (timer != null && !timer.isCancelled()) {
                timer.cancel();
            }
            writeTimer.remove(new IntPair(_msg.index, _msg.value));
        }
    }

    @Override
    public void sendRead(ActorRef replica, int index) {
        // create a message type ReadRequest and forward it to the replica
        Messages.ReadRequest message = new Messages.ReadRequest(index, getSelf());
        replica.tell(message, getSelf());

        // if (readTimer != null && !readTimer.isCancelled()) {
        //     readTimer.cancel();
        // }
        //
        // readTimer = getContext().getSystem().scheduler().scheduleOnce(
        //         scala.concurrent.duration.Duration.create(getReadTimeoutDelay(), "milliseconds"),
        //         getSelf(),
        //         new AbstractClient.ReadTimeout(getSelf(), replica, index),
        //         getContext().getSystem().dispatcher(),
        //         getSelf()
        // );
        // add a timer for this index to the map
        if (readTimer == null) {
            readTimer = new java.util.HashMap<>();
        }

        if (readTimer.containsKey(index)) {
            Cancellable timer = readTimer.get(index);
            if (timer != null && !timer.isCancelled()) {
                timer.cancel();
            }
        }

        Cancellable timer = getContext().getSystem().scheduler().scheduleOnce(
                scala.concurrent.duration.Duration.create(getReadTimeoutDelay(), "milliseconds"),
                getSelf(),
                new AbstractClient.ReadTimeout(getSelf(), replica, index),
                getContext().getSystem().dispatcher(),
                getSelf()
        );

        readTimer.put(index, timer);
    }

    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        // create a message type UpdateRequest and forward it to the replica
        Messages.UpdateRequest message = new Messages.UpdateRequest(index, value, getSelf(), false);
        replica.tell(message, getSelf());

        // if (writeTimer != null && !writeTimer.isCancelled()) {
        //     writeTimer.cancel();
        // }
        //
        // writeTimer = getContext().getSystem().scheduler().scheduleOnce(
        //         scala.concurrent.duration.Duration.create(getWriteTimeoutDelay(), "milliseconds"),
        //         getSelf(),
        //         new AbstractClient.WriteTimeout(getSelf(), replica, index, value),
        //         getContext().getSystem().dispatcher(),
        //         getSelf()
        // );
        // add a timer for this index and value to the map
        if (writeTimer == null) {
            writeTimer = new java.util.HashMap<>();
        }

        IntPair key = new IntPair(index, value);

        if (writeTimer.containsKey(key)) {
            Cancellable timer = writeTimer.get(key);
            if (timer != null && !timer.isCancelled()) {
                timer.cancel();
            }
        }

        Cancellable timer = getContext().getSystem().scheduler().scheduleOnce(
                scala.concurrent.duration.Duration.create(getWriteTimeoutDelay(), "milliseconds"),
                getSelf(),
                new AbstractClient.WriteTimeout(getSelf(), replica, index, value),
                getContext().getSystem().dispatcher(),
                getSelf()
        );

        writeTimer.put(key, timer);
    }

    public void handleReadTimeout(AbstractClient.ReadTimeout _msg) {
        callbackOnReadTimeout(_msg);
    }

    public void handleWriteTimeout(AbstractClient.WriteTimeout _msg) {
        callbackOnWriteTimeout(_msg);
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                // TODO add your message handlers here .match(, )
                .match(AbstractClient.ReadRequest.class, this::handleReadRequest)
                .match(AbstractClient.WriteRequest.class, this::handleWriteRequest)
                .match(AbstractClient.ReadResult.class, this::handleReadResult)
                .match(AbstractClient.WriteResult.class, this::handleWriteResult)
                .match(AbstractClient.ReadTimeout.class, this::handleReadTimeout)
                .match(AbstractClient.WriteTimeout.class, this::handleWriteTimeout)
                .build();
    }

}
