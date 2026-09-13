package it.unitn.ds.ReplicaManagers;

import java.util.concurrent.TimeUnit;

import akka.actor.Cancellable;
import it.unitn.ds.Messages;
import it.unitn.ds.Replica;
import scala.concurrent.duration.Duration;

/**
 * Manages the coordinator heartbeat: the coordinator periodically broadcasts
 * Heartbeat messages; every other replica resets a timeout each time it
 * receives one and triggers the election protocol when it expires.
 */
public class HeartbeatManager {
    private final Replica replica;

    // timer for heartbeat timeout detection
    private Cancellable heartbeatTimer = null;

    public HeartbeatManager(Replica replica) {
        this.replica = replica;
    }

    // start heartbeat (x coordinator)
    public void startCoordinatorHeartbeat() {
        Replica r = replica;
        r.actorContext().getSystem().scheduler().scheduleAtFixedRate(
                Duration.create(r.getCoordinatorBeatInterval(), TimeUnit.MILLISECONDS),
                Duration.create(r.getCoordinatorBeatInterval(), TimeUnit.MILLISECONDS),
                r.getSelfRef(),
                new Messages.Heartbeat(),
                r.actorContext().dispatcher(),
                r.getSelfRef());
    }

    // reset heartbeat timer on heartbeat message reception (x node)
    public void resetTimeout() {
        Replica r = replica;
        // cancel old timer (if it exists)
        if (heartbeatTimer != null) {
            heartbeatTimer.cancel();
        }

        // staggering time applied to the heartbeat timeout to avoid all nodes to fire
        // the timeout at the same time
        int staggeringTime = (int) (r.getId() * (500.0 / r.group.size()));

        // start new one
        heartbeatTimer = r.createTimer(new Messages.HeartbeatTimeout(),
                r.getCoordinatorBeatInterval() * 2 + staggeringTime);
    }

    // handle heartbeat message (x nodes and coordinator)
    public void handleHeartbeat(Messages.Heartbeat _msg) {
        Replica r = replica;
        // coordinator is in charge of telling the nodes that it is alive
        if (r.getId() == r.coordinatorId) {
            r.broadcast(new Messages.Heartbeat());
        } else {
            // when other nodes receive a heartbeat from the coordinator they can reset the
            // timer
            resetTimeout();
        }
    }

    public void cancelTimers() {
        if (heartbeatTimer != null)
            heartbeatTimer.cancel();
    }
}