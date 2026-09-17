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

    public HeartbeatManager(Replica _replica) {
        this.replica = _replica;
    }

    // start heartbeat (x coordinator)
    public void startCoordinatorHeartbeat() {
        replica.actorContext().getSystem().scheduler().scheduleAtFixedRate(
                Duration.create(replica.getCoordinatorBeatInterval(), TimeUnit.MILLISECONDS),
                Duration.create(replica.getCoordinatorBeatInterval(), TimeUnit.MILLISECONDS),
                replica.getSelfRef(),
                new Messages.Heartbeat(),
                replica.actorContext().dispatcher(),
                replica.getSelfRef());
    }

    // reset heartbeat timer on heartbeat message reception
    public void resetTimeout() {
        // cancel old timer (if it exists)
        if (heartbeatTimer != null) {
            heartbeatTimer.cancel();
        }

        // staggering time applied to the heartbeat timeout to avoid all nodes to fire
        // the timeout at the same time
        // int staggeringTime = (int) (replica.getId() * (500.0 / replica.group.size()));
        int staggeringTime = 0;

        // start new one
        heartbeatTimer = replica.createTimer(new Messages.HeartbeatTimeout(),
                replica.getCoordinatorBeatInterval() * 2 + staggeringTime);
    }

    // handle heartbeat message
    public void handleHeartbeat(Messages.Heartbeat _msg) {
        // coordinator is in charge of telling the nodes that it is alive
        if (replica.getId() == replica.coordinatorId) {
            replica.broadcast(new Messages.Heartbeat());
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
