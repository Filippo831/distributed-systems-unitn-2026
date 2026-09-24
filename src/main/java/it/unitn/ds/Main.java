package it.unitn.ds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import it.unitn.ds.AbstractClient.ReadRequest;
import it.unitn.ds.AbstractClient.ReadResult;
import it.unitn.ds.AbstractClient.ReadTimeout;
import it.unitn.ds.AbstractClient.WriteRequest;
import it.unitn.ds.AbstractClient.WriteResult;
import it.unitn.ds.AbstractClient.WriteTimeout;
import it.unitn.ds.AbstractReplica.CoordinatorElected;
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.AbstractReplica.ElectionStarted;
import it.unitn.ds.AbstractReplica.InitSystem;
import it.unitn.ds.AbstractReplica.UpdateApplied;

/**
 * Runtime stress/demo test for the replica system.
 *
 * Runs several scenarios that mimic real-life situations (coordinator outage,
 * replica dying mid-write, congestion, cascading failures, majority loss) and
 * prints a PASS/FAIL report per check. It only uses classes from the main
 * source set; nothing here depends on the test source set.
 *
 * Run it with: ./gradlew run
 */
public class Main {

    // =====================================================================
    // Minimal runtime-test framework (all self-contained in this file)
    // =====================================================================

    private record Event(String type, Object payload, long timestamp) {
        @Override
        public String toString() {
            return type + " " + payload;
        }
    }

    private static final class Timeouts {
        final long read;
        final long write;

        Timeouts(long read, long write) {
            this.read = read;
            this.write = write;
        }
    }

    /** Collects observed events and records check results. */
    private static final class Report {
        final List<Event> events = new CopyOnWriteArrayList<>();
        int passed = 0;
        int failed = 0;

        /** Returns the number of events recorded so far (used as a "from" marker). */
        int size() {
            return events.size();
        }

        void record(String type, Object payload) {
            Event e = new Event(type, payload, System.currentTimeMillis());
            events.add(e);
            System.out.println("         [event] " + e);
        }

        /** True if an event of {@code type} matching {@code cond} was recorded at index >= from. */
        boolean has(int from, String type, Predicate<Event> cond) {
            for (int i = from; i < events.size(); i++) {
                Event e = events.get(i);
                if (e.type().equals(type) && cond.test(e)) {
                    return true;
                }
            }
            return false;
        }

        void check(String name, boolean ok) {
            if (ok) {
                passed++;
                System.out.println("    ✔ PASS  " + name);
            } else {
                failed++;
                System.out.println("    ✘ FAIL  " + name);
            }
        }

        void note(String msg) {
            System.out.println("    · note  " + msg);
        }

        void summary(String phase) {
            System.out.println("    --> [" + phase + "] " + passed + " passed, " + failed + " failed");
        }
    }

    /** Actor that receives replica callbacks and client results/timeouts. */
    private static final class Listener extends AbstractActor {
        private final Report report;

        Listener(Report report) {
            this.report = report;
        }

        static Props props(Report report) {
            return Props.create(Listener.class, () -> new Listener(report));
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(ReadResult.class, r -> report.record("READ_RESULT", r))
                    .match(WriteResult.class, r -> report.record("WRITE_RESULT", r))
                    .match(ReadTimeout.class, r -> report.record("READ_TIMEOUT", r))
                    .match(WriteTimeout.class, r -> report.record("WRITE_TIMEOUT", r))
                    .match(CoordinatorElected.class, e -> report.record("COORDINATOR_ELECTED", e))
                    .match(ElectionStarted.class, e -> report.record("ELECTION_STARTED", e))
                    .match(UpdateApplied.class, u -> report.record("UPDATE_APPLIED", u))
                    .matchAny(o -> { /* ignore everything else (e.g. Crash) */ })
                    .build();
        }
    }

    // =====================================================================
    // Helpers
    // =====================================================================

    private static ActorSystem newSystem(String name) {
        // Silence Akka's own dead-letter/info log noise; the custom Logger output
        // and the framework's [event]/PASS/FAIL lines remain visible.
        Config cfg = ConfigFactory.parseString("akka.loglevel = OFF");
        return ActorSystem.create(name, cfg);
    }

    private static boolean await(Report report, int from, long timeoutMs, String type, Predicate<Event> cond)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (report.has(from, type, cond)) {
                return true;
            }
            Thread.sleep(2);
        }
        return false;
    }

    private static Map<Integer, ActorRef> createReplicas(ActorSystem sys, Report report,
            int n, int coordinator) {
        Map<Integer, ActorRef> replicas = new HashMap<>(n);
        ActorRef listener = sys.actorOf(Listener.props(report), "replicaListener");
        for (int i = 0; i < n; i++) {
            replicas.put(i, sys.actorOf(
                    Replica.propsWithListener(i, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY,
                            AbstractReplica.COORDINATOR_BEAT_INTERVAL, listener),
                    "Replica_" + i));
        }
        InitSystem init = new InitSystem(replicas, coordinator);
        for (ActorRef r : replicas.values()) {
            r.tell(init, ActorRef.noSender());
        }
        return replicas;
    }

    private static Map<Integer, ActorRef> createReplicas(ActorSystem sys, Report report, int n) {
        return createReplicas(sys, report, n, 0);
    }

    private static ActorRef createClient(ActorSystem sys, Report report, String name,
            Timeouts to, ActorRef target) {
        ActorRef listener = sys.actorOf(Listener.props(report), name + "_listener");
        return sys.actorOf(Client.propsWithListener(to.read, to.write,
                Optional.ofNullable(target), listener), name);
    }

    private static Timeouts timeouts(int n) {
        long read = (long) AbstractReplica.MAX_LATENCY * n * 8;
        long detection = (long) (AbstractReplica.COORDINATOR_BEAT_INTERVAL * 3.0)
                + ((long) AbstractReplica.MAX_LATENCY * n * 2);
        long ringHops = (long) n * AbstractReplica.MAX_LATENCY * 2;
        long write = read + (detection + ringHops) * 5;
        return new Timeouts(read, write);
    }

    private static long electionMaxDelay(int n) {
        long detection = (long) (AbstractReplica.COORDINATOR_BEAT_INTERVAL * 3.0)
                + ((long) AbstractReplica.MAX_LATENCY * n * 2);
        long ringHops = (long) n * AbstractReplica.MAX_LATENCY * 2;
        return (detection + ringHops) * 5;
    }

    private static int maxUpdateDelay(int n) {
        return 6 * AbstractReplica.MAX_LATENCY + n + 200;
    }

    private static boolean isWriteOk(Event e, int index, int value) {
        return e.payload() instanceof WriteResult wr && wr.success && wr.index == index && wr.value == value;
    }

    private static boolean isReadValue(Event e, int value) {
        return e.payload() instanceof ReadResult rr && rr.success && rr.value == value;
    }

    private static boolean isReadServed(Event e) {
        return e.payload() instanceof ReadResult rr && rr.success;
    }

    private static boolean isApplied(Event e, int replicaId, int value) {
        return e.payload() instanceof UpdateApplied ua
                && ua.replicaId == replicaId && ua.value == value;
    }

    private static boolean isElected(Event e, int... excluded) {
        if (!(e.payload() instanceof CoordinatorElected ce)) {
            return false;
        }
        for (int x : excluded) {
            if (ce.newCoordinatorId == x) {
                return false;
            }
        }
        return true;
    }

    /** Returns the id of the most recently observed elected coordinator, or -1. */
    private static int lastElectedCoordinator(Report report) {
        int last = -1;
        for (Event e : report.events) {
            if (e.type().equals("COORDINATOR_ELECTED") && e.payload() instanceof CoordinatorElected ce) {
                last = ce.newCoordinatorId;
            }
        }
        return last;
    }

    // =====================================================================
    // Scenarios
    // =====================================================================

    /** 0 — Baseline sanity: no crash, plain write + read. */
    private static Report scenario0Baseline() throws Exception {
        final int N = 7, TARGET = N - 1;
        Report r = new Report();
        ActorSystem sys = newSystem("sc0_baseline");
        Map<Integer, ActorRef> replicas = createReplicas(sys, r, N);
        Timeouts to = timeouts(N);
        Thread.sleep(600); // let heartbeats start

        ActorRef client = createClient(sys, r, "client", to, replicas.get(TARGET));

        int m = r.size();
        client.tell(new WriteRequest(0, 100), ActorRef.noSender());
        r.check("write succeeds with no crashes",
                await(r, m, to.write, "WRITE_RESULT", e -> isWriteOk(e, 0, 100)));

        m = r.size();
        client.tell(new ReadRequest(0), ActorRef.noSender());
        r.check("read returns the written value",
                await(r, m, to.read, "READ_RESULT", e -> isReadValue(e, 100)));

        sys.terminate();
        r.summary("baseline");
        return r;
    }

    /** 1 — Coordinator dies while idle (server reboot), must auto-failover. */
    private static Report scenario1CoordinatorIdleCrash() throws Exception {
        final int N = 7, COORD = 0, TARGET = N - 1;
        Report r = new Report();
        ActorSystem sys = newSystem("sc1_coord_idle_crash");
        Map<Integer, ActorRef> replicas = createReplicas(sys, r, N, COORD);
        Timeouts to = timeouts(N);
        Thread.sleep(600);

        replicas.get(COORD).tell(new Crash(Crash.Type.Now, 0), ActorRef.noSender());
        int m = r.size();
        r.check("a new coordinator is elected after the idle coordinator crash",
                await(r, m, electionMaxDelay(N), "COORDINATOR_ELECTED", e -> isElected(e, COORD)));

        ActorRef client = createClient(sys, r, "client", to, replicas.get(TARGET));
        m = r.size();
        client.tell(new WriteRequest(0, 100), ActorRef.noSender());
        r.check("write succeeds after failover",
                await(r, m, to.write, "WRITE_RESULT", e -> isWriteOk(e, 0, 100)));

        m = r.size();
        client.tell(new ReadRequest(0), ActorRef.noSender());
        r.check("read returns the written value after failover",
                await(r, m, to.read, "READ_RESULT", e -> isReadValue(e, 100)));

        sys.terminate();
        r.summary("coordinator idle crash");
        return r;
    }

    /** 2 — The client's replica dies mid-write (forwarded, not yet committed). */
    private static Report scenario2TargetReplicaDiesMidWrite() throws Exception {
        final int N = 7, COORD = 0, TARGET = N - 1, SURVIVOR = 1;
        Report r = new Report();
        ActorSystem sys = newSystem("sc2_target_dies_mid_write");
        Map<Integer, ActorRef> replicas = createReplicas(sys, r, N, COORD);
        Timeouts to = timeouts(N);
        Thread.sleep(600);

        ActorRef client = createClient(sys, r, "client", to, replicas.get(TARGET));

        // Schedule the target's crash: it dies as soon as it processes the first
        // Update of the 2PC — i.e. right after it forwarded the write to the
        // coordinator and acked it, but before any WriteOk can reach it (the
        // Update→WriteOk channel is FIFO). It can therefore never commit the
        // write nor notify the client.
        replicas.get(TARGET).tell(new Crash(Crash.Type.Update, 1), ActorRef.noSender());

        int m = r.size();
        client.tell(new WriteRequest(0, 200), ActorRef.noSender());

        // The remaining replicas + coordinator still form a quorum → the write commits.
        r.check("write still commits even though the client's replica died mid-write",
                await(r, m, to.write, "UPDATE_APPLIED", e -> isApplied(e, COORD, 200)));

        // The client is NOT expected to be answered here by design: the replica it
        // wrote to is dead and can no longer notify it, so no WRITE_RESULT arrives.
        r.note("client is intentionally left unanswered (its replica died mid-write) — expected behaviour");

        // Survivors must still serve the committed value.
        ActorRef reader = createClient(sys, r, "reader", to, replicas.get(SURVIVOR));
        m = r.size();
        reader.tell(new ReadRequest(0), ActorRef.noSender());
        r.check("a surviving replica serves the committed value",
                await(r, m, to.read, "READ_RESULT", e -> isReadValue(e, 200)));

        sys.terminate();
        r.summary("target replica dies mid-write");
        return r;
    }

    /** 3 — Coordinator dies mid-2PC (right after the Update broadcast). */
    private static Report scenario3CoordinatorDiesMid2PC() throws Exception {
        final int N = 7, COORD = 0, TARGET = N - 1;
        Report r = new Report();
        ActorSystem sys = newSystem("sc3_coord_dies_mid_2pc");
        Map<Integer, ActorRef> replicas = createReplicas(sys, r, N, COORD);
        Timeouts to = timeouts(N);
        Thread.sleep(600);

        ActorRef client = createClient(sys, r, "client", to, replicas.get(TARGET));

        // Schedule the coordinator's crash: it dies as soon as it processes the
        // first Ack of the 2PC — the Update broadcast is done but the quorum is
        // not reached yet, i.e. exactly "mid-2PC".
        replicas.get(COORD).tell(new Crash(Crash.Type.WriteOK, 1), ActorRef.noSender());

        int m = r.size();
        client.tell(new WriteRequest(0, 300), ActorRef.noSender());

        // Failover must happen.
        m = r.size();
        r.check("a new coordinator is elected after the mid-2PC crash",
                await(r, m, electionMaxDelay(N), "COORDINATOR_ELECTED", e -> isElected(e, COORD)));

        // The in-flight write may have been lost with the dying coordinator: record it.
        long answerWindow = Math.min(to.write, (long) maxUpdateDelay(N) * 12);
        boolean inflightAnswered = await(r, m, answerWindow, "WRITE_RESULT", e -> isWriteOk(e, 0, 300));
        r.note("in-flight write answered after failover: " + inflightAnswered
                + " (false = the write was lost with the dying coordinator)");

        // The system MUST be available again: a fresh write + read must work.
        ActorRef recovery = createClient(sys, r, "recoveryClient", to, replicas.get(TARGET));
        m = r.size();
        recovery.tell(new WriteRequest(0, 333), ActorRef.noSender());
        r.check("system is available again after the crash (new write)",
                await(r, m, to.write, "WRITE_RESULT", e -> isWriteOk(e, 0, 333)));

        m = r.size();
        recovery.tell(new ReadRequest(0), ActorRef.noSender());
        r.check("read returns the last committed value",
                await(r, m, to.read, "READ_RESULT", e -> isReadValue(e, 333)));

        sys.terminate();
        r.summary("coordinator dies mid-2PC");
        return r;
    }

    /** 4 — A second replica dies while the election ring is circulating. */
    private static Report scenario4CrashDuringElection() throws Exception {
        final int N = 7, COORD = 0, DIES_DURING_ELECTION = 2, TARGET = N - 1;
        Report r = new Report();
        ActorSystem sys = newSystem("sc4_crash_during_election");
        Map<Integer, ActorRef> replicas = createReplicas(sys, r, N, COORD);
        Timeouts to = timeouts(N);
        Thread.sleep(600);

        replicas.get(COORD).tell(new Crash(Crash.Type.Now, 0), ActorRef.noSender());

        // A second replica dies the moment the ring election message reaches it:
        // the crash fires inside handleElection, right after the message is
        // processed, so it goes down while the ring is still circulating.
        replicas.get(DIES_DURING_ELECTION).tell(new Crash(Crash.Type.Election, 1), ActorRef.noSender());

        int m = r.size();
        r.check("election starts after the coordinator crash",
                await(r, m, electionMaxDelay(N), "ELECTION_STARTED", e -> true));

        // The election must still converge on a survivor.
        m = r.size();
        r.check("election still converges despite a replica dying mid-election",
                await(r, m, electionMaxDelay(N), "COORDINATOR_ELECTED",
                        e -> isElected(e, COORD, DIES_DURING_ELECTION)));

        ActorRef client = createClient(sys, r, "client", to, replicas.get(TARGET));
        m = r.size();
        client.tell(new WriteRequest(0, 400), ActorRef.noSender());
        r.check("system available after 'crash during election' (write)",
                await(r, m, to.write, "WRITE_RESULT", e -> isWriteOk(e, 0, 400)));
        m = r.size();
        client.tell(new ReadRequest(0), ActorRef.noSender());
        r.check("system available after 'crash during election' (read)",
                await(r, m, to.read, "READ_RESULT", e -> isReadValue(e, 400)));

        sys.terminate();
        r.summary("crash during election");
        return r;
    }

    /** 5 — Two consecutive coordinator deaths: data must survive both failovers. */
    private static Report scenario5DoubleFailover() throws Exception {
        final int N = 7, COORD = 0, TARGET = N - 1;
        Report r = new Report();
        ActorSystem sys = newSystem("sc5_double_failover");
        Map<Integer, ActorRef> replicas = createReplicas(sys, r, N, COORD);
        Timeouts to = timeouts(N);
        Thread.sleep(600);

        ActorRef client = createClient(sys, r, "client", to, replicas.get(TARGET));

        // normal operation
        int m = r.size();
        client.tell(new WriteRequest(0, 500), ActorRef.noSender());
        r.check("initial write commits",
                await(r, m, to.write, "WRITE_RESULT", e -> isWriteOk(e, 0, 500)));
        m = r.size();
        client.tell(new ReadRequest(0), ActorRef.noSender());
        r.check("initial read sees 500",
                await(r, m, to.read, "READ_RESULT", e -> isReadValue(e, 500)));

        // first coordinator death
        replicas.get(COORD).tell(new Crash(Crash.Type.Now, 0), ActorRef.noSender());
        m = r.size();
        await(r, m, electionMaxDelay(N), "COORDINATOR_ELECTED", e -> isElected(e, COORD));
        final int c1 = lastElectedCoordinator(r);
        r.note("first new coordinator is " + c1 + ", routing writes to it");

        if (c1 >= 0) {
            ActorRef leader1 = replicas.get(c1);
            m = r.size();
            client.tell(new WriteRequest(0, 600, leader1), ActorRef.noSender());
            r.check("write after first failover commits",
                    await(r, m, to.write, "WRITE_RESULT", e -> isWriteOk(e, 0, 600)));
            m = r.size();
            client.tell(new ReadRequest(0, leader1), ActorRef.noSender());
            r.check("read sees 600 after first failover",
                    await(r, m, to.read, "READ_RESULT", e -> isReadValue(e, 600)));

            // second coordinator death: the current (new) coordinator dies too
            r.note("now crashing coordinator " + c1 + " as well");
            replicas.get(c1).tell(new Crash(Crash.Type.Now, 0), ActorRef.noSender());
            m = r.size();
            r.check("second failover elects yet another coordinator",
                    await(r, m, electionMaxDelay(N), "COORDINATOR_ELECTED",
                            e -> isElected(e, COORD, c1)));
            final int c2 = lastElectedCoordinator(r);
            r.note("second new coordinator is " + c2 + ", reconnecting the client to it");

            if (c2 >= 0) {
                ActorRef leader2 = replicas.get(c2);

                // data must survive two consecutive coordinator deaths
                m = r.size();
                client.tell(new ReadRequest(0, leader2), ActorRef.noSender());
                r.check("value 600 survives two consecutive coordinator deaths",
                        await(r, m, to.read, "READ_RESULT", e -> isReadValue(e, 600)));

                m = r.size();
                client.tell(new WriteRequest(0, 700, leader2), ActorRef.noSender());
                r.check("write after second failover commits",
                        await(r, m, to.write, "WRITE_RESULT", e -> isWriteOk(e, 0, 700)));
                m = r.size();
                client.tell(new ReadRequest(0, leader2), ActorRef.noSender());
                r.check("read sees 700 after second failover",
                        await(r, m, to.read, "READ_RESULT", e -> isReadValue(e, 700)));
            } else {
                r.note("second election did not complete; skipping post-failover checks (failure above)");
            }
        } else {
            r.note("first election did not complete; skipping post-failover checks (failure above)");
        }

        sys.terminate();
        r.summary("double coordinator failover");
        return r;
    }

    /** 6 — Majority of replicas down at once (outage, no quorum possible). */
    private static Report scenario6MajorityLoss() throws Exception {
        final int N = 7, TARGET = N - 1;
        Report r = new Report();
        ActorSystem sys = newSystem("sc6_majority_loss");
        Map<Integer, ActorRef> replicas = createReplicas(sys, r, N, 0);
        Timeouts to = timeouts(N);
        Thread.sleep(600);

        // More than half of the system dies at once (data-center outage): 4 of 7.
        for (int i = 0; i < N / 2 + 1; i++) { // 0..3
            replicas.get(i).tell(new Crash(Crash.Type.Now, 0), ActorRef.noSender());
        }

        ActorRef client = createClient(sys, r, "client", to, replicas.get(TARGET));
        int m = r.size();
        client.tell(new WriteRequest(0, 900), ActorRef.noSender());

        // Without a quorum the write can never commit.
        Thread.sleep(maxUpdateDelay(N));
        r.check("no quorum → a write does NOT commit during the outage",
                !r.has(m, "UPDATE_APPLIED",
                        e -> e.payload() instanceof UpdateApplied ua && ua.value == 900));

        // Reads during the outage: the survivors may still serve local reads, or the
        // system may sacrifice availability (correctness over availability) — record it.
        m = r.size();
        client.tell(new ReadRequest(0), ActorRef.noSender());
        boolean served = await(r, m, to.read, "READ_RESULT", Main::isReadServed);
        r.note("read during majority outage: "
                + (served ? "served by a survivor" : "not served (system favors correctness over availability)"));

        sys.terminate();
        r.summary("majority loss");
        return r;
    }

    // =====================================================================
    // Entry point
    // =====================================================================

    public static void main(String[] args) throws Exception {
        Logger.setDestinationStdout();
        Logger.setDebugEnabled(false); // set to true to see every internal replica log

        List<Report> reports = new ArrayList<>();

        banner("SCENARIO 0 — baseline sanity check (no crash)");
        reports.add(scenario0Baseline());

        banner("SCENARIO 1 — coordinator dies while idle → automatic failover");
        reports.add(scenario1CoordinatorIdleCrash());

        banner("SCENARIO 2 — the client's replica dies in the middle of a write");
        reports.add(scenario2TargetReplicaDiesMidWrite());

        banner("SCENARIO 3 — coordinator dies mid-2PC (right after the Update broadcast)");
        reports.add(scenario3CoordinatorDiesMid2PC());

        banner("SCENARIO 4 — a second replica dies while the election is running");
        reports.add(scenario4CrashDuringElection());

        banner("SCENARIO 5 — two consecutive coordinator deaths (double failover)");
        reports.add(scenario5DoubleFailover());

        banner("SCENARIO 6 — majority of replicas down at once (outage)");
        reports.add(scenario6MajorityLoss());

        int passed = 0, failed = 0;
        for (Report report : reports) {
            passed += report.passed;
            failed += report.failed;
        }

        System.out.println("\n============================================================");
        System.out.println("FINAL RESULT: " + passed + " checks passed, " + failed + " failed");
        System.out.println(failed == 0 ? "ALL SCENARIOS PASSED ✔" : "SOME SCENARIOS FAILED ✘ (see above)");
        System.out.println("============================================================\n");
    }

    private static void banner(String title) {
        String line = "========================================";
        System.out.println("\n" + line);
        System.out.println(title);
        System.out.println(line + "\n");
    }
}