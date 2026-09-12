package com.advaith.broker.network;

/**
 * Lets something outside the network layer influence how long
 * {@code select()} blocks and get a callback once per loop iteration,
 * without NetworkServer knowing anything about what that something is
 * (PRD §7.3). Before M3, {@code select()} blocked forever — nothing above
 * the network layer ever needed the loop to wake up on its own, only in
 * response to socket I/O. Fetch long-polling breaks that: a parked Fetch
 * whose {@code max_wait_ms} elapses with no new data must still get an
 * answer, and nothing about a timer elapsing shows up as a channel
 * becoming readable/writable, so the loop has to check for it itself on a
 * bounded timeout instead of sleeping indefinitely in {@code select()}.
 */
public interface SelectorTicker {

    /** A ticker with no pending work — the pre-M3 behavior of always blocking indefinitely. */
    SelectorTicker NONE = new SelectorTicker() {
        @Override
        public long millisUntilNextDeadline() {
            return -1;
        }

        @Override
        public void tick() {
        }
    };

    /**
     * Milliseconds until the next thing this ticker needs to happen (e.g.
     * the soonest parked Fetch's timeout), or {@code -1} if there is
     * nothing pending right now and {@code select()} should block
     * indefinitely until I/O wakes it instead.
     */
    long millisUntilNextDeadline();

    /**
     * Called once per selector loop iteration, whether it woke because of
     * I/O or because its bounded {@code select()} timed out. Implementations
     * sweep for and complete anything whose deadline has now passed.
     */
    void tick();

    /**
     * Combines several tickers into one NetworkServer can hold a single
     * reference to — needed since M3, where both Fetch long-polling and
     * consumer-group heartbeat/rebalance timeouts each need the selector
     * loop to wake itself up on a schedule, for the same underlying reason
     * (see the class javadoc) but driven by two independent, unrelated
     * pieces of state.
     */
    static SelectorTicker combine(SelectorTicker... tickers) {
        return new SelectorTicker() {
            @Override
            public long millisUntilNextDeadline() {
                long soonest = -1;
                for (SelectorTicker t : tickers) {
                    long deadline = t.millisUntilNextDeadline();
                    if (deadline < 0) {
                        continue;
                    }
                    if (soonest < 0 || deadline < soonest) {
                        soonest = deadline;
                    }
                }
                return soonest;
            }

            @Override
            public void tick() {
                for (SelectorTicker t : tickers) {
                    t.tick();
                }
            }
        };
    }
}
