package com.advaith.broker.api;

/**
 * Notified by ProduceHandler after every successful append (PRD §7.3 step
 * 3), so a parked Fetch on that exact partition can be completed the
 * instant qualifying data exists instead of waiting for the next timer
 * tick. FetchHandler is the only implementation — this exists as its own
 * interface (rather than ProduceHandler depending on FetchHandler
 * concretely) so the two peer handlers aren't wired to each other's full
 * surface, only to the one callback this pattern actually needs.
 */
public interface FetchCompletionListener {

    /** A no-op listener for call sites (mainly tests) that don't exercise long-polling. */
    FetchCompletionListener NONE = (topic, partitionIndex) -> { };

    void onAppended(String topic, int partitionIndex);
}
