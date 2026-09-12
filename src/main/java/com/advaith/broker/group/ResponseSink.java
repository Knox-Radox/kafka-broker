package com.advaith.broker.group;

/**
 * A place to send one typed result whenever it's actually ready — used
 * instead of depending on {@code api.RequestContext}/wire encoding
 * directly, so this package (group coordination) stays a pure domain
 * model and knows nothing about the network/dispatch layer or the
 * protocol codec above it (PRD §4's layering). A handler wraps its own
 * {@code RequestContext::sendAsync} (after encoding {@code T} to bytes)
 * as one of these when it calls into {@link GroupCoordinator}; the
 * coordinator only ever sees "a place to put the answer, whenever it's
 * decided" — it never touches a byte of wire format itself.
 */
@FunctionalInterface
public interface ResponseSink<T> {
    void send(T result);
}
