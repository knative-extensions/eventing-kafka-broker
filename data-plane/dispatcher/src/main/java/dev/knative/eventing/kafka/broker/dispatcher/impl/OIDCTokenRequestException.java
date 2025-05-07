package dev.knative.eventing.kafka.broker.dispatcher.impl;

public class OIDCTokenRequestException extends RuntimeException {
    public OIDCTokenRequestException(String message) {
        super(message);
    }
}
