package dev.knative.eventing.kafka.broker.dispatcher.impl.consumer;

import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.SpecVersion;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Set;

/**
 * This class represents a skipped offset. (e.g. transactional control events, or events skipped due to aborted transactions
 * with read_committed)
 * <p>
 */
public class OffsetSkippingCloudEvent implements CloudEvent {
    public OffsetSkippingCloudEvent() {
    }

    @Override
    public CloudEventData getData() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SpecVersion getSpecVersion() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public URI getSource() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getDataContentType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public URI getDataSchema() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSubject() {
        throw new UnsupportedOperationException();
    }

    @Override
    public OffsetDateTime getTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getAttribute(String attributeName) throws IllegalArgumentException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getExtension(String extensionName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> getExtensionNames() {
        throw new UnsupportedOperationException();
    }
}
