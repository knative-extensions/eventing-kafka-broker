package dev.knative.eventing.kafka.broker.tests;

import dev.knative.eventing.kafka.broker.core.ReactiveConsumerFactory;
import dev.knative.eventing.kafka.broker.core.ReactiveProducerFactory;
import dev.knative.eventing.kafka.broker.dispatchervertx.VertxConsumerFactory;
import dev.knative.eventing.kafka.broker.receiververtx.VertxProducerFactory;
import io.cloudevents.CloudEvent;

/*
 * This passes the Vertx Implementation of the ReactiveProducerFactory
 * and ReactiveConsumerFactory to the {@code DataPlaneTest}.
 */

public class DataPlaneVertxTest extends DataPlaneTest {

    @Override
    public ReactiveProducerFactory<String, CloudEvent> getProducerFactory() {
        return new VertxProducerFactory<>();
    }

    @Override
    public ReactiveConsumerFactory<String, CloudEvent> getConsumerFactory() {
        return new VertxConsumerFactory<>();
    }
}
