package dev.knative.eventing.kafka.broker.tests;

import dev.knative.eventing.kafka.broker.core.ReactiveConsumerFactory;
import dev.knative.eventing.kafka.broker.core.ReactiveProducerFactory;
import dev.knative.eventing.kafka.broker.dispatchervertx.VertxConsumerFactory;
import dev.knative.eventing.kafka.broker.receiverloom.LoomProducerFactory;

public class LoomDataPlaneTest extends AbstractDataPlaneTest {

    @Override
    protected ReactiveProducerFactory getReactiveKafkaProducer() {
        return new LoomProducerFactory<>();
    }

    @Override
    protected ReactiveConsumerFactory getReactiveConsumerFactory() {
        // for now, we don't have a loom consumer factory
        // so we use the vertx consumer factory instead
        // TODO: replace it with Loom Consumer Factory
        return new VertxConsumerFactory<>();
    }
}
