package dev.knative.eventing.kafka.broker.tests;

import dev.knative.eventing.kafka.broker.core.ReactiveConsumerFactory;
import dev.knative.eventing.kafka.broker.core.ReactiveProducerFactory;
import dev.knative.eventing.kafka.broker.dispatchervertx.VertxConsumerFactory;
import dev.knative.eventing.kafka.broker.receiververtx.VertxProducerFactory;

public class VertxDataPlaneTest extends AbstractDataPlaneTest {

    @Override
    protected ReactiveProducerFactory getReactiveKafkaProducer() {
        return new VertxProducerFactory<>();
    }

    @Override
    protected ReactiveConsumerFactory getReactiveConsumerFactory() {
        return new VertxConsumerFactory<>();
    }
}
