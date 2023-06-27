package dev.knative.eventing;

import java.util.Map;

import dev.knative.eventing.kafka.broker.dispatcher.ReactiveConsumerFactory;
import dev.knative.eventing.kafka.broker.dispatcher.ReactiveKafkaConsumer;
import io.vertx.core.Vertx;

public class MockReactiveConsumerFactory<K, V> implements ReactiveConsumerFactory<K, V> {

    @Override
    public ReactiveKafkaConsumer<K, V> create(Vertx vertx, Map<String, Object> configs) {
        return new MockReactiveKafkaConsumer<>(configs);
    }
    
}
