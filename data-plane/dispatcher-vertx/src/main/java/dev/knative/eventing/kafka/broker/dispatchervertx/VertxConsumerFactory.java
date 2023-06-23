package dev.knative.eventing.kafka.broker.dispatchervertx;

import java.util.Map;

import dev.knative.eventing.kafka.broker.dispatcher.ReactiveConsumerFactory;
import dev.knative.eventing.kafka.broker.dispatcher.ReactiveKafkaConsumer;
import io.vertx.core.Vertx;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.kafka.client.common.KafkaClientOptions;

public class VertxConsumerFactory<K, V> implements ReactiveConsumerFactory<K, V> {

    @Override
    public ReactiveKafkaConsumer<K, V> create(Vertx vertx, Map<String, Object> configs) {
        return new VertxKafkaConsumer<>(vertx, new KafkaClientOptions().setConfig(configs).setTracingPolicy(TracingPolicy.IGNORE));
    }

    
}
