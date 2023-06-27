package dev.knative.eventing;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import dev.knative.eventing.kafka.broker.dispatcher.ReactiveKafkaConsumer;
import io.vertx.core.Future;
import io.vertx.core.Handler;

public class MockReactiveKafkaConsumer<K, V> implements ReactiveKafkaConsumer<K, V> {

    private Consumer<K, V> consumer;

    public MockReactiveKafkaConsumer(Map<String, Object> configs) {
        consumer = new KafkaConsumer<K, V>(configs);
    }

    public MockReactiveKafkaConsumer(Consumer<K, V> consumer) {
        this.consumer = consumer;
    }

    @Override
    public Future<Map<TopicPartition, OffsetAndMetadata>> commit(Map<TopicPartition, OffsetAndMetadata> offset) {
        consumer.commitSync(offset);
        return Future.succeededFuture(offset);
    }

    @Override
    public Future<Void> close() {
        consumer.close();
        return Future.succeededFuture();
    }

    @Override
    public Future<Void> pause(Collection<TopicPartition> partitions) {
        consumer.pause(partitions);
        return Future.succeededFuture();
    }

    @Override
    public Future<ConsumerRecords<K, V>> poll(Duration timeout) {
        return Future.succeededFuture(consumer.poll(timeout));
    }

    @Override
    public Future<Void> resume(Collection<TopicPartition> partitions) {
        consumer.resume(partitions);
        return Future.succeededFuture();
    }

    @Override
    public Future<Void> subscribe(Collection<String> topics) {
        consumer.subscribe(topics);
        return Future.succeededFuture();
    }

    @Override
    public Consumer<K, V> unwrap() {
        return this.consumer;
    }

    @Override
    public ReactiveKafkaConsumer<K, V> exceptionHandler(Handler<Throwable> handler) {
        return this;
    }
    
}
