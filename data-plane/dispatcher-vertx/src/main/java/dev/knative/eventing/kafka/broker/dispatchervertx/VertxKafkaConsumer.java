/*
 * Copyright © 2018 Knative Authors (knative-dev@googlegroups.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.knative.eventing.kafka.broker.dispatchervertx;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import dev.knative.eventing.kafka.broker.dispatcher.ReactiveKafkaConsumer;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.common.KafkaClientOptions;
import io.vertx.kafka.client.consumer.KafkaConsumer;

public class VertxKafkaConsumer<K, V> implements ReactiveKafkaConsumer<K, V> {

    private KafkaConsumer<K, V> consumer;

    public VertxKafkaConsumer(Vertx v, KafkaClientOptions configs) {
        consumer = KafkaConsumer.create(v, configs);
    }

    @Override
    public Future<Map<TopicPartition, OffsetAndMetadata>> commit(Map<TopicPartition, OffsetAndMetadata> offset) {

        final var vertxOffset = offset.entrySet().stream()
            .collect(Collectors.toMap(
                entry -> new io.vertx.kafka.client.common.TopicPartition(entry.getKey().topic(),entry.getKey().partition()),
                entry -> new io.vertx.kafka.client.consumer.OffsetAndMetadata(entry.getValue().offset(),entry.getValue().metadata())
            ));

        return consumer.commit(vertxOffset).map(vertxOffset.entrySet().stream()
            .collect(Collectors.toMap(
                entry -> new TopicPartition(entry.getKey().getTopic(), entry.getKey().getPartition()),
                entry -> new OffsetAndMetadata(entry.getValue().getOffset(), entry.getValue().getMetadata())
            ))
        );
    }

    @Override
    public Future<Void> close() {
        return consumer.close();
    }

    @Override
    public Future<Void> pause(Collection<TopicPartition> partitions) {
        var vertxTopicPartitions = partitions.stream()
            .map(topicPartition -> new io.vertx.kafka.client.common.TopicPartition(topicPartition.topic(),topicPartition.partition()))
            .collect(Collectors.toSet());

        return consumer.pause(vertxTopicPartitions);
    }

    @Override
    public Future<ConsumerRecords<K, V>> poll(Duration timeout) {
        return consumer.poll(timeout).map(kafkaConsumerRecords -> kafkaConsumerRecords.records());
    }

    @Override
    public Future<Void> resume(Collection<TopicPartition> partitions) {
        var vertxTopicPartitions = partitions.stream()
            .map(topicPartition -> new io.vertx.kafka.client.common.TopicPartition(topicPartition.topic(),topicPartition.partition()))
            .collect(Collectors.toSet());

        return consumer.resume(vertxTopicPartitions);
    }

    @Override
    public Future<Void> subscribe(Collection<String> topics) {
        return consumer.subscribe(new HashSet<>(topics));
    }

    @Override
    public Future<Void> subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        Handler<Set<io.vertx.kafka.client.common.TopicPartition>> handler = partitions -> {
            Set<TopicPartition> apachePartitions = new HashSet<>();
            for (io.vertx.kafka.client.common.TopicPartition vertxPartition : partitions) {
                apachePartitions.add(new TopicPartition(vertxPartition.getTopic(), vertxPartition.getPartition()));
            }

            listener.onPartitionsRevoked(apachePartitions);
        };
        consumer = consumer.partitionsRevokedHandler(handler);

        return consumer.subscribe(new HashSet<>(topics));
    }

    @Override
    public Consumer<K, V> unwrap() {
        return consumer.unwrap();
    }

    @Override
    public ReactiveKafkaConsumer<K, V> exceptionHandler(Handler<Throwable> handler) {
        consumer = consumer.exceptionHandler(handler);
        return this;
    }

}
