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
package dev.knative.eventing.kafka.broker.dispatcher;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import dev.knative.eventing.kafka.broker.core.ReactiveKafkaConsumer;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;

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
        final Promise<ConsumerRecords<K, V>> p = Promise.promise();
        final var records = consumer.poll(timeout);
        
        // waits timeout time for new records to be available.
        ((ContextInternal) Vertx.currentContext()).setTimer(timeout.toMillis(), v -> p.complete(records));
        return p.future();
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
    public Future<Void> subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        consumer.subscribe(topics, listener);
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
