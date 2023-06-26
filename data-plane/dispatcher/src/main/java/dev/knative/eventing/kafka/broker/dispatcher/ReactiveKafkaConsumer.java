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
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import io.vertx.core.Future;
import io.vertx.core.Handler;

/**
 * A reactive Kafka consumer interface for handling Kafka communication.
 *
 * @param <K> The type of the Kafka message key.
 * @param <V> The type of the Kafka message value.
 */
public interface ReactiveKafkaConsumer<K, V> {

    /**
     * Commits the specified offsets.
     *
     * @param offsets The offsets to commit.
     * @return A future containing the offsets that were committed.
     */
    Future<Map<TopicPartition, OffsetAndMetadata>> commit(Map<TopicPartition, OffsetAndMetadata> offset);

    /**
     * Closes the consumer.
     *
     * @return A future indicating the success or failure of the close operation.
     */
    Future<Void> close();

    /**
     * Pauses consumption from the specified partitions.
     *
     * @param partitions The partitions to pause consumption from.
     * @return A future indicating the success or failure of the pause operation.
     */
    Future<Void> pause(Collection<TopicPartition> partitions);

    /**
     * Polls for records from Kafka with a specified timeout.
     *
     * @param timeout The maximum time to wait for records to be available.
     * @return A future containing the records retrieved from Kafka.
     */
    Future<ConsumerRecords<K, V>> poll(Duration timeout);

    /**
     * Resumes consumption from the specified partitions.
     *
     * @param partitions The partitions to resume consumption from.
     * @return A future indicating the success or failure of the resume operation.
     */
    Future<Void> resume(Collection<TopicPartition> partitions);

    /**
     * Subscribes to the specified topics to start consuming from them.
     *
     * @param topics The topics to subscribe to.
     * @return A future indicating the success or failure of the subscribe operation.
     */
    Future<Void> subscribe(Collection<String> topics);

    /**
     * Retrieves the underlying Kafka Consumer instance.
     *
     * @return The KafkaConsumer instance.
     */
    Consumer<K, V> unwrap();

    /**
     * Sets an exception handler for handling exceptions thrown by the consumer.
     *
     * @param handler The exception handler.
     * @return This consumer instance with the exception handler set.
     */
    ReactiveKafkaConsumer<K, V> exceptionHandler(Handler<Throwable> handler);

}
