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
package dev.knative.eventing.kafka.broker.receiver;

import io.vertx.core.Future;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Interface for a reactive Kafka producer consist with all the methods needed.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public interface ReactiveKafkaProducer<K, V> {

    /**
     * Send a record to a Kafka topic
     *
     * @param record the record to send
     * @return a future completed with the record metadata or with a failure if the
     *         record cannot be sent
     */
    Future<RecordMetadata> send(ProducerRecord<K, V> record);

    /**
     * Close the producer
     *
     * @return a future notifying when the producer is closed
     */
    Future<Void> close();

    /**
     * Flush the producer
     * <p>
     * This method ensures all records stored in the producer buffer are actually
     * sent to the Kafka cluster.
     *
     * @return a future notifying when the producer is flushed
     */
    Future<Void> flush();

    /**
     * Wnrap the underlying Kafka producer
     * <p>
     * Use this if you want to access the Kafka producer directly.
     *
     * @return the instance underlying KafkaProducer
     */
    Producer<K, V> unwrap();
}
