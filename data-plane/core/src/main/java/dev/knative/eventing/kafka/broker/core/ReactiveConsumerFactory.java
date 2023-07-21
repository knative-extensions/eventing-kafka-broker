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
package dev.knative.eventing.kafka.broker.core;

import io.vertx.core.Vertx;
import java.util.Map;

/**
 * A factory interface for creating reactive Kafka consumers.
 *
 * @param <K> The type of the Kafka message key.
 * @param <V> The type of the Kafka message value.
 */
@FunctionalInterface
public interface ReactiveConsumerFactory<K, V> {

    /**
     * Creates a new reactive Kafka consumer using the provided Vertx instance and configuration.
     *
     * @param vertx   The Vertx instance to be used by the vertx consumer only.
     * @param configs The configuration options for the consumer.
     * @return The created reactive Kafka consumer.
     */
    ReactiveKafkaConsumer<K, V> create(Vertx vertx, Map<String, Object> configs);
}
