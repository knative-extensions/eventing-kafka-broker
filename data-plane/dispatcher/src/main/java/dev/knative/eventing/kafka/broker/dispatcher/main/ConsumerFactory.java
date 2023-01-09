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
package dev.knative.eventing.kafka.broker.dispatcher.main;

import io.vertx.core.Vertx;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.kafka.client.common.KafkaClientOptions;
import io.vertx.kafka.client.consumer.KafkaConsumer;

import java.util.Map;

@FunctionalInterface
public interface ConsumerFactory<K, V> {

  KafkaConsumer<K, V> create(final Vertx vertx, final Map<String, Object> consumerConfigs);

  static <K, V> ConsumerFactory<K, V> defaultFactory() {
    return new ConsumerFactory<>() {

      private KafkaConsumer<K, V> consumer;

      @Override
      public KafkaConsumer<K, V> create(Vertx vertx, Map<String, Object> consumerConfigs) {
        if (consumer == null) {
          consumer = KafkaConsumer.create(vertx, new KafkaClientOptions().setConfig(consumerConfigs)
            // Disable tracing provided by vertx-kafka-client, because it doesn't work well with our dispatch logic.
            // RecordDispatcher, when receiving a new record, takes care of adding the proper receive record span.
            .setTracingPolicy(TracingPolicy.IGNORE));
        }
        return consumer;
      }
    };
  }
}


