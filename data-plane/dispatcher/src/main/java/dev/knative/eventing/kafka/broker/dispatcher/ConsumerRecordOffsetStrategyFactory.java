/*
 * Copyright 2020 The Knative Authors
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

import dev.knative.eventing.kafka.broker.core.Egress;
import dev.knative.eventing.kafka.broker.core.Resource;
import dev.knative.eventing.kafka.broker.dispatcher.strategy.UnorderedConsumerRecordOffsetStrategy;
import io.vertx.kafka.client.consumer.KafkaConsumer;

@FunctionalInterface
public interface ConsumerRecordOffsetStrategyFactory<K, V> {

  ConsumerRecordOffsetStrategy<K, V> get(final KafkaConsumer<K, V> consumer, final Resource resource,
                                         final Egress egress);

  static <K, V> ConsumerRecordOffsetStrategyFactory<K, V> unordered() {
    return (consumer, broker, trigger) -> new UnorderedConsumerRecordOffsetStrategy<>(consumer);
  }

}
