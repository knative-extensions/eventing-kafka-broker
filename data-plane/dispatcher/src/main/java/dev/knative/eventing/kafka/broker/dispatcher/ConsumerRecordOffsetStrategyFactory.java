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

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.dispatcher.strategy.UnorderedConsumerRecordOffsetStrategy;
import io.cloudevents.CloudEvent;
import io.micrometer.core.instrument.Counter;
import io.vertx.kafka.client.consumer.KafkaConsumer;

@FunctionalInterface
public interface ConsumerRecordOffsetStrategyFactory {

  ConsumerRecordOffsetStrategy get(final KafkaConsumer<String, CloudEvent> consumer,
                                   final DataPlaneContract.Resource resource,
                                   final DataPlaneContract.Egress egress);

  static ConsumerRecordOffsetStrategyFactory unordered(final Counter eventsSentCounter) {
    return (consumer, broker, trigger) -> new UnorderedConsumerRecordOffsetStrategy(consumer, eventsSentCounter);
  }

}
