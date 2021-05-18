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
package dev.knative.eventing.kafka.broker.core.testing;

import io.cloudevents.CloudEvent;
import io.cloudevents.kafka.CloudEventSerializer;

// Workaround:
//  Kafka 2.7 producer mock calls serialize(topic, ce) because
//  "just to throw ClassCastException if serializers are not the proper ones to serialize key/value"
//  https://github.com/apache/kafka/blob/3db46769baa379a0775bcd76396d24d637a55768/clients/src/main/java/org/apache/kafka/clients/producer/MockProducer.java#L306-L308
public class CloudEventSerializerMock extends CloudEventSerializer {
  @Override
  public byte[] serialize(final String topic, final CloudEvent data) {
    return null;
  }
}
