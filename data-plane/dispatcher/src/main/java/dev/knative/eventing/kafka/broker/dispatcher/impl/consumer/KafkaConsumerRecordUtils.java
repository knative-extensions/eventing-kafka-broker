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
package dev.knative.eventing.kafka.broker.dispatcher.impl.consumer;

import io.cloudevents.CloudEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public final class KafkaConsumerRecordUtils {

  private KafkaConsumerRecordUtils() {
  }

  public static <T> ConsumerRecord<T, CloudEvent> copyRecordAssigningValue(final ConsumerRecord<T, CloudEvent> record,
                                                                           final CloudEvent value) {
    return new ConsumerRecord<>(
      record.topic(),
      record.partition(),
      record.offset(),
      record.timestamp(),
      record.timestampType(),
      record.checksum(),
      record.serializedKeySize(),
      record.serializedValueSize(),
      record.key(),
      value,
      record.headers(),
      record.leaderEpoch()
    );
  }
}
