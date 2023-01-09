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
package dev.knative.eventing.kafka.broker.dispatcher.impl;

import io.cloudevents.CloudEvent;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

class ConsumerRecordContext {

  private KafkaConsumerRecord<Object, CloudEvent> record;
  private long receivedAtMs;

  ConsumerRecordContext(KafkaConsumerRecord<Object, CloudEvent> record) {
    this.record = record;
    this.resetTimer();
  }

  void resetTimer() {
    this.receivedAtMs = System.currentTimeMillis();
  }

  long performLatency() {
    return System.currentTimeMillis() - receivedAtMs;
  }

  KafkaConsumerRecord<Object, CloudEvent> getRecord() {
    return record;
  }

  void setRecord(final KafkaConsumerRecord<Object, CloudEvent> record) {
    this.record = record;
  }
}
