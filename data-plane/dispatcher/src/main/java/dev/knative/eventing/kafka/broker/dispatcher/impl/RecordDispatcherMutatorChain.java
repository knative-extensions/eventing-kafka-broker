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

import dev.knative.eventing.kafka.broker.dispatcher.CloudEventMutator;
import dev.knative.eventing.kafka.broker.dispatcher.RecordDispatcher;
import io.cloudevents.CloudEvent;
import io.vertx.core.Future;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * {@link RecordDispatcherMutatorChain} chains {@link RecordDispatcher}s and applies mutations using a provided
 * {@link CloudEventMutator} before passing the {@link KafkaConsumerRecord} to the next {@link RecordDispatcher}.
 */
public class RecordDispatcherMutatorChain implements RecordDispatcher {

  private final RecordDispatcher next;
  private final CloudEventMutator cloudEventMutator;

  public RecordDispatcherMutatorChain(final RecordDispatcher next,
                                      final CloudEventMutator cloudEventMutator) {
    this.next = next;
    this.cloudEventMutator = cloudEventMutator;
  }

  @Override
  public Future<Void> dispatch(KafkaConsumerRecord<Object, CloudEvent> kafkaRecord) {
    final var record = kafkaRecord.record();
    final var newRecord = new ConsumerRecord<>(
      record.topic(),
      record.partition(),
      record.offset(),
      record.timestamp(),
      record.timestampType(),
      record.serializedKeySize(),
      record.serializedValueSize(),
      record.key(),
      cloudEventMutator.apply(record.value()),
      record.headers(),
      record.leaderEpoch()
    );
    return next.dispatch(new KafkaConsumerRecordImpl<>(newRecord));
  }

  @Override
  public Future<Void> close() {
    return next.close();
  }

}
