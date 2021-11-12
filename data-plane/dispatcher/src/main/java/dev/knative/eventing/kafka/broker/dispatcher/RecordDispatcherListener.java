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

import dev.knative.eventing.kafka.broker.core.AsyncCloseable;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

/**
 * This class contains hooks for listening events through the {@link dev.knative.eventing.kafka.broker.dispatcher.RecordDispatcher} lifecycle.
 */
public interface RecordDispatcherListener extends AsyncCloseable {

  /**
   * The given record has been received.
   *
   * @param record record received.
   */
  void recordReceived(KafkaConsumerRecord<?, ?> record);

  /**
   * The given record cannot be delivered to dead letter sink.
   *
   * @param record record undeliverable to dead letter sink.
   * @param ex     exception occurred.
   */
  void failedToSendToDeadLetterSink(KafkaConsumerRecord<?, ?> record, Throwable ex);

  /**
   * The given event doesn't pass the filter.
   *
   * @param record record discarded.
   */
  void recordDiscarded(KafkaConsumerRecord<?, ?> record);

  /**
   * The given record has been successfully sent to subscriber.
   *
   * @param record record sent to subscriber.
   */
  void successfullySentToSubscriber(KafkaConsumerRecord<?, ?> record);

  /**
   * The given record has been successfully sent to dead letter sink.
   *
   * @param record record sent to dead letter sink.
   */
  void successfullySentToDeadLetterSink(KafkaConsumerRecord<?, ?> record);
}
