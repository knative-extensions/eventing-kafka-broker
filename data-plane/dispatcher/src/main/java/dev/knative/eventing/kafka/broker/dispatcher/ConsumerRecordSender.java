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

import io.cloudevents.CloudEvent;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

public interface ConsumerRecordSender {

  /**
   * Send the given record. (the record passed the filter)
   *
   * @param record record to send
   * @return a successful future or a failed future.
   */
  Future<HttpResponse<Buffer>> send(KafkaConsumerRecord<String, CloudEvent> record);

  /**
   * Close consumer record sender.
   *
   * @return a successful future or a failed future.
   */
  Future<?> close();

  /**
   * Create a ConsumerRecordSender from the given futures.
   *
   * @param closeFuture Future to return on close.
   * @param sendFuture  Future to return on send.
   * @return A ConsumerRecordSender that returns a failed future on send.
   */
  static ConsumerRecordSender create(Future<HttpResponse<Buffer>> sendFuture, Future<?> closeFuture) {
    return new ConsumerRecordSender() {

      private final Future<HttpResponse<Buffer>> send = sendFuture;
      private final Future<?> close = closeFuture;

      @Override
      public Future<HttpResponse<Buffer>> send(KafkaConsumerRecord<String, CloudEvent> record) {
        return this.send;
      }

      @Override
      public Future<?> close() {
        return this.close;
      }
    };
  }
}
