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
package dev.knative.eventing.kafka.broker.receiver;

import io.cloudevents.CloudEvent;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

@FunctionalInterface
public interface RequestToRecordMapper {

  /**
   * Map the given HTTP request to a Kafka record.
   *
   * @param request http request.
   * @param topic   topic to send the event
   * @return kafka record (record can be null).
   */
  Future<KafkaProducerRecord<String, CloudEvent>> recordFromRequest(
    final HttpServerRequest request,
    final String topic
  );
}
