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
package dev.knative.eventing.kafka.broker.receiver.impl;

import dev.knative.eventing.kafka.broker.receiver.RequestToRecordMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.message.MessageReader;
import io.cloudevents.http.vertx.VertxMessageFactory;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

/**
 * This class implements a strict {@link HttpServerRequest} to {@link KafkaProducerRecord} mapper.
 * The conversion will fail if the request does not contain a valid {@link CloudEvent}.
 * <p>
 * This class is stateless, hence thread safe and shareable among verticles.
 */
public class StrictRequestToRecordMapper implements RequestToRecordMapper {

  private static class SingletonContainer {
    private static final StrictRequestToRecordMapper INSTANCE = new StrictRequestToRecordMapper();
  }

  public static RequestToRecordMapper getInstance() {
    return StrictRequestToRecordMapper.SingletonContainer.INSTANCE;
  }

  private StrictRequestToRecordMapper() {
  }

  @Override
  public Future<KafkaProducerRecord<String, CloudEvent>> requestToRecord(
    final HttpServerRequest request,
    final String topic) {

    return VertxMessageFactory.createReader(request)
      .map(MessageReader::toEvent)
      .map(event -> {
        if (event == null) {
          throw new IllegalArgumentException("event cannot be null");
        }
        return KafkaProducerRecord.create(topic, event);
      });
  }
}
