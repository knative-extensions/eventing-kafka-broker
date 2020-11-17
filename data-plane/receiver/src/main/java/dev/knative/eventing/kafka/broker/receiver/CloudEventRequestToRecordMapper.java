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

package dev.knative.eventing.kafka.broker.receiver;

import static net.logstash.logback.argument.StructuredArguments.keyValue;

import dev.knative.eventing.kafka.broker.core.cloudevents.PartitionKey;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.message.MessageReader;
import io.cloudevents.http.vertx.VertxMessageFactory;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloudEventRequestToRecordMapper implements RequestToRecordMapper<String, CloudEvent> {

  private static final Logger logger = LoggerFactory.getLogger(CloudEventRequestToRecordMapper.class);

  @Override
  public Future<KafkaProducerRecord<String, CloudEvent>> recordFromRequest(
    final HttpServerRequest request,
    final String topic) {

    return VertxMessageFactory.createReader(request)
      // TODO is this conversion really necessary?
      //      can be used Message?
      .map(MessageReader::toEvent)
      .map(event -> {
        if (event == null) {
          throw new IllegalArgumentException("event cannot be null");
        }
        logger.debug("received event {}", keyValue("event", event));

        return KafkaProducerRecord.create(topic, PartitionKey.extract(event), event);
      });
  }
}
