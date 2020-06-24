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

import static java.lang.String.join;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.message.MessageReader;
import io.cloudevents.http.vertx.VertxMessageFactory;
import io.cloudevents.lang.Nullable;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import java.util.StringTokenizer;

public class CloudEventRequestToRecordMapper implements RequestToRecordMapper<String, CloudEvent> {

  static final int PATH_TOKEN_NUMBER = 2;
  static final String PATH_DELIMITER = "/";
  static final String TOPIC_DELIMITER = "-";
  public static final String TOPIC_PREFIX = "knative-";

  @Override
  public Future<KafkaProducerRecord<String, CloudEvent>> recordFromRequest(
      final HttpServerRequest request) {

    return VertxMessageFactory.createReader(request)
        // TODO is this conversion really necessary?
        //      can be used Message?
        .map(MessageReader::toEvent)
        .compose(event -> {
          if (event == null) {
            return Future.failedFuture(new IllegalArgumentException("event cannot be null"));
          }

          final var topic = topic(request.path());
          if (topic == null) {
            return Future.failedFuture(new IllegalArgumentException("unable to determine topic"));
          }

          // TODO set the correct producer record key
          return Future.succeededFuture(KafkaProducerRecord.create(topic, "", event));
        });
  }

  @Nullable
  static String topic(final String path) {
    // The expected request path is of the form `/<broker-namespace>/<broker-name>`, that maps to
    // topic `TOPIC_PREFIX<broker-namespace>-<broker-name>`, so validate path and return topic name.
    // In case such topic doesn't exists the following reasons apply:
    //  - The Broker doesn't exist
    //  - The Broker is not Ready

    final var tokenizer = new StringTokenizer(path, PATH_DELIMITER, false);
    if (tokenizer.countTokens() != PATH_TOKEN_NUMBER) {
      return null;
    }

    return TOPIC_PREFIX + join(TOPIC_DELIMITER, tokenizer.nextToken(), tokenizer.nextToken());
  }

}
