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

import dev.knative.eventing.kafka.broker.dispatcher.CloudEventSender;
import dev.knative.eventing.kafka.broker.dispatcher.ResponseHandler;
import io.cloudevents.CloudEvent;
import io.vertx.core.Future;
import org.slf4j.LoggerFactory;

/**
 * This class implements a {@link ResponseHandler} that will convert the sink response into a {@link CloudEvent} and post it to a URL.
 */
public class ResponseToHttpEndpointHandler extends BaseResponseHandler implements ResponseHandler {

  private final CloudEventSender cloudEventSender;

  public ResponseToHttpEndpointHandler(CloudEventSender cloudEventSender) {
    super(LoggerFactory.getLogger(ResponseToKafkaTopicHandler.class));
    this.cloudEventSender = cloudEventSender;
  }

  @Override
  protected Future<Void> doHandleEvent(CloudEvent event) {
    return cloudEventSender.send(event).mapEmpty();
  }

  @Override
  public Future<Void> close() {
    return this.cloudEventSender.close().mapEmpty();
  }
}
