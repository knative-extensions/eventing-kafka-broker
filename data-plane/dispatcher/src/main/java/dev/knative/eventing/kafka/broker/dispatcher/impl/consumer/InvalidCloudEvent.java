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
import io.cloudevents.CloudEventData;
import io.cloudevents.SpecVersion;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Set;

/**
 * This class wraps the value of a Kafka Message that doesn't follow the Kafka protocol binding for CloudEvents.
 * <p>
 * See {@link InvalidCloudEventInterceptor} and {@link CloudEventDeserializer} for more details.
 */
public class InvalidCloudEvent implements CloudEvent {

  private final byte[] data;

  public InvalidCloudEvent(byte[] data) {
    this.data = data;
  }

  public byte[] data() {
    return this.data;
  }

  @Override
  public CloudEventData getData() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SpecVersion getSpecVersion() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getType() {
    throw new UnsupportedOperationException();
  }

  @Override
  public URI getSource() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getDataContentType() {
    throw new UnsupportedOperationException();
  }

  @Override
  public URI getDataSchema() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getSubject() {
    throw new UnsupportedOperationException();
  }

  @Override
  public OffsetDateTime getTime() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object getAttribute(String attributeName) throws IllegalArgumentException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object getExtension(String extensionName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getExtensionNames() {
    throw new UnsupportedOperationException();
  }
}
