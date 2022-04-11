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
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * CloudEventDeserializer is the deserializer used for deserializing {@link CloudEvent}.
 * <p>
 * This is an adapted form of {@link io.cloudevents.kafka.CloudEventDeserializer} to handle invalid CloudEvents, as
 * required by a {@code KafkaSource} instance.
 * <p>
 * Invalid {@link CloudEvent} are wrapped in an instance of {@link InvalidCloudEvent} to enable further processing,
 * typically done by a {@link org.apache.kafka.clients.consumer.ConsumerInterceptor}.
 */
public class CloudEventDeserializer implements Deserializer<CloudEvent> {

  private static final Logger logger = LoggerFactory.getLogger(CloudEventDeserializer.class);

  public static final String INVALID_CE_WRAPPER_ENABLED = "cloudevent.invalid.transformer.enabled";

  private final io.cloudevents.kafka.CloudEventDeserializer internalDeserializer;

  public CloudEventDeserializer() {
    internalDeserializer = new io.cloudevents.kafka.CloudEventDeserializer();
  }

  /**
   * Configure this class.
   *
   * @param configs configs in key/value pairs
   * @param isKey   whether is for key or value
   */
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    internalDeserializer.configure(configs, isKey);
  }

  @Override
  public CloudEvent deserialize(final String topic, final byte[] data) {
    logger.debug("Found invalid CloudEvent for topic {}", topic);
    return new InvalidCloudEvent(data);
  }

  /**
   * Deserialize a record value from a byte array into a CloudEvent.
   *
   * @param topic   topic associated with the data
   * @param headers headers associated with the record; may be empty.
   * @param data    serialized bytes; may be null;
   *                implementations are recommended to handle null by returning a value or null rather than throwing an exception.
   * @return deserialized typed data; may be null
   */
  @Override
  public CloudEvent deserialize(final String topic, final Headers headers, byte[] data) {
    try {
      return internalDeserializer.deserialize(topic, headers, data);
    } catch (final Throwable ignored) {
      return new InvalidCloudEvent(data);
    }
  }

  @Override
  public void close() {
    internalDeserializer.close();
  }
}
