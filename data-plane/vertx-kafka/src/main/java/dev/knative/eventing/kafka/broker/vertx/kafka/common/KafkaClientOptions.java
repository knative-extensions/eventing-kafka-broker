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

/*
 * Copied from https://github.com/vert-x3/vertx-kafka-client
 *
 * Copyright 2016 Red Hat Inc.
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
package dev.knative.eventing.kafka.broker.vertx.kafka.common;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.core.json.JsonObject;
import io.vertx.core.tracing.TracingPolicy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Generic KafkaClient options.
 */
@DataObject(generateConverter = true)
public class KafkaClientOptions {
  /**
   * Default peer address to set in traces tags is null, and will automatically pick up bootstrap server from config
   */
  public static final String DEFAULT_TRACE_PEER_ADDRESS = null;

  /**
   * Default tracing policy is 'propagate'
   */
  public static final TracingPolicy DEFAULT_TRACING_POLICY = TracingPolicy.PROPAGATE;

  private Map<String, Object> config;
  private String tracePeerAddress = DEFAULT_TRACE_PEER_ADDRESS;
  private TracingPolicy tracingPolicy = DEFAULT_TRACING_POLICY;

  public KafkaClientOptions() {
  }

  /**
   * Create KafkaClientOptions from underlying Kafka config as map
   * @param config config map to be passed down to underlying Kafka client
   * @return an instance of KafkaClientOptions
   */
  public static KafkaClientOptions fromMap(Map<String, Object> config, boolean isProducer) {
    String tracePeerAddress = (String) config.getOrDefault(isProducer ? ProducerConfig.BOOTSTRAP_SERVERS_CONFIG : ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
    return new KafkaClientOptions().setTracePeerAddress(tracePeerAddress);
  }

  /**
   * Create KafkaClientOptions from underlying Kafka config as Properties
   * @param config config properties to be passed down to underlying Kafka client
   * @return an instance of KafkaClientOptions
   */
  public static KafkaClientOptions fromProperties(Properties config, boolean isProducer) {
    String tracePeerAddress = (String) config.getOrDefault(isProducer ? ProducerConfig.BOOTSTRAP_SERVERS_CONFIG : ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
    return new KafkaClientOptions().setTracePeerAddress(tracePeerAddress);
  }

  /**
   * @return the kafka config
   */
  public Map<String, Object> getConfig() {
    return config;
  }

  /**
   * Set the Kafka config.
   *
   * @param config the config
   * @return a reference to this, so the API can be used fluently
   */
  public KafkaClientOptions setConfig(Map<String, Object> config) {
    this.config = config;
    return this;
  }

  /**
   * Set a Kafka config entry.
   *
   * @param key the config key
   * @param value the config value
   * @return a reference to this, so the API can be used fluently
   */
  @GenIgnore
  public KafkaClientOptions setConfig(String key, Object value) {
    if (config == null) {
      config = new HashMap<>();
    }
    config.put(key, value);
    return this;
  }

  /**
   * @return the kafka tracing policy
   */
  public TracingPolicy getTracingPolicy() {
    return tracingPolicy;
  }

  /**
   * Set the Kafka tracing policy.
   *
   * @param tracingPolicy the tracing policy
   * @return a reference to this, so the API can be used fluently
   */
  public KafkaClientOptions setTracingPolicy(TracingPolicy tracingPolicy) {
    this.tracingPolicy = tracingPolicy;
    return this;
  }

  /**
   * @return the Kafka "peer address" to show in trace tags
   */
  public String getTracePeerAddress() {
    return tracePeerAddress;
  }

  /**
   * Set the Kafka address to show in trace tags.
   * Or leave it unset to automatically pick up bootstrap server from config instead.
   *
   * @param tracePeerAddress the Kafka "peer address" to show in trace tags
   * @return a reference to this, so the API can be used fluently
   */
  public KafkaClientOptions setTracePeerAddress(String tracePeerAddress) {
    this.tracePeerAddress = tracePeerAddress;
    return this;
  }

  public JsonObject toJson() {
    return new JsonObject();
  }
}
