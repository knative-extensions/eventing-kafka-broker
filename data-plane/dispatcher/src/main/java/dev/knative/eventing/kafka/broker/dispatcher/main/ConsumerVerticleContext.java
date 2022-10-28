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
package dev.knative.eventing.kafka.broker.dispatcher.main;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.metrics.Metrics;
import dev.knative.eventing.kafka.broker.core.security.AuthProvider;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.InvalidCloudEventInterceptor;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.KeyDeserializer;
import dev.knative.eventing.kafka.broker.dispatcher.impl.http.WebClientCloudEventSender;
import io.cloudevents.CloudEvent;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.vertx.ext.web.client.WebClientOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

public class ConsumerVerticleContext {

  public final static Logger logger = LoggerFactory.getLogger(ConsumerVerticleContext.class);

  private DataPlaneContract.Resource resource;

  private DataPlaneContract.Egress egress;
  private DataPlaneContract.EgressConfig egressConfig;

  private AuthProvider authProvider;
  private MeterRegistry metricsRegistry;

  private Map<String, Object> consumerConfigs;
  private Map<String, Object> producerConfigs;
  private WebClientOptions webClientOptions;

  private ConsumerFactory<Object, CloudEvent> consumerFactory;
  private ProducerFactory<String, CloudEvent> producerFactory;

  private Integer maxPollRecords;
  private static final int DEFAULT_MAX_POLL_RECORDS = 50;

  private ConsumerVerticleLoggingContext loggingContext;

  private Tags tags;

  public ConsumerVerticleContext withConsumerConfigs(final Map<String, Object> consumerConfigs) {
    this.consumerConfigs = new HashMap<>(consumerConfigs);
    return this;
  }

  public ConsumerVerticleContext withProducerConfigs(final Map<String, Object> producerConfigs) {
    this.producerConfigs = new HashMap<>(producerConfigs);
    return this;
  }

  public ConsumerVerticleContext withResource(final DataPlaneContract.Resource resource, final DataPlaneContract.Egress egress) {
    Objects.requireNonNull(resource);
    Objects.requireNonNull(consumerConfigs);
    Objects.requireNonNull(producerConfigs);

    // Copy resource and remove egresses to avoid keeping references to all egresses.
    this.resource = DataPlaneContract.Resource.newBuilder(resource).clearEgresses().build();
    withEgress(egress);

    consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, resource.getBootstrapServers());
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, resource.getBootstrapServers());

    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, egress.getConsumerGroup());
    consumerConfigs.put(KeyDeserializer.KEY_TYPE, egress.getKeyType());
    if (isResourceReferenceDefined(resource.getReference())) {
      // Set the resource reference so that when the interceptor gets a record that is not a CloudEvent, it can set
      // CloudEvents context attributes accordingly (see InvalidCloudEventInterceptor for more information).
      consumerConfigs.put(InvalidCloudEventInterceptor.SOURCE_NAME_CONFIG, resource.getReference().getName());
      consumerConfigs.put(InvalidCloudEventInterceptor.SOURCE_NAMESPACE_CONFIG, resource.getReference().getNamespace());
    }

    this.tags = Tags.of(
      // Resource tags
      Tag.of(Metrics.Tags.RESOURCE_NAME, resource.getReference().getName()),
      Tag.of(Metrics.Tags.RESOURCE_NAMESPACE, resource.getReference().getNamespace()),
      // Egress tags
      Tag.of(Metrics.Tags.CONSUMER_NAME, egress.getReference().getName())
    );

    return this;
  }

  private void withEgress(final DataPlaneContract.Egress egress) {
    Objects.requireNonNull(egress);
    Objects.requireNonNull(resource);
    Objects.requireNonNull(consumerConfigs);

    this.egress = egress;

    if (egress.hasEgressConfig()) {
      this.egressConfig = egress.getEgressConfig();
    } else {
      this.egressConfig = resource.getEgressConfig();
    }

    final var maxProcessingTime = maxProcessingTimeMs(consumerConfigs, egressConfig);
    consumerConfigs.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxProcessingTime);
  }

  public ConsumerVerticleContext withAuthProvider(final AuthProvider authProvider) {
    this.authProvider = authProvider;
    return this;
  }

  public ConsumerVerticleContext withMeterRegistry(MeterRegistry metricsRegistry) {
    this.metricsRegistry = metricsRegistry;
    return this;
  }

  public ConsumerVerticleContext withWebClientOptions(final WebClientOptions webClientOptions) {
    this.webClientOptions = new WebClientOptions(webClientOptions);
    return this;
  }

  public ConsumerVerticleContext withConsumerFactory(final ConsumerFactory<Object, CloudEvent> consumerFactory) {
    this.consumerFactory = consumerFactory;
    return this;
  }

  public ConsumerVerticleContext withProducerFactory(final ProducerFactory<String, CloudEvent> producerFactory) {
    this.producerFactory = producerFactory;
    return this;
  }

  public DataPlaneContract.Resource getResource() {
    return resource;
  }

  public DataPlaneContract.Egress getEgress() {
    return egress;
  }

  public int getMaxPollRecords() {
    if (this.maxPollRecords == null) {
      final var mpr = getConsumerConfigs().get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
      if (mpr == null) {
        this.maxPollRecords = DEFAULT_MAX_POLL_RECORDS;
      } else {
        this.maxPollRecords = Integer.parseInt(mpr.toString());
      }
    }
    return this.maxPollRecords;
  }

  public DataPlaneContract.EgressConfig getEgressConfig() {
    return egressConfig;
  }

  public AuthProvider getAuthProvider() {
    return authProvider;
  }

  public MeterRegistry getMetricsRegistry() {
    return metricsRegistry;
  }

  public Map<String, Object> getConsumerConfigs() {
    return consumerConfigs;
  }

  public Map<String, Object> getProducerConfigs() {
    return producerConfigs;
  }

  public WebClientOptions getWebClientOptions() {
    return webClientOptions;
  }

  private synchronized ConsumerVerticleLoggingContext getLoggingContext() {
    if (loggingContext == null) {
      loggingContext = new ConsumerVerticleLoggingContext(this);
    }
    return loggingContext;
  }

  public Object getLoggingKeyValue() {
    return keyValue("context", getLoggingContext());
  }

  public ConsumerFactory<Object, CloudEvent> getConsumerFactory() {
    return this.consumerFactory;
  }

  public ProducerFactory<String, CloudEvent> getProducerFactory() {
    return this.producerFactory;
  }

  public Tags getTags() {
    return tags;
  }

  private static int maxProcessingTimeMs(final Map<String, Object> consumerConfigs, final DataPlaneContract.EgressConfig egressConfig) {
    final var mpr = consumerConfigs.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
    final long maxPollRecords = mpr == null ? /* default max.poll.records */ 500L : Long.parseLong(mpr.toString());
    final var retryPolicy = WebClientCloudEventSender.computeRetryPolicy(egressConfig);
    final var retry = egressConfig.getRetry();
    final var timeout = egressConfig.getTimeout() > 0 ? egressConfig.getTimeout() : WebClientCloudEventSender.DEFAULT_TIMEOUT_MS;

    long maxProcessingTimeForSingleRecord = timeout;
    for (int i = 1; i <= retry; i++) {
      maxProcessingTimeForSingleRecord += timeout + retryPolicy.apply(i);
    }
    // In addition, we add some seconds as overhead for each retry.
    final long overhead = 1000L * retry;
    maxProcessingTimeForSingleRecord += overhead;
    // So far, the max processing time calculated is for one single record,
    // however, we poll records in batches based on the `max.poll.records`
    // configuration, so the total max processing time is:
    //
    // Times 2 for dead letter sink retries.
    final var total = 2 * maxPollRecords * maxProcessingTimeForSingleRecord;

    if (total >= Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return (int) total;
  }

  private static boolean isResourceReferenceDefined(DataPlaneContract.Reference resource) {
    return resource != null && !resource.getNamespace().isBlank() && !resource.getName().isBlank();
  }

  @Override
  public String toString() {
    return "ConsumerVerticleContext{" + getLoggingContext() + "}";
  }
}
