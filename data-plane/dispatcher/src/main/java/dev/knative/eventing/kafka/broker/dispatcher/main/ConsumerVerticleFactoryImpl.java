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
import dev.knative.eventing.kafka.broker.core.security.AuthProvider;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerVerticleFactory;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.ConsumerVerticle;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.ext.web.client.WebClientOptions;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

public class ConsumerVerticleFactoryImpl implements ConsumerVerticleFactory {

  private final Map<String, Object> consumerConfigs;
  private final WebClientOptions webClientOptions;
  private final Map<String, Object> producerConfigs;
  private final AuthProvider authProvider;
  private final MeterRegistry metricsRegistry;

  /**
   * All args constructor.
   *
   * @param consumerConfigs  base consumer configurations.
   * @param webClientOptions web client options.
   * @param producerConfigs  base producer configurations.
   * @param authProvider     auth provider.
   * @param metricsRegistry  meter registry to use to create metricsRegistry.
   */
  public ConsumerVerticleFactoryImpl(
    final Properties consumerConfigs,
    final WebClientOptions webClientOptions,
    final Properties producerConfigs,
    final AuthProvider authProvider,
    final MeterRegistry metricsRegistry) {

    Objects.requireNonNull(consumerConfigs, "provide consumerConfigs");
    Objects.requireNonNull(webClientOptions, "provide webClientOptions");
    Objects.requireNonNull(producerConfigs, "provide producerConfigs");

    this.consumerConfigs = consumerConfigs.entrySet()
      .stream()
      .map(e -> new SimpleImmutableEntry<>(e.getKey().toString(), e.getValue()))
      .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    this.producerConfigs = producerConfigs.entrySet()
      .stream()
      .map(e -> new SimpleImmutableEntry<>(e.getKey().toString(), e.getValue().toString()))
      .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

    this.webClientOptions = webClientOptions;
    this.authProvider = authProvider;
    this.metricsRegistry = metricsRegistry;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ConsumerVerticle get(final DataPlaneContract.Resource resource, final DataPlaneContract.Egress egress) {
    Objects.requireNonNull(resource, "provide resource");
    Objects.requireNonNull(egress, "provide egress");

    return new ConsumerVerticleBuilder(
      new ConsumerVerticleContext()
        .withConsumerConfigs(consumerConfigs)
        .withProducerConfigs(producerConfigs)
        .withWebClientOptions(webClientOptions)
        .withAuthProvider(authProvider)
        .withMeterRegistry(metricsRegistry)
        .withResource(resource, egress)
        .withConsumerFactory(ConsumerFactory.defaultFactory())
        .withProducerFactory(ProducerFactory.defaultFactory())
    )
      .build();
  }
}
