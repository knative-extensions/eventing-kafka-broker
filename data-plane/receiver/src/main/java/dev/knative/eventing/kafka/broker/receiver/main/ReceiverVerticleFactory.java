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

package dev.knative.eventing.kafka.broker.receiver.main;

import dev.knative.eventing.kafka.broker.core.metrics.Metrics;
import dev.knative.eventing.kafka.broker.core.metrics.Metrics;
import dev.knative.eventing.kafka.broker.core.security.AuthProvider;
import dev.knative.eventing.kafka.broker.receiver.IngressRequestHandler;
import dev.knative.eventing.kafka.broker.receiver.handler.IngressRequestHandlerImpl;
import dev.knative.eventing.kafka.broker.receiver.handler.MethodNotAllowedHandler;
import dev.knative.eventing.kafka.broker.receiver.handler.SimpleProbeHandler;
import dev.knative.eventing.kafka.broker.receiver.impl.IngressProducerReconcilableStore;
import dev.knative.eventing.kafka.broker.receiver.impl.ReceiverVerticle;
import dev.knative.eventing.kafka.broker.receiver.impl.StrictRequestToRecordMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Handler;
import io.vertx.core.Verticle;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.kafka.client.producer.KafkaProducer;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

class ReceiverVerticleFactory implements Supplier<Verticle> {

  private final ReceiverEnv env;
  private final Properties producerConfigs;
  private final HttpServerOptions httpServerOptions;

  private final Iterable<Handler<HttpServerRequest>> preHandlers;
  private final IngressRequestHandler ingressRequestHandler;

  ReceiverVerticleFactory(final ReceiverEnv env,
                          final Properties producerConfigs,
                          final MeterRegistry metricsRegistry,
                          final HttpServerOptions httpServerOptions) {

    this.env = env;
    this.producerConfigs = producerConfigs;
    this.httpServerOptions = httpServerOptions;

    this.preHandlers = List.of(
      new SimpleProbeHandler(env.getLivenessProbePath(), env.getReadinessProbePath()),
      MethodNotAllowedHandler.getInstance()
    );
    this.ingressRequestHandler = new IngressRequestHandlerImpl(
      StrictRequestToRecordMapper.getInstance(),
      metricsRegistry.counter(Metrics.HTTP_REQUESTS_MALFORMED_COUNT),
    metricsRegistry.counter(Metrics.HTTP_REQUESTS_PRODUCE_COUNT)
    );
  }

  @Override
  public Verticle get() {
    return new ReceiverVerticle(
      httpServerOptions,
      v -> new IngressProducerReconcilableStore(
        AuthProvider.kubernetes(),
        producerConfigs,
        properties -> KafkaProducer.create(v, properties)
      ),
      this.preHandlers,
      this.ingressRequestHandler
    );
  }
}
