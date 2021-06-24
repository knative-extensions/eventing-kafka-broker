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

import dev.knative.eventing.kafka.broker.core.security.AuthProvider;
import dev.knative.eventing.kafka.broker.receiver.ReceiverVerticle;
import dev.knative.eventing.kafka.broker.receiver.RequestMapper;
import dev.knative.eventing.kafka.broker.receiver.SimpleProbeHandlerDecorator;
import dev.knative.eventing.kafka.broker.receiver.StrictRequestToRecordMapper;
import io.micrometer.core.instrument.Counter;
import io.vertx.core.Verticle;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.kafka.client.producer.KafkaProducer;
import java.util.Properties;
import java.util.function.Supplier;

class ReceiverVerticleFactory implements Supplier<Verticle> {

  private final ReceiverEnv env;
  private final Properties producerConfigs;
  private final Counter badRequestCounter;
  private final Counter produceEventsCounter;
  private final HttpServerOptions httpServerOptions;

  ReceiverVerticleFactory(final ReceiverEnv env,
                          final Properties producerConfigs,
                          final Counter badRequestCounter,
                          final Counter produceEventsCounter,
                          final HttpServerOptions httpServerOptions) {

    this.env = env;
    this.producerConfigs = producerConfigs;
    this.badRequestCounter = badRequestCounter;
    this.produceEventsCounter = produceEventsCounter;
    this.httpServerOptions = httpServerOptions;
  }

  @Override
  public Verticle get() {
    return new ReceiverVerticle(
      httpServerOptions,
      v -> new RequestMapper(
        v,
        AuthProvider.kubernetes(),
        producerConfigs,
        StrictRequestToRecordMapper.getInstance(),
        properties -> KafkaProducer.create(v, properties),
        badRequestCounter,
        produceEventsCounter
      ),
      h -> new SimpleProbeHandlerDecorator(
        env.getLivenessProbePath(),
        env.getReadinessProbePath(),
        h
      )
    );
  }
}
