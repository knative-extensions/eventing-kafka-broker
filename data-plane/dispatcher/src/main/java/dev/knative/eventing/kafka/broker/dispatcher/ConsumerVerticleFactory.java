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

package dev.knative.eventing.kafka.broker.dispatcher;

import dev.knative.eventing.kafka.broker.core.wrappers.Egress;
import dev.knative.eventing.kafka.broker.core.wrappers.Resource;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;

/**
 * ConsumerVerticleFactory is responsible for instantiating consumer verticles.
 */
@FunctionalInterface
public interface ConsumerVerticleFactory {

  /**
   * Get a new consumer verticle.
   *
   * @param egress trigger data.
   * @return a new consumer verticle.
   */
  Future<AbstractVerticle> get(final Resource resource, final Egress egress);
}
