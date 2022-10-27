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

import dev.knative.eventing.kafka.broker.dispatcher.main.ConsumerVerticleContext;

import java.util.Set;

public class UnorderedConsumerVerticleTest extends AbstractConsumerVerticleTest {

  @Override
  ConsumerVerticle createConsumerVerticle(final ConsumerVerticleContext context,
                                          final ConsumerVerticle.Initializer initializer) {
    return new UnorderedConsumerVerticle(context, initializer);
  }
}
