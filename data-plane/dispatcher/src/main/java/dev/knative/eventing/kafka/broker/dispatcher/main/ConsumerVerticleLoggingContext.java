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

import java.util.List;

/**
 * This class holds logging information for a given consumer verticle.
 */
public final class ConsumerVerticleLoggingContext {

  private final List<String> topics;
  private final String consumerGroup;
  private final DataPlaneContract.Reference reference;

  public ConsumerVerticleLoggingContext(final ConsumerVerticleContext context) {
    this.topics = context.getResource().getTopicsList();
    this.consumerGroup = context.getEgress().getConsumerGroup();
    this.reference = context.getEgress().getReference();
  }

  @Override
  public String toString() {
    return "{" +
      "topics=" + topics +
      ", consumerGroup='" + consumerGroup + '\'' +
      ", reference=" + reference +
      '}';
  }
}
