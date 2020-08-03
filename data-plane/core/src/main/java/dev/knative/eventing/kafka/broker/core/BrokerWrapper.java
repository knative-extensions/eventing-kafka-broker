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

package dev.knative.eventing.kafka.broker.core;

import dev.knative.eventing.kafka.broker.core.config.BrokersConfig.Broker;
import java.util.Objects;

/**
 * BrokerWrapper wraps a Broker for implementing the Broker interface.
 *
 * <p>The wrapped Broker Broker must not be modified by callers.
 */
public class BrokerWrapper implements dev.knative.eventing.kafka.broker.core.Broker {

  private final Broker broker;

  /**
   * All args constructor.
   *
   * @param broker broker (it must not be modified by callers)
   */
  public BrokerWrapper(final Broker broker) {
    this.broker = broker;
  }

  @Override
  public String id() {
    return broker.getId();
  }

  @Override
  public String topic() {
    return broker.getTopic();
  }

  @Override
  public String deadLetterSink() {
    return broker.getDeadLetterSink();
  }

  @Override
  public String namespace() {
    return broker.getNamespace();
  }

  @Override
  public String name() {
    return broker.getName();
  }

  @Override
  public String bootstrapServers() {
    return broker.getBootstrapServers();
  }

  @Override
  public String path() {
    return "/" + namespace() + "/" + name();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BrokerWrapper that = (BrokerWrapper) o;

    return broker.getId().equals(that.id())
        && broker.getDeadLetterSink().equals(that.deadLetterSink())
        && broker.getTopic().equals(that.topic())
        && broker.getName().equals(that.name())
        && broker.getNamespace().equals(that.namespace())
        && broker.getBootstrapServers().equals(that.bootstrapServers())
        && path().equals(that.path());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        broker.getId(),
        broker.getDeadLetterSink(),
        broker.getTopic(),
        broker.getNamespace(),
        broker.getName(),
        broker.getBootstrapServers(),
        path()
    );
  }

  @Override
  public String toString() {
    return "BrokerWrapper{"
        + "broker=" + broker
        + '}';
  }
}
