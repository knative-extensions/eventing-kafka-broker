package dev.knative.eventing.kafka.broker.dispatcher;

import dev.knative.eventing.kafka.broker.core.Broker;
import dev.knative.eventing.kafka.broker.core.Trigger;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;

/**
 * ConsumerVerticleFactory is responsible for instantiating consumer verticles.
 */
@FunctionalInterface
public interface ConsumerVerticleFactory<T> {

  /**
   * Get a new consumer verticle.
   *
   * @param trigger trigger data.
   * @return a new consumer verticle.
   */
  Future<AbstractVerticle> get(final Broker broker, final Trigger<T> trigger);
}
