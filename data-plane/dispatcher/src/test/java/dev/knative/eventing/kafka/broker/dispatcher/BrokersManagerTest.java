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

import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.broker1;
import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.broker2;
import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.trigger1;
import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.trigger2;
import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.trigger3;
import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.trigger4;
import static org.assertj.core.api.Assertions.assertThat;


import dev.knative.eventing.kafka.broker.core.Broker;
import dev.knative.eventing.kafka.broker.core.Trigger;
import io.cloudevents.CloudEvent;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@ExtendWith(VertxExtension.class)
@Execution(ExecutionMode.CONCURRENT)
public class BrokersManagerTest {

  @Test
  @Timeout(value = 2)
  public void shouldAddBrokerAndDeployVerticles(final Vertx vertx, final VertxTestContext context) {

    final var brokers = Map.of(
        broker1(), Set.of(trigger1(), trigger2(), trigger4()),
        broker2(), Set.of(trigger1(), trigger2(), trigger3())
    );
    final var numTriggers = numTriggers(brokers);
    final var checkpoints = context.checkpoint(1);

    final var brokersManager = new BrokersManager<CloudEvent>(
        vertx,
        (broker, trigger) -> Future.succeededFuture(new AbstractVerticle() {
        }),
        100,
        100
    );

    brokersManager.reconcile(brokers)
        .onSuccess(ignored -> context.verify(() -> {
          assertThat(vertx.deploymentIDs()).hasSize(numTriggers);
          checkpoints.flag();
        }))
        .onFailure(context::failNow);
  }

  @Test
  @Timeout(value = 2)
  public void shouldNotDeployWhenFailedToGetVerticle(
      final Vertx vertx,
      final VertxTestContext context) {

    final var brokers = Map.of(
        broker1(), Set.of(trigger1(), trigger2(), trigger4()),
        broker2(), Set.of(trigger1(), trigger2(), trigger3())
    );
    final var checkpoint = context.checkpoint(1);

    final var brokersManager = new BrokersManager<CloudEvent>(
        vertx,
        (broker, trigger) -> Future.failedFuture(new UnsupportedOperationException()),
        100,
        100
    );

    brokersManager.reconcile(brokers)
        .onFailure(ignored -> context.verify(() -> {
          assertThat(vertx.deploymentIDs()).hasSize(0);
          checkpoint.flag();
        }))
        .onFailure(context::failNow);
  }

  @Test
  @Timeout(value = 2)
  public void shouldStopVerticleWhenTriggerDeleted(
      final Vertx vertx,
      final VertxTestContext context) {

    final var brokersOld = Map.of(
        broker1(), Set.of(trigger1())
    );
    final var numTriggersOld = numTriggers(brokersOld);

    final Map<Broker, Set<Trigger<CloudEvent>>> brokersNew = Map.of(
        broker1(), Set.of()
    );
    final var numTriggersNew = numTriggers(brokersNew);

    final var checkpoints = context.checkpoint(2);

    final var brokersManager = new BrokersManager<CloudEvent>(
        vertx,
        (broker, trigger) -> Future.succeededFuture(new AbstractVerticle() {
        }),
        100,
        100
    );

    brokersManager.reconcile(brokersOld)
        .onSuccess(ignored -> {

          context.verify(() -> {
            assertThat(vertx.deploymentIDs()).hasSize(numTriggersOld);
            checkpoints.flag();
          });

          brokersManager.reconcile(brokersNew)
              .onSuccess(ok -> context.verify(() -> {
                assertThat(vertx.deploymentIDs()).hasSize(numTriggersNew);
                checkpoints.flag();
              }))
              .onFailure(context::failNow);
        })
        .onFailure(context::failNow);
  }

  @Test
  @Timeout(value = 2)
  public void shouldStopVerticlesWhenBrokerDeleted(
      final Vertx vertx,
      final VertxTestContext context) {

    final var brokersOld = Map.of(
        broker1(), Set.of(trigger1(), trigger2(), trigger4()),
        broker2(), Set.of(trigger1(), trigger2(), trigger3())
    );
    final var numTriggersOld = numTriggers(brokersOld);

    final Map<Broker, Set<Trigger<CloudEvent>>> brokersNew = Map.of(
        broker1(), Set.of()
    );
    final var numTriggersNew = numTriggers(brokersNew);

    final var checkpoints = context.checkpoint(2);

    final var brokersManager = new BrokersManager<CloudEvent>(
        vertx,
        (broker, trigger) -> Future.succeededFuture(new AbstractVerticle() {
        }),
        100,
        100
    );

    brokersManager.reconcile(brokersOld)
        .onSuccess(ignored -> {
          context.verify(() -> {
            assertThat(vertx.deploymentIDs()).hasSize(numTriggersOld);
            checkpoints.flag();
          });

          brokersManager.reconcile(brokersNew)
              .onSuccess(ok -> context.verify(() -> {
                assertThat(vertx.deploymentIDs()).hasSize(numTriggersNew);
                checkpoints.flag();
              }))
              .onFailure(context::failNow);
        })
        .onFailure(context::failNow);
  }

  @Test
  @Timeout(value = 2)
  public void shouldStopAndStartVerticlesWhenTriggerDeletedAndReAdded(
      final Vertx vertx,
      final VertxTestContext context) {

    final var brokersOld = Map.of(
        broker1(), Set.of(trigger1(), trigger2(), trigger4()),
        broker2(), Set.of(trigger1(), trigger2(), trigger3())
    );
    final var numTriggersOld = numTriggers(brokersOld);

    final Map<Broker, Set<Trigger<CloudEvent>>> brokersNew = Map.of(
        broker1(), Set.of(trigger3(), trigger1()),
        broker2(), Set.of(trigger1())
    );
    final var numTriggersNew = numTriggers(brokersNew);

    final var checkpoints = context.checkpoint(3);

    final var brokersManager = new BrokersManager<CloudEvent>(
        vertx,
        (broker, trigger) -> Future.succeededFuture(new AbstractVerticle() {
        }),
        100,
        100
    );

    final var oldDeployments = vertx.deploymentIDs();
    brokersManager.reconcile(brokersOld)
        .onSuccess(ignored -> {

          context.verify(() -> {
            assertThat(oldDeployments).hasSize(numTriggersOld);
            checkpoints.flag();
          });

          brokersManager.reconcile(brokersNew)
              .onSuccess(ok -> {
                context.verify(() -> {
                  assertThat(vertx.deploymentIDs()).hasSize(numTriggersNew);
                  assertThat(vertx.deploymentIDs()).contains(oldDeployments.toArray(new String[0]));
                  checkpoints.flag();
                });

                brokersManager.reconcile(brokersOld).onSuccess(ok2 -> context.verify(() -> {
                  assertThat(oldDeployments).hasSize(numTriggersOld);
                  checkpoints.flag();
                }));
              })
              .onFailure(context::failNow);
        })
        .onFailure(context::failNow);
  }

  @Test
  @Timeout(value = 2)
  public void shouldDoNothingWhenTheStateIsTheSame(
      final Vertx vertx,
      final VertxTestContext context) {

    final var brokers = Map.of(
        broker1(), Set.of(trigger1(), trigger2(), trigger4()),
        broker2(), Set.of(trigger1(), trigger2(), trigger3())
    );
    final var numTriggers = numTriggers(brokers);

    final var brokers2 = Map.of(
        broker1(), Set.of(trigger1(), trigger2(), trigger4()),
        broker2(), Set.of(trigger1(), trigger2(), trigger3())
    );

    final var checkpoints = context.checkpoint(2);

    final var brokersManager = new BrokersManager<CloudEvent>(
        vertx,
        (broker, trigger) -> Future.succeededFuture(new AbstractVerticle() {
        }),
        100,
        100
    );
    brokersManager.reconcile(brokers)
        .onSuccess(ignored -> {

          final var deployments = vertx.deploymentIDs();

          context.verify(() -> {
            assertThat(deployments).hasSize(numTriggers);
            checkpoints.flag();
          });

          brokersManager.reconcile(brokers2).onSuccess(ok -> context.verify(() -> {
            assertThat(vertx.deploymentIDs()).containsExactly(deployments.toArray(new String[0]));
            checkpoints.flag();
          }));
        })
        .onFailure(context::failNow);
  }

  @Test
  public void shouldThrowIfBrokersInitialCapacityIsLessOrEqualToZero(final Vertx vertx) {
    Assertions.assertThrows(IllegalArgumentException.class, () -> new BrokersManager<>(
        vertx,
        (broker, trigger) -> Future.succeededFuture(),
        -1,
        100
    ));
  }

  @Test
  public void shouldThrowIfTriggersInitialCapacityIsLessOrEqualToZero(final Vertx vertx) {
    Assertions.assertThrows(IllegalArgumentException.class, () -> new BrokersManager<>(
        vertx,
        (broker, trigger) -> Future.succeededFuture(),
        100,
        -1
    ));
  }

  private static int numTriggers(Map<Broker, Set<Trigger<CloudEvent>>> brokers) {
    return brokers.values().stream().mapToInt(Set::size).sum();
  }
}
