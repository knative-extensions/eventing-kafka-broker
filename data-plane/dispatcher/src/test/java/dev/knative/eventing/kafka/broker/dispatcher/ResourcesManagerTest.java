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

import static dev.knative.eventing.kafka.broker.core.testing.CoreObjects.egress1;
import static dev.knative.eventing.kafka.broker.core.testing.CoreObjects.egress2;
import static dev.knative.eventing.kafka.broker.core.testing.CoreObjects.egress3;
import static dev.knative.eventing.kafka.broker.core.testing.CoreObjects.egress4;
import static dev.knative.eventing.kafka.broker.core.testing.CoreObjects.resource1;
import static dev.knative.eventing.kafka.broker.core.testing.CoreObjects.resource2;
import static org.assertj.core.api.Assertions.assertThat;

import dev.knative.eventing.kafka.broker.core.wrappers.Egress;
import dev.knative.eventing.kafka.broker.core.wrappers.Resource;
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
public class ResourcesManagerTest {

  @Test
  @Timeout(value = 2)
  public void shouldAddResourceAndDeployVerticles(final Vertx vertx, final VertxTestContext context) {

    final var resources = Map.of(
      resource1(), Set.of(egress1(), egress2(), egress4()),
      resource2(), Set.of(egress1(), egress2(), egress3())
    );
    final var numEgresses = numEgresses(resources);
    final var checkpoints = context.checkpoint(1);

    final var resourcesManager = new ResourcesManager(
      vertx,
      (resource, egress) -> Future.succeededFuture(new AbstractVerticle() {
      }),
      100,
      100
    );

    resourcesManager.reconcile(resources)
      .onSuccess(ignored -> context.verify(() -> {
        assertThat(vertx.deploymentIDs()).hasSize(numEgresses);
        checkpoints.flag();
      }))
      .onFailure(context::failNow);
  }

  @Test
  @Timeout(value = 2)
  public void shouldNotDeployWhenFailedToGetVerticle(
    final Vertx vertx,
    final VertxTestContext context) {

    final var resources = Map.of(
      resource1(), Set.of(egress1(), egress2(), egress4()),
      resource2(), Set.of(egress1(), egress2(), egress3())
    );
    final var checkpoint = context.checkpoint(1);

    final var resourcesManager = new ResourcesManager(
      vertx,
      (resource, egress) -> Future.failedFuture(new UnsupportedOperationException()),
      100,
      100
    );

    resourcesManager.reconcile(resources)
      .onFailure(ignored -> context.verify(() -> {
        assertThat(vertx.deploymentIDs()).hasSize(0);
        checkpoint.flag();
      }))
      .onFailure(context::failNow);
  }

  @Test
  @Timeout(value = 2)
  public void shouldStopVerticleWhenEgressDeleted(
    final Vertx vertx,
    final VertxTestContext context) {

    final var resourcesOld = Map.of(
      resource1(), Set.of(egress1())
    );
    final var numEgressesOld = numEgresses(resourcesOld);

    final Map<Resource, Set<Egress>> resourcesNew = Map.of(
      resource1(), Set.of()
    );
    final var numEgressesNew = numEgresses(resourcesNew);

    final var checkpoints = context.checkpoint(2);

    final var resourcesManager = new ResourcesManager(
      vertx,
      (resource, egress) -> Future.succeededFuture(new AbstractVerticle() {
      }),
      100,
      100
    );

    resourcesManager.reconcile(resourcesOld)
      .onSuccess(ignored -> {

        context.verify(() -> {
          assertThat(vertx.deploymentIDs()).hasSize(numEgressesOld);
          checkpoints.flag();
        });

        resourcesManager.reconcile(resourcesNew)
          .onSuccess(ok -> context.verify(() -> {
            assertThat(vertx.deploymentIDs()).hasSize(numEgressesNew);
            checkpoints.flag();
          }))
          .onFailure(context::failNow);
      })
      .onFailure(context::failNow);
  }

  @Test
  @Timeout(value = 2)
  public void shouldStopVerticlesWhenResourceDeleted(
    final Vertx vertx,
    final VertxTestContext context) {

    final var resourcesOld = Map.of(
      resource1(), Set.of(egress1(), egress2(), egress4()),
      resource2(), Set.of(egress1(), egress2(), egress3())
    );
    final var numEgressesOld = numEgresses(resourcesOld);

    final Map<Resource, Set<Egress>> resourcesNew = Map.of(
      resource1(), Set.of()
    );
    final var numEgressesNew = numEgresses(resourcesNew);

    final var checkpoints = context.checkpoint(2);

    final var resourcesManager = new ResourcesManager(
      vertx,
      (resource, egress) -> Future.succeededFuture(new AbstractVerticle() {
      }),
      100,
      100
    );

    resourcesManager.reconcile(resourcesOld)
      .onSuccess(ignored -> {
        context.verify(() -> {
          assertThat(vertx.deploymentIDs()).hasSize(numEgressesOld);
          checkpoints.flag();
        });

        resourcesManager.reconcile(resourcesNew)
          .onSuccess(ok -> context.verify(() -> {
            assertThat(vertx.deploymentIDs()).hasSize(numEgressesNew);
            checkpoints.flag();
          }))
          .onFailure(context::failNow);
      })
      .onFailure(context::failNow);
  }

  @Test
  @Timeout(value = 2)
  public void shouldStopAndStartVerticlesWhenEgressDeletedAndReAdded(
    final Vertx vertx,
    final VertxTestContext context) {

    final var resourcesOld = Map.of(
      resource1(), Set.of(egress1(), egress2(), egress4()),
      resource2(), Set.of(egress1(), egress2(), egress3())
    );
    final var numEgressesOld = numEgresses(resourcesOld);

    final Map<Resource, Set<Egress>> resourcesNew = Map.of(
      resource1(), Set.of(egress3(), egress1()),
      resource2(), Set.of(egress1())
    );
    final var numEgressesNew = numEgresses(resourcesNew);

    final var checkpoints = context.checkpoint(3);

    final var resourcesManager = new ResourcesManager(
      vertx,
      (resource, egress) -> Future.succeededFuture(new AbstractVerticle() {
      }),
      100,
      100
    );

    final var oldDeployments = vertx.deploymentIDs();
    resourcesManager.reconcile(resourcesOld)
      .onSuccess(ignored -> {

        context.verify(() -> {
          assertThat(oldDeployments).hasSize(numEgressesOld);
          checkpoints.flag();
        });

        resourcesManager.reconcile(resourcesNew)
          .onSuccess(ok -> {
            context.verify(() -> {
              assertThat(vertx.deploymentIDs()).hasSize(numEgressesNew);
              assertThat(vertx.deploymentIDs()).containsAll(oldDeployments);
              checkpoints.flag();
            });

            resourcesManager.reconcile(resourcesOld)
              .onSuccess(ok2 -> context.verify(() -> {
                assertThat(oldDeployments).hasSize(numEgressesOld);
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

    final var resources = Map.of(
      resource1(), Set.of(egress1(), egress2(), egress4()),
      resource2(), Set.of(egress1(), egress2(), egress3())
    );
    final var numEgresses = numEgresses(resources);

    final var resources2 = Map.of(
      resource1(), Set.of(egress1(), egress2(), egress4()),
      resource2(), Set.of(egress1(), egress2(), egress3())
    );

    final var checkpoints = context.checkpoint(2);

    final var resourcesManager = new ResourcesManager(
      vertx,
      (resource, egress) -> Future.succeededFuture(new AbstractVerticle() {
      }),
      100,
      100
    );
    resourcesManager.reconcile(resources)
      .onSuccess(ignored -> {

        final var deployments = vertx.deploymentIDs();

        context.verify(() -> {
          assertThat(deployments).hasSize(numEgresses);
          checkpoints.flag();
        });

        resourcesManager.reconcile(resources2).onSuccess(ok -> context.verify(() -> {
          assertThat(vertx.deploymentIDs()).containsExactly(deployments.toArray(new String[0]));
          checkpoints.flag();
        }));
      })
      .onFailure(context::failNow);
  }

  @Test
  public void shouldThrowIfResourcesInitialCapacityIsLessOrEqualToZero(final Vertx vertx) {
    Assertions.assertThrows(IllegalArgumentException.class, () -> new ResourcesManager(
      vertx,
      (resource, egress) -> Future.succeededFuture(),
      -1,
      100
    ));
  }

  @Test
  public void shouldThrowIfEgressesInitialCapacityIsLessOrEqualToZero(final Vertx vertx) {
    Assertions.assertThrows(IllegalArgumentException.class, () -> new ResourcesManager(
      vertx,
      (resource, egress) -> Future.succeededFuture(),
      100,
      -1
    ));
  }

  private static int numEgresses(Map<Resource, Set<Egress>> resources) {
    return resources.values().stream().mapToInt(Set::size).sum();
  }
}
