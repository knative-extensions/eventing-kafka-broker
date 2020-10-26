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
import static dev.knative.eventing.kafka.broker.core.testing.CoreObjects.egress5;
import static dev.knative.eventing.kafka.broker.core.testing.CoreObjects.egress6;
import static dev.knative.eventing.kafka.broker.core.testing.CoreObjects.resource1;
import static dev.knative.eventing.kafka.broker.core.testing.CoreObjects.resource2;
import static org.assertj.core.api.Assertions.assertThat;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.reconciler.impl.ResourcesReconcilerImpl;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@ExtendWith(VertxExtension.class)
@Execution(ExecutionMode.CONCURRENT)
public class ConsumerDeployerTest {

  @Test
  @Timeout(value = 2)
  public void shouldAddResourceAndDeployVerticles(final Vertx vertx, final VertxTestContext context) {
    final var resources = List.of(
      resource1(),
      resource2()
    );
    final var numEgresses = numEgresses(resources);
    final var checkpoints = context.checkpoint(1);

    final var consumerDeployer = new ConsumerDeployer(
      vertx,
      (resource, egress) -> new AbstractVerticle() {
      },
      100
    );

    final var reconciler = ResourcesReconcilerImpl
      .builder()
      .watchEgress(consumerDeployer)
      .build();

    reconciler.reconcile(resources)
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

    final var resources = List.of(
      resource1(),
      resource2()
    );
    final var checkpoint = context.checkpoint(1);

    final var consumerDeployer = new ConsumerDeployer(
      vertx,
      (resource, egress) -> {
        throw new UnsupportedOperationException();
      },
      100
    );

    final var reconciler = ResourcesReconcilerImpl
      .builder()
      .watchEgress(consumerDeployer)
      .build();

    reconciler.reconcile(resources)
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

    final var resourcesOld = List.of(
      DataPlaneContract.Resource.newBuilder()
        .setUid("1-1234")
        .addTopics("1-12345")
        .addEgresses(egress1())
        .build()
    );
    final var numEgressesOld = numEgresses(resourcesOld);

    final var resourcesNew = List.of(
      DataPlaneContract.Resource.newBuilder()
        .setUid("1-1234")
        .addTopics("1-12345")
        .build()
    );
    final var numEgressesNew = numEgresses(resourcesNew);

    final var checkpoints = context.checkpoint(2);

    final var consumerDeployer = new ConsumerDeployer(
      vertx,
      (resource, egress) -> new AbstractVerticle() {
      },
      100
    );

    final var reconciler = ResourcesReconcilerImpl
      .builder()
      .watchEgress(consumerDeployer)
      .build();

    reconciler.reconcile(resourcesOld)
      .onSuccess(ignored -> {

        context.verify(() -> {
          assertThat(vertx.deploymentIDs()).hasSize(numEgressesOld);
          checkpoints.flag();
        });

        reconciler.reconcile(resourcesNew)
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

    final var resourcesOld = List.of(
      DataPlaneContract.Resource.newBuilder()
        .setUid("1-1234")
        .addTopics("1-12345")
        .addAllEgresses(Arrays.asList(
          egress1(),
          egress2(),
          egress3()
        ))
        .build(),
      DataPlaneContract.Resource.newBuilder()
        .setUid("2-1234")
        .addTopics("2-12345")
        .addAllEgresses(Arrays.asList(
          egress4()
        ))
        .build()
    );
    final var numEgressesOld = numEgresses(resourcesOld);

    final var resourcesNew = List.of(
      DataPlaneContract.Resource.newBuilder()
        .setUid("1-1234")
        .addTopics("1-12345")
        .build()
    );
    final var numEgressesNew = numEgresses(resourcesNew);

    final var checkpoints = context.checkpoint(2);

    final var consumerDeployer = new ConsumerDeployer(
      vertx,
      (resource, egress) -> new AbstractVerticle() {
      },
      100
    );

    final var reconciler = ResourcesReconcilerImpl
      .builder()
      .watchEgress(consumerDeployer)
      .build();

    reconciler.reconcile(resourcesOld)
      .onSuccess(ignored -> {
        context.verify(() -> {
          assertThat(vertx.deploymentIDs()).hasSize(numEgressesOld);
          checkpoints.flag();
        });

        reconciler.reconcile(resourcesNew)
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

    final var resourcesOld = List.of(
      DataPlaneContract.Resource.newBuilder()
        .setUid("1-1234")
        .addTopics("1-12345")
        .addAllEgresses(Arrays.asList(
          egress1(),
          egress2()
        ))
        .build(),
      DataPlaneContract.Resource.newBuilder()
        .setUid("2-1234")
        .addTopics("2-12345")
        .addAllEgresses(Arrays.asList(
          egress4(),
          egress5(),
          egress6()
        ))
        .build()
    );
    final var numEgressesOld = numEgresses(resourcesOld);

    final var resourcesNew = List.of(
      DataPlaneContract.Resource.newBuilder()
        .setUid("1-1234")
        .addTopics("1-12345")
        .addAllEgresses(Arrays.asList(
          egress1(),
          egress3()
        ))
        .build(),
      DataPlaneContract.Resource.newBuilder()
        .setUid("2-1234")
        .addTopics("2-12345")
        .addEgresses(
          egress4()
        )
        .build()
    );
    final var numEgressesNew = numEgresses(resourcesNew);

    final var checkpoints = context.checkpoint(3);

    final var consumerDeployer = new ConsumerDeployer(
      vertx,
      (resource, egress) -> new AbstractVerticle() {
      },
      100
    );

    final var reconciler = ResourcesReconcilerImpl
      .builder()
      .watchEgress(consumerDeployer)
      .build();

    final var oldDeployments = vertx.deploymentIDs();
    reconciler.reconcile(resourcesOld)
      .onSuccess(ignored -> {

        context.verify(() -> {
          assertThat(oldDeployments).hasSize(numEgressesOld);
          checkpoints.flag();
        });

        reconciler.reconcile(resourcesNew)
          .onSuccess(ok -> {
            context.verify(() -> {
              assertThat(vertx.deploymentIDs()).hasSize(numEgressesNew);
              assertThat(vertx.deploymentIDs()).containsAll(oldDeployments);
              checkpoints.flag();
            });

            reconciler.reconcile(resourcesOld)
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

    final var resources = List.of(
      DataPlaneContract.Resource.newBuilder()
        .setUid("1-1234")
        .addTopics("1-12345")
        .addAllEgresses(Arrays.asList(
          egress1(),
          egress2()
        ))
        .build(),
      DataPlaneContract.Resource.newBuilder()
        .setUid("2-1234")
        .addTopics("2-12345")
        .addAllEgresses(Arrays.asList(
          egress4(),
          egress5(),
          egress6()
        ))
        .build()
    );
    final var numEgresses = numEgresses(resources);

    final var checkpoints = context.checkpoint(2);

    final var consumerDeployer = new ConsumerDeployer(
      vertx,
      (resource, egress) -> new AbstractVerticle() {
      },
      100
    );

    final var reconciler = ResourcesReconcilerImpl
      .builder()
      .watchEgress(consumerDeployer)
      .build();

    reconciler.reconcile(resources)
      .onSuccess(ignored -> {

        final var deployments = vertx.deploymentIDs();

        context.verify(() -> {
          assertThat(deployments).hasSize(numEgresses);
          checkpoints.flag();
        });

        reconciler.reconcile(resources).onSuccess(ok -> context.verify(() -> {
          assertThat(vertx.deploymentIDs()).containsExactly(deployments.toArray(new String[0]));
          checkpoints.flag();
        }));
      })
      .onFailure(context::failNow);
  }

  @Test
  public void shouldThrowIfEgressesInitialCapacityIsLessOrEqualToZero(final Vertx vertx) {
    Assertions.assertThrows(IllegalArgumentException.class, () -> new ConsumerDeployer(
      vertx,
      (resource, egress) -> null,
      -1
    ));
  }

  private static int numEgresses(Collection<DataPlaneContract.Resource> resources) {
    return resources.stream().mapToInt(DataPlaneContract.Resource::getEgressesCount).sum();
  }
}
