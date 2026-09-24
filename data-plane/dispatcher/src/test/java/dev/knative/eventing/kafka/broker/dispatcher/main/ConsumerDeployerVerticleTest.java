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
import dev.knative.eventing.kafka.broker.core.reconciler.ResourcesReconciler;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@ExtendWith(VertxExtension.class)
@Execution(ExecutionMode.CONCURRENT)
public class ConsumerDeployerVerticleTest {

    private static final int NUM_SYSTEM_VERTICLES = 1;

    @Test
    @Timeout(value = 2)
    public void shouldAddResourceAndDeployVerticles(final Vertx vertx, final VertxTestContext context)
            throws ExecutionException, InterruptedException {
        final var resources = List.of(resource1(), resource2());
        final var numEgresses = numEgresses(resources);
        final var checkpoints = context.checkpoint(1);

        final var consumerDeployer = new ConsumerDeployerVerticle(egressContext -> new AbstractVerticle() {}, 100);

        vertx.deployVerticle(consumerDeployer)
                .toCompletionStage()
                .toCompletableFuture()
                .get();

        final var reconciler =
                ResourcesReconciler.builder().watchEgress(consumerDeployer).build();

        reconciler
                .reconcile(DataPlaneContract.Contract.newBuilder()
                        .addAllResources(resources)
                        .build())
                .onSuccess(ignored -> context.verify(() -> {
                    assertThat(vertx.deploymentIDs()).hasSize(numEgresses + NUM_SYSTEM_VERTICLES);
                    checkpoints.flag();
                }))
                .onFailure(context::failNow);
    }

    @Test
    @Timeout(value = 2)
    public void shouldNotDeployWhenFailedToGetVerticle(final Vertx vertx, final VertxTestContext context)
            throws ExecutionException, InterruptedException {

        final var resources = List.of(resource1(), resource2());
        final var checkpoint = context.checkpoint(1);

        final var consumerDeployer = new ConsumerDeployerVerticle(
                egressContext -> {
                    throw new UnsupportedOperationException();
                },
                100);

        vertx.deployVerticle(consumerDeployer)
                .toCompletionStage()
                .toCompletableFuture()
                .get();

        final var reconciler =
                ResourcesReconciler.builder().watchEgress(consumerDeployer).build();

        reconciler
                .reconcile(DataPlaneContract.Contract.newBuilder()
                        .addAllResources(resources)
                        .build())
                .onFailure(ignored -> context.verify(() -> {
                    assertThat(vertx.deploymentIDs()).hasSize(NUM_SYSTEM_VERTICLES);
                    checkpoint.flag();
                }))
                .onSuccess(v -> context.failNow("Unexpected success"));
    }

    @Test
    @Timeout(value = 2)
    public void shouldStopVerticleWhenEgressDeleted(final Vertx vertx, final VertxTestContext context)
            throws ExecutionException, InterruptedException {

        final var resourcesOld = List.of(DataPlaneContract.Resource.newBuilder()
                .setUid("1-1234")
                .addTopics("1-12345")
                .addEgresses(egress1())
                .build());
        final var numEgressesOld = numEgresses(resourcesOld);

        final var resourcesNew = List.of(DataPlaneContract.Resource.newBuilder()
                .setUid("1-1234")
                .addTopics("1-12345")
                .build());
        final var numEgressesNew = numEgresses(resourcesNew);

        final var checkpoints = context.checkpoint(2);

        final var consumerDeployer = new ConsumerDeployerVerticle(egressContext -> new AbstractVerticle() {}, 100);

        vertx.deployVerticle(consumerDeployer)
                .toCompletionStage()
                .toCompletableFuture()
                .get();

        final var reconciler =
                ResourcesReconciler.builder().watchEgress(consumerDeployer).build();

        reconciler
                .reconcile(DataPlaneContract.Contract.newBuilder()
                        .addAllResources(resourcesOld)
                        .build())
                .onSuccess(ignored -> {
                    context.verify(() -> {
                        assertThat(vertx.deploymentIDs()).hasSize(numEgressesOld + NUM_SYSTEM_VERTICLES);
                        checkpoints.flag();
                    });

                    reconciler
                            .reconcile(DataPlaneContract.Contract.newBuilder()
                                    .addAllResources(resourcesNew)
                                    .build())
                            .onSuccess(ok -> context.verify(() -> {
                                assertThat(vertx.deploymentIDs()).hasSize(numEgressesNew + NUM_SYSTEM_VERTICLES);
                                checkpoints.flag();
                            }))
                            .onFailure(context::failNow);
                })
                .onFailure(context::failNow);
    }

    @Test
    @Timeout(value = 2)
    public void shouldStopVerticlesWhenResourceDeleted(final Vertx vertx, final VertxTestContext context)
            throws ExecutionException, InterruptedException {

        final var resourcesOld = List.of(
                DataPlaneContract.Resource.newBuilder()
                        .setUid("1-1234")
                        .addTopics("1-12345")
                        .addAllEgresses(Arrays.asList(egress1(), egress2(), egress3()))
                        .build(),
                DataPlaneContract.Resource.newBuilder()
                        .setUid("2-1234")
                        .addTopics("2-12345")
                        .addAllEgresses(Arrays.asList(egress4()))
                        .build());
        final var numEgressesOld = numEgresses(resourcesOld);

        final var resourcesNew = List.of(DataPlaneContract.Resource.newBuilder()
                .setUid("1-1234")
                .addTopics("1-12345")
                .build());
        final var numEgressesNew = numEgresses(resourcesNew);

        final var checkpoints = context.checkpoint(2);

        final var consumerDeployer = new ConsumerDeployerVerticle(egressContext -> new AbstractVerticle() {}, 100);

        vertx.deployVerticle(consumerDeployer)
                .toCompletionStage()
                .toCompletableFuture()
                .get();

        final var reconciler =
                ResourcesReconciler.builder().watchEgress(consumerDeployer).build();

        reconciler
                .reconcile(DataPlaneContract.Contract.newBuilder()
                        .addAllResources(resourcesOld)
                        .build())
                .onSuccess(ignored -> {
                    context.verify(() -> {
                        assertThat(vertx.deploymentIDs()).hasSize(numEgressesOld + NUM_SYSTEM_VERTICLES);
                        checkpoints.flag();
                    });

                    reconciler
                            .reconcile(DataPlaneContract.Contract.newBuilder()
                                    .addAllResources(resourcesNew)
                                    .build())
                            .onSuccess(ok -> context.verify(() -> {
                                assertThat(vertx.deploymentIDs()).hasSize(numEgressesNew + NUM_SYSTEM_VERTICLES);
                                checkpoints.flag();
                            }))
                            .onFailure(context::failNow);
                })
                .onFailure(context::failNow);
    }

    @Test
    @Timeout(value = 2)
    public void shouldStopAndStartVerticlesWhenEgressDeletedAndReAdded(
            final Vertx vertx, final VertxTestContext context) throws ExecutionException, InterruptedException {

        final var resourcesOld = List.of(
                DataPlaneContract.Resource.newBuilder()
                        .setUid("1-1234")
                        .addTopics("1-12345")
                        .addAllEgresses(Arrays.asList(egress1(), egress2()))
                        .build(),
                DataPlaneContract.Resource.newBuilder()
                        .setUid("2-1234")
                        .addTopics("2-12345")
                        .addAllEgresses(Arrays.asList(egress4(), egress5(), egress6()))
                        .build());
        final var numEgressesOld = numEgresses(resourcesOld);

        final var resourcesNew = List.of(
                DataPlaneContract.Resource.newBuilder()
                        .setUid("1-1234")
                        .addTopics("1-12345")
                        .addAllEgresses(Arrays.asList(egress1(), egress3()))
                        .build(),
                DataPlaneContract.Resource.newBuilder()
                        .setUid("2-1234")
                        .addTopics("2-12345")
                        .addEgresses(egress4())
                        .build());
        final var numEgressesNew = numEgresses(resourcesNew);

        final var checkpoints = context.checkpoint(3);

        final var consumerDeployer = new ConsumerDeployerVerticle(egressContext -> new AbstractVerticle() {}, 100);

        vertx.deployVerticle(consumerDeployer)
                .toCompletionStage()
                .toCompletableFuture()
                .get();

        final var reconciler =
                ResourcesReconciler.builder().watchEgress(consumerDeployer).build();

        final var oldDeployments = vertx.deploymentIDs();
        reconciler
                .reconcile(DataPlaneContract.Contract.newBuilder()
                        .addAllResources(resourcesOld)
                        .build())
                .onSuccess(ignored -> {
                    context.verify(() -> {
                        assertThat(oldDeployments).hasSize(numEgressesOld + NUM_SYSTEM_VERTICLES);
                        checkpoints.flag();
                    });

                    reconciler
                            .reconcile(DataPlaneContract.Contract.newBuilder()
                                    .addAllResources(resourcesNew)
                                    .build())
                            .onSuccess(ok -> {
                                context.verify(() -> {
                                    assertThat(vertx.deploymentIDs()).hasSize(numEgressesNew + NUM_SYSTEM_VERTICLES);
                                    assertThat(vertx.deploymentIDs()).containsAll(oldDeployments);
                                    checkpoints.flag();
                                });

                                reconciler
                                        .reconcile(DataPlaneContract.Contract.newBuilder()
                                                .addAllResources(resourcesOld)
                                                .build())
                                        .onSuccess(ok2 -> context.verify(() -> {
                                            assertThat(oldDeployments).hasSize(numEgressesOld + NUM_SYSTEM_VERTICLES);
                                            checkpoints.flag();
                                        }));
                            })
                            .onFailure(context::failNow);
                })
                .onFailure(context::failNow);
    }

    @Test
    @Timeout(value = 2)
    public void shouldDoNothingWhenTheStateIsTheSame(final Vertx vertx, final VertxTestContext context)
            throws ExecutionException, InterruptedException {

        final var resources = List.of(
                DataPlaneContract.Resource.newBuilder()
                        .setUid("1-1234")
                        .addTopics("1-12345")
                        .addAllEgresses(Arrays.asList(egress1(), egress2()))
                        .build(),
                DataPlaneContract.Resource.newBuilder()
                        .setUid("2-1234")
                        .addTopics("2-12345")
                        .addAllEgresses(Arrays.asList(egress4(), egress5(), egress6()))
                        .build());
        final var numEgresses = numEgresses(resources);

        final var checkpoints = context.checkpoint(2);

        final var consumerDeployer = new ConsumerDeployerVerticle(egressContext -> new AbstractVerticle() {}, 100);

        vertx.deployVerticle(consumerDeployer)
                .toCompletionStage()
                .toCompletableFuture()
                .get();

        final var reconciler =
                ResourcesReconciler.builder().watchEgress(consumerDeployer).build();

        reconciler
                .reconcile(DataPlaneContract.Contract.newBuilder()
                        .addAllResources(resources)
                        .build())
                .onSuccess(ignored -> {
                    final var deployments = vertx.deploymentIDs();

                    context.verify(() -> {
                        assertThat(deployments).hasSize(numEgresses + NUM_SYSTEM_VERTICLES);
                        checkpoints.flag();
                    });

                    reconciler
                            .reconcile(DataPlaneContract.Contract.newBuilder()
                                    .addAllResources(resources)
                                    .build())
                            .onSuccess(ok -> context.verify(() -> {
                                assertThat(vertx.deploymentIDs()).containsAll(deployments);
                                checkpoints.flag();
                            }));
                })
                .onFailure(context::failNow);
    }

    @Test
    public void shouldThrowIfEgressesInitialCapacityIsLessOrEqualToZero(final Vertx vertx) {
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> new ConsumerDeployerVerticle(egressContext -> null, -1));
    }

    @Test
    @Timeout(value = 5)
    public void shouldNotOrphanVerticleWhenUpdateOverlapsInFlightDeploy(
            final Vertx vertx, final VertxTestContext context) throws ExecutionException, InterruptedException {

        // Simulate a slow-deploying verticle: the first deploy completes only after we manually complete the promise.
        final Promise<Void> slowDeployGate = Promise.promise();
        final int[] deployCount = {0};

        final var consumerDeployer = new ConsumerDeployerVerticle(
                egressContext -> new AbstractVerticle() {
                    @Override
                    public void start(Promise<Void> startPromise) {
                        deployCount[0]++;
                        if (deployCount[0] == 1) {
                            // First deploy: block until the gate is released (simulates slow startup)
                            slowDeployGate.future().onComplete(startPromise);
                        } else {
                            startPromise.complete();
                        }
                    }
                },
                100);

        vertx.deployVerticle(consumerDeployer)
                .toCompletionStage()
                .toCompletableFuture()
                .get();

        final var resource = DataPlaneContract.Resource.newBuilder()
                .setUid("test-resource")
                .addTopics("test-topic")
                .addEgresses(egress1())
                .build();

        final var reconciler =
                ResourcesReconciler.builder().watchEgress(consumerDeployer).build();

        // Start first new-egress deploy (will block in start())
        final var firstDeploy = reconciler.reconcile(
                DataPlaneContract.Contract.newBuilder().addResources(resource).build());

        // Fire an update while the first deploy is still in-flight (gate not yet released).
        // Without the serialization fix this races with the in-flight deploy and orphans a verticle.
        final var update = reconciler.reconcile(DataPlaneContract.Contract.newBuilder()
                .addResources(DataPlaneContract.Resource.newBuilder()
                        .setUid("test-resource")
                        .addTopics("test-topic")
                        .addEgresses(DataPlaneContract.Egress.newBuilder(egress1())
                                .setDestination("http://updated-destination/")
                                .build())
                        .build())
                .build());

        // Release the gate so the first deploy can finish.
        slowDeployGate.complete();

        // Wait for both operations to settle, then verify exactly one verticle is running.
        firstDeploy
                .compose(v -> update)
                .onComplete(r -> context.verify(() -> {
                    // The consumerDeployer verticle itself + exactly one consumer verticle.
                    assertThat(vertx.deploymentIDs()).hasSize(NUM_SYSTEM_VERTICLES + 1);
                    context.completeNow();
                }));
    }

    private static int numEgresses(Collection<DataPlaneContract.Resource> resources) {
        return resources.stream()
                .mapToInt(DataPlaneContract.Resource::getEgressesCount)
                .sum();
    }
}
