package dev.knative.eventing.kafka.broker.core.reconciler.impl;

import static net.logstash.logback.argument.StructuredArguments.keyValue;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.reconciler.ResourcesReconciler;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourcesReconcilerMessageHandler implements Handler<Message<Object>> {

  private static final Logger logger = LoggerFactory.getLogger(ResourcesReconcilerMessageHandler.class);

  public final static String ADDRESS = "resourcesreconciler.core";

  private final ResourcesReconciler resourcesReconciler;

  public ResourcesReconcilerMessageHandler(
    ResourcesReconciler resourcesReconciler) {
    this.resourcesReconciler = resourcesReconciler;
  }

  @Override
  public void handle(Message<Object> event) {
    DataPlaneContract.Contract contract = (DataPlaneContract.Contract) event.body();
    resourcesReconciler.reconcile(contract.getResourcesList())
      .onSuccess(
        v -> logger.info("reconciled contract generation {}", keyValue("contractGeneration", contract.getGeneration())))
      .onFailure(cause -> logger
        .error("failed to reconcile contract generation {}", keyValue("contractGeneration", contract.getGeneration()),
          cause));
  }

  public static MessageConsumer<Object> start(EventBus eventBus, ResourcesReconciler reconciler) {
    return eventBus.localConsumer(ADDRESS, new ResourcesReconcilerMessageHandler(reconciler));
  }
}
