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
import dev.knative.eventing.kafka.broker.core.metrics.Metrics;
import dev.knative.eventing.kafka.broker.core.security.Credentials;
import dev.knative.eventing.kafka.broker.core.security.KafkaClientsAuth;
import dev.knative.eventing.kafka.broker.dispatcher.CloudEventSender;
import dev.knative.eventing.kafka.broker.dispatcher.DeliveryOrder;
import dev.knative.eventing.kafka.broker.dispatcher.Filter;
import dev.knative.eventing.kafka.broker.dispatcher.ResponseHandler;
import dev.knative.eventing.kafka.broker.dispatcher.impl.NoopResponseHandler;
import dev.knative.eventing.kafka.broker.dispatcher.impl.RecordDispatcherImpl;
import dev.knative.eventing.kafka.broker.dispatcher.impl.RecordDispatcherMutatorChain;
import dev.knative.eventing.kafka.broker.dispatcher.impl.ResponseToHttpEndpointHandler;
import dev.knative.eventing.kafka.broker.dispatcher.impl.ResponseToKafkaTopicHandler;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.CloudEventOverridesMutator;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.ConsumerVerticle;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.OffsetManager;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.OrderedConsumerVerticle;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.PartitionRevokedHandler;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.UnorderedConsumerVerticle;
import dev.knative.eventing.kafka.broker.dispatcher.impl.filter.subscriptionsapi.AllFilter;
import dev.knative.eventing.kafka.broker.dispatcher.impl.filter.subscriptionsapi.AnyFilter;
import dev.knative.eventing.kafka.broker.dispatcher.impl.filter.subscriptionsapi.CeSqlFilter;
import dev.knative.eventing.kafka.broker.dispatcher.impl.filter.subscriptionsapi.ExactFilter;
import dev.knative.eventing.kafka.broker.dispatcher.impl.filter.subscriptionsapi.NotFilter;
import dev.knative.eventing.kafka.broker.dispatcher.impl.filter.subscriptionsapi.PrefixFilter;
import dev.knative.eventing.kafka.broker.dispatcher.impl.filter.subscriptionsapi.SuffixFilter;
import dev.knative.eventing.kafka.broker.dispatcher.impl.http.WebClientCloudEventSender;
import io.cloudevents.CloudEvent;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.ext.web.client.WebClient;
import io.vertx.kafka.client.common.KafkaClientOptions;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.common.tracing.ConsumerTracer;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

public class ConsumerVerticleBuilder {

  private final static CloudEventSender NO_DEAD_LETTER_SINK_SENDER = CloudEventSender.noop("No dead letter sink set");

  private final ConsumerVerticleContext consumerVerticleContext;

  public ConsumerVerticleBuilder(final ConsumerVerticleContext consumerVerticleContext) {
    this.consumerVerticleContext = consumerVerticleContext;
  }

  public ConsumerVerticle build() {
    return createConsumerVerticle(getInitializer());
  }

  private ConsumerVerticle.Initializer getInitializer() {
    return (vertx, consumerVerticle) -> consumerVerticleContext.getAuthProvider()
      .getCredentials(consumerVerticleContext.getResource())
      .onSuccess(credentials -> build(vertx, consumerVerticle, credentials))
      .mapEmpty();
  }

  private void build(final Vertx vertx, final ConsumerVerticle consumerVerticle, final Credentials credentials) {
    KafkaClientsAuth.attachCredentials(consumerVerticleContext.getConsumerConfigs(), credentials);
    KafkaClientsAuth.attachCredentials(consumerVerticleContext.getProducerConfigs(), credentials);

    final KafkaConsumer<Object, CloudEvent> consumer = this.consumerVerticleContext
      .getConsumerFactory()
      .create(vertx, consumerVerticleContext.getConsumerConfigs());
    consumerVerticle.setConsumer(consumer);

    final var metricsCloser = Metrics.register(consumer.unwrap());
    consumerVerticle.setCloser(metricsCloser);

    final var egressSubscriberSender = createConsumerRecordSender(vertx);
    final var egressDeadLetterSender = createDeadLetterSinkRecordSender(vertx);
    final var responseHandler = createResponseHandler(vertx);

    final var offsetManager = new OffsetManager(
      vertx,
      consumer,
      (v) -> {
      },
      getCommitIntervalMs()
    );

    final var recordDispatcher = new RecordDispatcherMutatorChain(
      new RecordDispatcherImpl(
        consumerVerticleContext,
        getFilter(),
        egressSubscriberSender,
        egressDeadLetterSender,
        responseHandler,
        offsetManager,
        ConsumerTracer.create(((VertxInternal) vertx).tracer(),
          new KafkaClientOptions().setConfig(consumerVerticleContext.getConsumerConfigs())
            // Make sure the policy is propagate for the manually instantiated consumer tracer
            .setTracingPolicy(TracingPolicy.PROPAGATE)), Metrics.getRegistry()),

      new CloudEventOverridesMutator(consumerVerticleContext.getResource().getCloudEventOverrides())
    );
    consumerVerticle.setRecordDispatcher(recordDispatcher);

    final var partitionRevokedHandlers = List.of(
      consumerVerticle.getPartitionsRevokedHandler(),
      offsetManager.getPartitionRevokedHandler()
    );
    consumer.partitionsRevokedHandler(handlePartitionsRevoked(partitionRevokedHandlers));
  }

  private ConsumerVerticle createConsumerVerticle(final ConsumerVerticle.Initializer initializer) {
    return switch (DeliveryOrder.fromContract(consumerVerticleContext.getEgress().getDeliveryOrder())) {
      case ORDERED -> new OrderedConsumerVerticle(consumerVerticleContext, initializer);
      case UNORDERED -> new UnorderedConsumerVerticle(consumerVerticleContext, initializer);
    };
  }

  /**
   * For each handler call partitionRevoked and wait for the future to complete.
   *
   * @param partitionRevokedHandlers partition revoked handlers
   * @return a single handler that dispatches the partition revoked event to the given handlers.
   */
  private Handler<Set<TopicPartition>> handlePartitionsRevoked(final List<PartitionRevokedHandler> partitionRevokedHandlers) {
    return partitions -> {

      ConsumerVerticleContext.logger.info("Received revoke partitions for consumer {} {}",
        consumerVerticleContext.getLoggingKeyValue(),
        keyValue("partitions", partitions)
      );

      final var futures = new ArrayList<Future<Void>>(partitionRevokedHandlers.size());
      for (PartitionRevokedHandler partitionRevokedHandler : partitionRevokedHandlers) {
        futures.add(partitionRevokedHandler.partitionRevoked(partitions));
      }

      for (final var future : futures) {
        try {
          future.toCompletionStage().toCompletableFuture().get(1, TimeUnit.SECONDS);
        } catch (final Exception ignored) {
          ConsumerVerticleContext.logger.warn("Partition revoked handler failed {} {}",
            consumerVerticleContext.getLoggingKeyValue(),
            keyValue("partitions", partitions)
          );
        }
      }
    };
  }

  private Filter getFilter() {
    // Dialected filters should override the attributes filter
    if (consumerVerticleContext.getEgress().getDialectedFilterCount() > 0) {
      return getFilter(consumerVerticleContext.getEgress().getDialectedFilterList());
    } else if (consumerVerticleContext.getEgress().hasFilter()) {
      return new ExactFilter(consumerVerticleContext.getEgress().getFilter().getAttributesMap());
    }
    return Filter.noop();
  }

  private static Filter getFilter(List<DataPlaneContract.DialectedFilter> filters) {
    return new AllFilter(filters.stream().map(ConsumerVerticleBuilder::getFilter).collect(Collectors.toSet()));
  }

  private static Filter getFilter(DataPlaneContract.DialectedFilter filter) {
    return switch (filter.getFilterCase()) {
      case EXACT -> new ExactFilter(filter.getExact().getAttributesMap());
      case PREFIX -> new PrefixFilter(filter.getPrefix().getAttributesMap());
      case SUFFIX -> new SuffixFilter(filter.getSuffix().getAttributesMap());
      case NOT -> new NotFilter(getFilter(filter.getNot().getFilter()));
      case ANY -> new AnyFilter(filter.getAny().getFiltersList().stream().map(ConsumerVerticleBuilder::getFilter).collect(Collectors.toSet()));
      case ALL -> new AllFilter(filter.getAll().getFiltersList().stream().map(ConsumerVerticleBuilder::getFilter).collect(Collectors.toSet()));
      case CESQL -> new CeSqlFilter(filter.getCesql().getExpression());
      default -> Filter.noop();
    };
  }

  private ResponseHandler createResponseHandler(final Vertx vertx) {
    if (consumerVerticleContext.getEgress().hasReplyUrl()) {
      return new ResponseToHttpEndpointHandler(new WebClientCloudEventSender(
        vertx,
        WebClient.create(vertx, consumerVerticleContext.getWebClientOptions()),
        consumerVerticleContext.getEgress().getReplyUrl(),
        consumerVerticleContext
      ));
    }

    if (consumerVerticleContext.getEgress().hasDiscardReply()) {
      return new NoopResponseHandler();
    }

    final KafkaProducer<String, CloudEvent> producer = this.consumerVerticleContext
      .getProducerFactory()
      .create(vertx, consumerVerticleContext.getProducerConfigs());
    return new ResponseToKafkaTopicHandler(producer, consumerVerticleContext.getResource().getTopics(0));
  }

  private CloudEventSender createConsumerRecordSender(final Vertx vertx) {
    return new WebClientCloudEventSender(
      vertx,
      WebClient.create(vertx, consumerVerticleContext.getWebClientOptions()),
      consumerVerticleContext.getEgress().getDestination(),
      consumerVerticleContext
    );
  }

  private CloudEventSender createDeadLetterSinkRecordSender(final Vertx vertx) {
    if (hasDeadLetterSink(consumerVerticleContext.getEgressConfig())) {
      var webClientOptions = consumerVerticleContext.getWebClientOptions();
      // TODO use numPartitions for ordered delivery or max.poll.records for unordered delivery
      // if (egress.getVReplicas() > 0) {
      //   webClientOptions = new WebClientOptions(this.webClientOptions);
      //   webClientOptions.setMaxPoolSize(egress.getVReplicas());
      // }
      return new WebClientCloudEventSender(
        vertx,
        WebClient.create(vertx, webClientOptions),
        consumerVerticleContext.getEgressConfig().getDeadLetter(),
        consumerVerticleContext
      );
    }

    return NO_DEAD_LETTER_SINK_SENDER;
  }

  private int getCommitIntervalMs() {
    final var commitInterval = consumerVerticleContext.getConsumerConfigs().get(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG);
    if (commitInterval == null) {
      return 5000;
    }
    return Integer.parseInt(String.valueOf(commitInterval));
  }

  private static boolean hasDeadLetterSink(final DataPlaneContract.EgressConfig egressConfig) {
    return !(egressConfig == null || egressConfig.getDeadLetter().isEmpty());
  }
}
