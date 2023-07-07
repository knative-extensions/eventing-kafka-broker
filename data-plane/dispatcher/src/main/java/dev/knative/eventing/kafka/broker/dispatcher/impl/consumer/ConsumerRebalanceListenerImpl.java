package dev.knative.eventing.kafka.broker.dispatcher.impl.consumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import dev.knative.eventing.kafka.broker.dispatcher.main.ConsumerVerticleContext;
import io.vertx.core.Future;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

public class ConsumerRebalanceListenerImpl implements ConsumerRebalanceListener {

    private final ConsumerVerticleContext consumerVerticleContext;

    private Collection<PartitionRevokedHandler> partitionRevokedHandlers = new ArrayList<>();

    public ConsumerRebalanceListenerImpl(final ConsumerVerticleContext consumerVerticleContext) {
        this.consumerVerticleContext = consumerVerticleContext;
    }

    void addPartitionRevokedHandler(PartitionRevokedHandler partitionRevokedHandler) {
        this.partitionRevokedHandlers.add(partitionRevokedHandler);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
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
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        ConsumerVerticleContext.logger.info("Received assign partitions for consumer {} {}",
                consumerVerticleContext.getLoggingKeyValue(),
                keyValue("partitions", partitions)
        );
    }
    
}
