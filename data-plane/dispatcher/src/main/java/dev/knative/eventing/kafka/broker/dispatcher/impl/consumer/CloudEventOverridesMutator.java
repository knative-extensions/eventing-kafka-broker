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
package dev.knative.eventing.kafka.broker.dispatcher.impl.consumer;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.dispatcher.CloudEventMutator;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * CloudEventOverridesMutator is a {@link CloudEventMutator} that applies a given set of
 * {@link dev.knative.eventing.kafka.broker.contract.DataPlaneContract.CloudEventOverrides}.
 */
public class CloudEventOverridesMutator implements CloudEventMutator {

    private final DataPlaneContract.CloudEventOverrides cloudEventOverrides;

    private static final CloudEventDeserializer cloudEventDeserializer = new CloudEventDeserializer();

    public CloudEventOverridesMutator(final DataPlaneContract.CloudEventOverrides cloudEventOverrides) {
        this.cloudEventOverrides = cloudEventOverrides;
    }

    @Override
    public CloudEvent apply(ConsumerRecord<Object, CloudEvent> record) {
        final var cloudEvent = this.maybeDeserializeFromHeaders(record);
        final var builder = CloudEventBuilder.from(cloudEvent);
        applyKafkaMetadata(builder, record.partition(), record.offset());
        applyCloudEventOverrides(builder);
        return builder.build();
    }

    private CloudEvent maybeDeserializeFromHeaders(ConsumerRecord<Object, CloudEvent> record) {
        if (record.value() != null) {
            return record.value();
        }
        // A valid CloudEvent in the CE binary protocol binding of Kafka
        // might be composed by only Headers.
        //
        // KafkaConsumer doesn't call the deserializer if the value
        // is null.
        //
        // That means that we get a record with a null value and some CE
        // headers even though the record is a valid CloudEvent.
        return cloudEventDeserializer.deserialize(record.topic(), record.headers(), null);
    }

    private void applyCloudEventOverrides(CloudEventBuilder builder) {
        cloudEventOverrides.getExtensionsMap().forEach(builder::withExtension);
    }

    private void applyKafkaMetadata(CloudEventBuilder builder, Number partition, Number offset) {
        builder.withExtension("knativekafkapartition", partition);
        builder.withExtension("knativekafkaoffset", offset);
    }
}
