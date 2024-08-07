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
package dev.knative.eventing.kafka.broker.receiver;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.ReactiveKafkaProducer;
import dev.knative.eventing.kafka.broker.core.eventtype.EventType;
import io.cloudevents.CloudEvent;
import io.fabric8.kubernetes.client.informers.cache.Lister;
import io.vertx.core.Future;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * This interface wraps a {@link ReactiveKafkaProducer} together with the topic where the ingress should produce to.
 */
public interface IngressProducer {

    /**
     * Convenience method for {@link ReactiveKafkaProducer#send(ProducerRecord)}.
     *
     * @see ReactiveKafkaProducer#send(ProducerRecord)
     */
    default Future<RecordMetadata> send(ProducerRecord<String, CloudEvent> record) {
        return getKafkaProducer().send(record);
    }

    /**
     * @return the unwrapped kafka producer.
     */
    ReactiveKafkaProducer<String, CloudEvent> getKafkaProducer();

    /**
     * @return the topic where the record should be sent to.
     */
    String getTopic();

    /**
     * @return the resource associated with this producer.
     */
    DataPlaneContract.Reference getReference();

    /**
     * @return whether event type autocreate is enabled
     */
    default boolean isEventTypeAutocreateEnabled() {
        return false;
    }

    /**
     * @return the Lister for eventtypes in the correct namespace for this producer
     */
    Lister<EventType> getEventTypeLister();

    /**
     * @return the OIDC audience for the ingress.
     */
    String getAudience();
}
