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

import io.cloudevents.CloudEvent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NullCloudEventInterceptor implements ConsumerInterceptor<Object, CloudEvent> {

    private static final CloudEventDeserializer cloudEventDeserializer = new CloudEventDeserializer();

    private static final Logger logger = LoggerFactory.getLogger(NullCloudEventInterceptor.class);

    @Override
    public ConsumerRecords<Object, CloudEvent> onConsume(ConsumerRecords<Object, CloudEvent> records) {
        if (records.isEmpty()) {
            return records;
        }
        final Map<TopicPartition, List<ConsumerRecord<Object, CloudEvent>>> validRecords =
                new HashMap<>(records.count());
        for (final var record : records) {
            final var tp = new TopicPartition(record.topic(), record.partition());
            if (!validRecords.containsKey(tp)) {
                validRecords.put(tp, new ArrayList<>());
            }
            validRecords.get(tp).add(maybeDeserializeRecord(record));
        }

        return new ConsumerRecords<>(validRecords);
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {
        // Intentionally left blank
    }

    @Override
    public void close() {
        // Intentionally left blank
    }

    @Override
    public void configure(Map<String, ?> map) {
        logger.info("NullCloudEventInterceptor configured");
    }

    private ConsumerRecord<Object, CloudEvent> maybeDeserializeRecord(ConsumerRecord<Object, CloudEvent> record) {
        if (record.value() != null) {
            return record;
        }
        // A valid CloudEvent in the CE binary protocol binding of Kafka
        // might be composed by only Headers.
        //
        // KafkaConsumer doesn't call the deserializer if the value
        // is null.
        //
        // That means that we get a record with a null value and some CE
        // headers even though the record is a valid CloudEvent.
        logger.debug("deserializing null record");
        return KafkaConsumerRecordUtils.copyRecordAssigningValue(
                record, cloudEventDeserializer.deserialize(record.topic(), record.headers(), null));
    }
}
