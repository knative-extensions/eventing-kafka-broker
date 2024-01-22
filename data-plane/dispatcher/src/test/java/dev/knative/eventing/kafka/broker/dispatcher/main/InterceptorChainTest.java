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

import static org.assertj.core.api.Assertions.assertThat;

import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.CloudEventDeserializer;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.InvalidCloudEvent;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.InvalidCloudEventInterceptor;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.NullCloudEventInterceptor;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.BytesCloudEventData;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

public class InterceptorChainTest {
    public static class InterceptorChain {
        InvalidCloudEventInterceptor invalidCloudEventInterceptor;
        NullCloudEventInterceptor nullCloudEventInterceptor;

        Map<String, String> configs;

        public InterceptorChain() {
            this.invalidCloudEventInterceptor = new InvalidCloudEventInterceptor();
            this.nullCloudEventInterceptor = new NullCloudEventInterceptor();

            this.configs = new HashMap<>();
            withKindPlural(configs);
            withSourceName(configs);
            withSourceNamespace(configs);
            withEnabled(configs);

            this.invalidCloudEventInterceptor.configure(this.configs);
        }

        public Map<String, String> getConfigs() {
            return this.configs;
        }

        public ConsumerRecords<Object, CloudEvent> onConsume(final ConsumerRecords<Object, CloudEvent> records) {
            return this.invalidCloudEventInterceptor.onConsume(this.nullCloudEventInterceptor.onConsume(records));
        }

        private static void withKindPlural(final Map<String, String> configs) {
            with(configs, InvalidCloudEventInterceptor.KIND_PLURAL_CONFIG, "kafkasources");
        }

        private static void withSourceName(final Map<String, String> configs) {
            with(configs, InvalidCloudEventInterceptor.SOURCE_NAME_CONFIG, "ks");
        }

        private static void withSourceNamespace(final Map<String, String> configs) {
            with(configs, InvalidCloudEventInterceptor.SOURCE_NAMESPACE_CONFIG, "knative-ns");
        }

        private static void withEnabled(final Map<String, String> configs) {
            with(configs, CloudEventDeserializer.INVALID_CE_WRAPPER_ENABLED, "true");
        }

        private static <T> void with(final Map<String, T> configs, final String key, final T p) {
            configs.put(key, p);
        }
    }

    @Test
    public void shouldTransformEventsToValidEventsHandlingNullValues() {
        var interceptor = new InterceptorChain();

        var input = mockInvalidRecords();

        var configs = interceptor.getConfigs();

        var got = interceptor.onConsume(input);

        for (final var r : input) {
            var tp = new TopicPartition(r.topic(), r.partition());
            var inputRecords = input.records(tp);
            var expected = new ArrayList<ConsumerRecord<Object, CloudEvent>>();
            for (var i : inputRecords) {
                var value = CloudEventBuilder.v1()
                        .withId(String.format("partition:%d/offset:%d", i.partition(), i.offset()))
                        .withTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(i.timestamp()), ZoneId.of("UTC")))
                        .withSource(URI.create(String.format(
                                "/apis/v1/namespaces/%s/%s/%s#%s",
                                configs.get(InvalidCloudEventInterceptor.SOURCE_NAMESPACE_CONFIG),
                                configs.get(InvalidCloudEventInterceptor.KIND_PLURAL_CONFIG),
                                configs.get(InvalidCloudEventInterceptor.SOURCE_NAME_CONFIG),
                                i.topic())))
                        .withSubject(String.format("partition:%d#%d", i.partition(), i.offset()))
                        .withType("dev.knative.kafka.event");

                if (i.value() != null) {
                    value.withData(BytesCloudEventData.wrap(new byte[] {1}));
                }

                i.headers()
                        .forEach(h -> value.withExtension(
                                "kafkaheader" + h.key(), new String(h.value(), StandardCharsets.UTF_8)));

                setKeys(value, i);

                expected.add(new ConsumerRecord<>(
                        i.topic(),
                        i.partition(),
                        i.offset(),
                        i.timestamp(),
                        i.timestampType(),
                        i.serializedKeySize(),
                        i.serializedValueSize(),
                        i.key(),
                        value.build(),
                        i.headers(),
                        i.leaderEpoch()));
            }

            var pairs = zip(expected, got.records(tp));
            for (var p : pairs) {
                assertConsumerRecordEquals(p.getKey(), p.getValue());
            }
        }
    }

    @Test
    public void shouldTransformNullEventsToValidEvents() {
        var interceptor = new InterceptorChain();

        var input = mockValidRecords();

        var configs = interceptor.getConfigs();

        var got = interceptor.onConsume(input);

        for (final var r : input) {
            var tp = new TopicPartition(r.topic(), r.partition());
            var inputRecords = input.records(tp);
            var expected = new ArrayList<ConsumerRecord<Object, CloudEvent>>();
            for (var i : inputRecords) {
                var value = i.value();

                if (value == null) {
                    var builder = CloudEventBuilder.v1()
                            .withId(String.format("partition:%d/offset:%d", i.partition(), i.offset()))
                            .withTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(i.timestamp()), ZoneId.of("UTC")))
                            .withSource(URI.create(String.format(
                                    "/apis/v1/namespaces/%s/%s/%s#%s",
                                    configs.get(InvalidCloudEventInterceptor.SOURCE_NAMESPACE_CONFIG),
                                    configs.get(InvalidCloudEventInterceptor.KIND_PLURAL_CONFIG),
                                    configs.get(InvalidCloudEventInterceptor.SOURCE_NAME_CONFIG),
                                    i.topic())))
                            .withSubject(String.format("partition:%d#%d", i.partition(), i.offset()))
                            .withType("dev.knative.kafka.event");

                    setKeys(builder, i);

                    value = builder.build();
                }

                expected.add(new ConsumerRecord<>(
                        i.topic(),
                        i.partition(),
                        i.offset(),
                        i.timestamp(),
                        i.timestampType(),
                        i.serializedKeySize(),
                        i.serializedValueSize(),
                        i.key(),
                        value,
                        i.headers(),
                        i.leaderEpoch()));
            }

            var pairs = zip(expected, got.records(tp));
            for (var p : pairs) {
                assertConsumerRecordEquals(p.getKey(), p.getValue());
            }
        }
    }

    private void assertConsumerRecordEquals(
            final ConsumerRecord<Object, CloudEvent> actual, final ConsumerRecord<Object, CloudEvent> expected) {
        assertThat(actual.topic()).isEqualTo(expected.topic());
        assertThat(actual.partition()).isEqualTo(expected.partition());
        assertThat(actual.offset()).isEqualTo(expected.offset());
        assertThat(actual.key()).isEqualTo(expected.key());
        assertThat(actual.value()).isEqualTo(expected.value());
        assertThat(actual.serializedKeySize()).isEqualTo(expected.serializedKeySize());
        assertThat(actual.timestamp()).isEqualTo(expected.timestamp());
        assertThat(actual.timestampType()).isEqualTo(expected.timestampType());
        assertThat(actual.headers()).isEqualTo(expected.headers());
    }

    private void setKeys(io.cloudevents.core.v1.CloudEventBuilder value, ConsumerRecord<Object, CloudEvent> i) {
        if (i.key() instanceof Number) {
            value.withExtension("partitionkey", (Number) i.key());
            value.withExtension("key", (Number) i.key());
        } else if (i.key() instanceof String) {
            value.withExtension("partitionkey", i.key().toString());
            value.withExtension("key", i.key().toString());
        } else if (i.key() instanceof byte[]) {
            value.withExtension("partitionkey", (byte[]) i.key());
            value.withExtension("key", (byte[]) i.key());
        } else if (i.key() != null) {
            throw new IllegalArgumentException("unknown type for key: " + i.key());
        }
    }

    public static <A, B> List<Map.Entry<A, B>> zip(List<A> as, List<B> bs) {
        if (as.size() != bs.size()) {
            throw new IllegalArgumentException("List must have the same length");
        }
        return IntStream.range(0, as.size())
                .mapToObj(i -> Map.entry(as.get(i), bs.get(i)))
                .collect(Collectors.toList());
    }

    private static ConsumerRecords<Object, CloudEvent> mockInvalidRecords() {
        return new ConsumerRecords<>(Map.of(
                new TopicPartition("t1", 0),
                List.of(
                        new ConsumerRecord<>("t1", 0, 0, "a", new InvalidCloudEvent(new byte[] {1})),
                        new ConsumerRecord<>("t1", 0, 1, "a", null))));
    }

    private static ConsumerRecords<Object, CloudEvent> mockValidRecords() {
        return new ConsumerRecords<>(Map.of(
                new TopicPartition("t1", 0),
                List.of(
                        new ConsumerRecord<>(
                                "t1",
                                0,
                                0,
                                "a",
                                CloudEventBuilder.v1()
                                        .withId("1")
                                        .withType("example.event.type")
                                        .withSource(URI.create("localhost"))
                                        .build()),
                        new ConsumerRecord<>("t1", 0, 1, "a", null))));
    }
}
