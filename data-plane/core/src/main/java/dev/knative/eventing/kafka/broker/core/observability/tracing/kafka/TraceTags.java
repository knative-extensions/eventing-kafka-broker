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

/**
 * This piece of code is inspired from vert-x3/vertx-kafka-client project.
 * The original source code can be found here:
 * https://github.com/vert-x3/vertx-kafka-client/blob/a0e349fca33d3bb4f003ac53e6e0def42a76e8ab/src/main/java/io/vertx/kafka/client/common/tracing/TraceTags.java
 */
package dev.knative.eventing.kafka.broker.core.observability.tracing.kafka;

import io.vertx.core.spi.tracing.TagExtractor;
import java.util.function.Function;

/**
 * Tags for Kafka Tracing
 */
public enum TraceTags {
    // See https://github.com/opentracing/specification/blob/master/semantic_conventions.md
    PEER_ADDRESS("peer.address", q -> q.address),
    PEER_HOSTNAME("peer.hostname", q -> q.hostname),
    PEER_PORT("peer.port", q -> q.port),
    PEER_SERVICE("peer.service", q -> "kafka"),
    BUS_DESTINATION("message_bus.destination", q -> q.topic);

    static final TagExtractor<TraceContext> TAG_EXTRACTOR = new TagExtractor<TraceContext>() {
        private final TraceTags[] TAGS = TraceTags.values();

        @Override
        public int len(TraceContext obj) {
            return TAGS.length;
        }

        @Override
        public String name(TraceContext obj, int index) {
            return TAGS[index].name;
        }

        @Override
        public String value(TraceContext obj, int index) {
            return TAGS[index].fn.apply(obj);
        }
    };

    final String name;
    final Function<TraceContext, String> fn;

    TraceTags(String name, Function<TraceContext, String> fn) {
        this.name = name;
        this.fn = fn;
    }
}
