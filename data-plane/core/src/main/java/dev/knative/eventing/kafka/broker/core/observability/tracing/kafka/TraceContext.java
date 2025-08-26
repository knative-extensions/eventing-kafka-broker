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
 * https://github.com/vert-x3/vertx-kafka-client/blob/a0e349fca33d3bb4f003ac53e6e0def42a76e8ab/src/main/java/io/vertx/kafka/client/common/tracing/TraceContext.java
 */
package dev.knative.eventing.kafka.broker.core.observability.tracing.kafka;

/**
 * TraceContext holds some context for tracing during a message writing / reading process
 */
class TraceContext {
    final String kind;
    final String address;
    final String hostname;
    final String port;
    final String topic;

    TraceContext(String kind, String address, String hostname, String port, String topic) {
        this.kind = kind;
        this.address = address;
        this.hostname = hostname;
        this.port = port;
        this.topic = topic;
    }
}
