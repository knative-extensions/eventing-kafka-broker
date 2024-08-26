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

package dev.knative.eventing.kafka.broker.core.filter;

import dev.knative.eventing.kafka.broker.core.filter.subscriptionsapi.ExactFilter;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.v1.CloudEventV1;
import java.net.URI;
import java.util.Map;

public class ExactFilterBenchmark {

    public static CloudEvent event() {
        return CloudEventBuilder.v1()
                .withId("abc")
                .withSource(URI.create("http://localhost"))
                .withType("test")
                .withDataSchema(URI.create("/api/schema"))
                .withDataContentType("testContentType")
                .withSubject("testSubject")
                .build();
    }

    public static class ExactFilterBenchmarkExactMatchID extends FilterBenchmark {

        @Override
        protected Filter createFilter() {
            return new ExactFilter(Map.of(CloudEventV1.ID, "abc"));
        }

        @Override
        protected CloudEvent createEvent() {
            return event();
        }
    }

    public static class ExactFilterBenchmarkAllAttributes extends FilterBenchmark {

        @Override
        protected Filter createFilter() {
            return new ExactFilter(Map.of(
                    CloudEventV1.ID, "abc",
                    CloudEventV1.SOURCE, "http://localhost",
                    CloudEventV1.TYPE, "test",
                    CloudEventV1.DATASCHEMA, "/api/schema",
                    CloudEventV1.DATACONTENTTYPE, "testContentType",
                    CloudEventV1.SUBJECT, "testSubject"));
        }

        @Override
        protected CloudEvent createEvent() {
            return event();
        }
    }

    public static class ExactFilterBenchmarkNoMatch extends FilterBenchmark {

        @Override
        protected Filter createFilter() {
            return new ExactFilter(Map.of(
                    CloudEventV1.ID, "qwertyuiopasdfghjklzxcvbnm",
                    CloudEventV1.SOURCE, "qwertyuiopasdfghjklzxcvbnm"));
        }

        @Override
        protected CloudEvent createEvent() {
            return event();
        }
    }
}
