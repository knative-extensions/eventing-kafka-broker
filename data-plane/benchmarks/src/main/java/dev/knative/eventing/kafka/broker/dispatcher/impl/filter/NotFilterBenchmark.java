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

package dev.knative.eventing.kafka.broker.dispatcher.impl.filter;

import dev.knative.eventing.kafka.broker.dispatcher.Filter;
import dev.knative.eventing.kafka.broker.dispatcher.impl.filter.subscriptionsapi.ExactFilter;
import dev.knative.eventing.kafka.broker.dispatcher.impl.filter.subscriptionsapi.NotFilter;
import dev.knative.eventing.kafka.broker.dispatcher.impl.filter.subscriptionsapi.PrefixFilter;
import dev.knative.eventing.kafka.broker.dispatcher.impl.filter.subscriptionsapi.SuffixFilter;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.v1.CloudEventV1;
import java.net.URI;
import java.util.Map;

public class NotFilterBenchmark {
    public static CloudEvent event() {
        return CloudEventBuilder.v1()
                .withId("abcdefghijklmnop")
                .withSource(URI.create("http://localhost"))
                .withType("com.github.pull.create")
                .withDataSchema(URI.create("/api/schema"))
                .withDataContentType("testContentType")
                .withSubject("testSubject")
                .build();
    }

    public class NotFilterWithExactFilterBenchmark extends FilterBenchmark {

        @Override
        protected Filter createFilter() {
            return new NotFilter(new ExactFilter(Map.of(CloudEventV1.ID, "abcdefghijklmnop")));
        }

        @Override
        protected CloudEvent createEvent() {
            return event();
        }
    }

    public class NotFilterWithPrefixFilterBenchmark extends FilterBenchmark {

        @Override
        protected Filter createFilter() {
            return new NotFilter(new PrefixFilter(Map.of(CloudEventV1.TYPE, "com.github")));
        }

        @Override
        protected CloudEvent createEvent() {
            return event();
        }
    }

    public class NotFilterWithSuffixFilterBenchmark extends FilterBenchmark {

        @Override
        protected Filter createFilter() {
            return new NotFilter(new SuffixFilter(Map.of(CloudEventV1.SOURCE, "/localhost")));
        }

        @Override
        protected CloudEvent createEvent() {
            return event();
        }
    }
}
