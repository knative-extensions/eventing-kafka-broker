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
import dev.knative.eventing.kafka.broker.dispatcher.impl.filter.subscriptionsapi.*;
import io.cloudevents.CloudEvent;
import java.util.List;
import java.util.Map;

public class AnyFilterBenchmark {
    public static CloudEvent event() {
        final var returnFirst = Math.random() > 0.5;
        if (returnFirst) {
            return SampleEvent.event();
        } else {
            return SampleEvent.otherEvent();
        }
    }

    public static ExactFilter makeExactFilter() {
        return new ExactFilter(Map.of("type", "com.github.pull.create"));
    }

    public static PrefixFilter makePrefixFilter() {
        return new PrefixFilter(Map.of("type", "com.g"));
    }

    public static SuffixFilter makeSuffixFilter() {
        return new SuffixFilter(Map.of("source", "lhost"));
    }

    public static PrefixFilter makePrefixFilterNoMatch() {
        return new PrefixFilter(Map.of("type", "other.event"));
    }

    public static SuffixFilter makeSuffixFilterNoMatch() {
        return new SuffixFilter(Map.of("source", "qwertyuiop"));
    }

    public static class AnyFilterWithExactFilterBenchmark extends FilterBenchmark {

        @Override
        protected Filter createFilter() {
            return new AnyFilter(List.of(makeExactFilter()), vertx, FILTER_REORDER_TIME_MILLISECONDS);
        }

        @Override
        protected CloudEvent createEvent() {
            return SampleEvent.event();
        }
    }

    public static class AnyFilterMatchAllSubfilters extends FilterBenchmark {

        @Override
        protected Filter createFilter() {
            return new AnyFilter(
                    List.of(makeExactFilter(), makePrefixFilter(), makeSuffixFilter()),
                    vertx,
                    FILTER_REORDER_TIME_MILLISECONDS);
        }

        @Override
        protected CloudEvent createEvent() {
            return SampleEvent.event();
        }
    }

    public static class AnyFilterFirstMatchAtEnd extends FilterBenchmark {

        @Override
        protected Filter createFilter() {
            return new AnyFilter(
                    List.of(makePrefixFilterNoMatch(), makeSuffixFilterNoMatch(), makeExactFilter()),
                    vertx,
                    FILTER_REORDER_TIME_MILLISECONDS);
        }

        @Override
        protected CloudEvent createEvent() {
            return SampleEvent.event();
        }
    }

    public static class AnyFilterFirstMatchAtStart extends FilterBenchmark {

        @Override
        protected Filter createFilter() {
            return new AnyFilter(
                    List.of(makeExactFilter(), makePrefixFilterNoMatch(), makeSuffixFilterNoMatch()),
                    vertx,
                    FILTER_REORDER_TIME_MILLISECONDS);
        }

        @Override
        protected CloudEvent createEvent() {
            return SampleEvent.event();
        }
    }

    public static class AnyFilter2EventsMatch2DifferentFilters extends FilterBenchmark {

        @Override
        protected Filter createFilter() {
            return new AnyFilter(
                    List.of(makePrefixFilter(), makePrefixFilterNoMatch()), vertx, FILTER_REORDER_TIME_MILLISECONDS);
        }

        @Override
        protected CloudEvent createEvent() {
            return event();
        }
    }

    public static class AnyFilter2EventsMatch2DifferentFiltersOneFilterMatchesNeither extends FilterBenchmark {
        @Override
        protected Filter createFilter() {
            return new AnyFilter(
                    List.of(makeSuffixFilterNoMatch(), makePrefixFilter(), makePrefixFilterNoMatch()),
                    vertx,
                    FILTER_REORDER_TIME_MILLISECONDS);
        }

        @Override
        protected CloudEvent createEvent() {
            return event();
        }
    }
}
