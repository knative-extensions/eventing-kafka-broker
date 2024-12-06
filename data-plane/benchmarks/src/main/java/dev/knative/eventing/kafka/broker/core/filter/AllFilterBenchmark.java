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

import dev.knative.eventing.kafka.broker.core.filter.subscriptionsapi.AllFilter;
import dev.knative.eventing.kafka.broker.core.filter.subscriptionsapi.ExactFilter;
import dev.knative.eventing.kafka.broker.core.filter.subscriptionsapi.PrefixFilter;
import dev.knative.eventing.kafka.broker.core.filter.subscriptionsapi.SuffixFilter;
import io.cloudevents.CloudEvent;
import java.util.List;
import java.util.Map;

public class AllFilterBenchmark {
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

    public static class AllFilterWithExactFilter extends FilterBenchmark {

        @Override
        protected Filter createFilter() {
            return AllFilter.newFilter(List.of(makeExactFilter()));
        }

        @Override
        protected CloudEvent createEvent() {
            return SampleEvent.event();
        }
    }

    public static class AllFilterMatchAllSubFilters extends FilterBenchmark {

        @Override
        protected Filter createFilter() {
            return AllFilter.newFilter(List.of(makeExactFilter(), makePrefixFilter(), makeSuffixFilter()));
        }

        @Override
        protected CloudEvent createEvent() {
            return SampleEvent.event();
        }
    }

    public static class AllFilterFirstMatchEndOfArray extends FilterBenchmark {

        @Override
        protected Filter createFilter() {
            return AllFilter.newFilter(
                    List.of(makePrefixFilterNoMatch(), makeSuffixFilterNoMatch(), makeExactFilter()));
        }

        @Override
        protected CloudEvent createEvent() {
            return SampleEvent.event();
        }
    }

    public static class AllFilterFirstMatchStartOfArray extends FilterBenchmark {

        @Override
        protected Filter createFilter() {
            return AllFilter.newFilter(
                    List.of(makeExactFilter(), makePrefixFilterNoMatch(), makeSuffixFilterNoMatch()));
        }

        @Override
        protected CloudEvent createEvent() {
            return SampleEvent.event();
        }
    }

    public static class AllFilterOneNonMatchingFilterInMiddle extends FilterBenchmark {

        @Override
        protected Filter createFilter() {
            return AllFilter.newFilter(List.of(makeExactFilter(), makePrefixFilterNoMatch(), makePrefixFilter()));
        }

        @Override
        protected CloudEvent createEvent() {
            return SampleEvent.event();
        }
    }

    public static class AllFilterNoMatchingFilters extends FilterBenchmark {

        @Override
        protected Filter createFilter() {
            return AllFilter.newFilter(List.of(makePrefixFilterNoMatch(), makeSuffixFilterNoMatch()));
        }

        @Override
        protected CloudEvent createEvent() {
            return SampleEvent.event();
        }
    }
}
