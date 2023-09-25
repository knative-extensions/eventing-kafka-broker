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
import java.util.Map;
import java.util.Set;

public class AnyFilterBenchmark {
    private boolean returnFirst = false;

    public CloudEvent event() {
        this.returnFirst = !this.returnFirst;
        if (this.returnFirst) {
            return SampleEvent.event();
        } else {
            return SampleEvent.otherEvent();
        }
    }

    public ExactFilter makeExactFilter() {
        return new ExactFilter(Map.of("id", "com.github.pull.create"));
    }

    public PrefixFilter makePrefixFilter() {
        return new PrefixFilter(Map.of("type", "com.g"));
    }

    public SuffixFilter makeSuffixFilter() {
        return new SuffixFilter(Map.of("source", "lhost"));
    }

    public PrefixFilter makePrefixFilterNoMatch() {
        return new PrefixFilter(Map.of("type", "other.event"));
    }

    public SuffixFilter makeSufficFilterNoMatch() {
        return new SuffixFilter(Map.of("source", "qwertyuiop"));
    }

    public class AnyFilterWithExactFilterBenchmark extends FilterBenchmark {

        @Override
        protected Filter createFilter() {
            return new AnyFilter(Set.of(makeExactFilter()));
        }

        @Override
        protected CloudEvent createEvent() {
            return SampleEvent.event();
        }
    }

    public class AnyFilterMatchAllSubfilters extends FilterBenchmark {

        @Override
        protected Filter createFilter() {
            return new AnyFilter(Set.of(makeExactFilter(), makePrefixFilter(), makeSuffixFilter()));
        }

        @Override
        protected CloudEvent createEvent() {
            return SampleEvent.event();
        }
    }

    public class AnyFilterFirstMatchAtEnd extends FilterBenchmark {

        @Override
        protected Filter createFilter() {
            return new AnyFilter(Set.of(makePrefixFilterNoMatch(), makeSufficFilterNoMatch(), makeExactFilter()));
        }

        @Override
        protected CloudEvent createEvent() {
            return SampleEvent.event();
        }
    }

    public class AnyFilterFirstMatchAtStart extends FilterBenchmark {

        @Override
        protected Filter createFilter() {
            return new AnyFilter(Set.of(makeExactFilter(), makePrefixFilterNoMatch(), makeSufficFilterNoMatch()));
        }

        @Override
        protected CloudEvent createEvent() {
            return SampleEvent.event();
        }
    }

    public class AnyFilter2EventsMatch2DifferentFilters extends FilterBenchmark {

        @Override
        protected Filter createFilter() {
            return new AnyFilter(Set.of(makePrefixFilter(), makePrefixFilterNoMatch()));
        }

        @Override
        protected CloudEvent createEvent() {
            return event();
        }
    }

    public class AnyFilter2EventsMatch2DifferentFiltersOneFilterMatchesNeither extends FilterBenchmark {
        @Override
        protected Filter createFilter() {
            return new AnyFilter(Set.of(makeSufficFilterNoMatch(), makePrefixFilter(), makePrefixFilterNoMatch()));
        }

        @Override
        protected CloudEvent createEvent() {
            return event();
        }
    }
}
