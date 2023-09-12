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
import dev.knative.eventing.kafka.broker.dispatcher.impl.filter.subscriptionsapi.SuffixFilter;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.v1.CloudEventV1;
import java.util.Map;

public class SuffixFilterBenchmark {
    public static CloudEvent event() {
        return SampleEvent.event();
    }

    public static class SuffixFilterIDBenchmark extends FilterBenchmark {

        @Override
        protected Filter createFilter() {
            return new SuffixFilter(Map.of(CloudEventV1.ID, "lmnop"));
        }

        @Override
        protected CloudEvent createEvent() {
            return event();
        }
    }

    public static class SuffixFilterAllContextAttributes5CharsBenchmark extends FilterBenchmark {

        @Override
        protected Filter createFilter() {
            return new SuffixFilter(Map.of(
                    CloudEventV1.ID, "lmnop",
                    CloudEventV1.SOURCE, "lhost",
                    CloudEventV1.TYPE, "reate",
                    CloudEventV1.DATASCHEMA, "chema",
                    CloudEventV1.DATACONTENTTYPE, "tType",
                    CloudEventV1.SUBJECT, "bject"));
        }

        @Override
        protected CloudEvent createEvent() {
            return event();
        }
    }

    public static class SuffixFilterAllContextAttributes3CharsBenchmark extends FilterBenchmark {

        @Override
        protected Filter createFilter() {
            return new SuffixFilter(Map.of(
                    CloudEventV1.ID, "nop",
                    CloudEventV1.SOURCE, "ost",
                    CloudEventV1.TYPE, "ate",
                    CloudEventV1.DATASCHEMA, "ema",
                    CloudEventV1.DATACONTENTTYPE, "ype",
                    CloudEventV1.SUBJECT, "ect"));
        }

        @Override
        protected CloudEvent createEvent() {
            return event();
        }
    }

    public static class SuffixFilterLongNoBenchmark extends FilterBenchmark {

        @Override
        protected Filter createFilter() {
            return new SuffixFilter(Map.of(
                    CloudEventV1.ID, "qwertyuiopasdfghjklzxcvbnm",
                    CloudEventV1.SOURCE, "qwertyuiopasdfghjklzxcvbnm"));
        }

        @Override
        protected CloudEvent createEvent() {
            return event();
        }
    }

    public static class SuffixFilterMediumNoBenchmark extends FilterBenchmark {

        @Override
        protected Filter createFilter() {
            return new SuffixFilter(Map.of(
                    CloudEventV1.ID, "hjklzxcvbnm",
                    CloudEventV1.SOURCE, "hjklzxcvbnm"));
        }

        @Override
        protected CloudEvent createEvent() {
            return event();
        }
    }

    public static class SuffixFilterShortNoBenchmark extends FilterBenchmark {

        @Override
        protected Filter createFilter() {
            return new SuffixFilter(Map.of(
                    CloudEventV1.ID, "bnm",
                    CloudEventV1.SOURCE, "bnm"));
        }

        @Override
        protected CloudEvent createEvent() {
            return event();
        }
    }
}
