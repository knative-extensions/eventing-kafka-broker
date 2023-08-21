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
import dev.knative.eventing.kafka.broker.dispatcher.impl.filter.subscriptionsapi.PrefixFilter;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.v1.CloudEventV1;

import java.net.URI;
import java.util.Map;

public class PrefixFilterBenchmark {
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

  public class PrefixFilterIDBenchmark extends FilterBenchmark {

    @Override
    protected Filter createFilter() {
      return new PrefixFilter(Map.of(CloudEventV1.ID, "abcde"));
    }

    @Override
    protected CloudEvent createEvent() {
      return event();
    }
  }

  public class PrefixFilterAllContextAttributes5CharsBenchmark extends FilterBenchmark {

    @Override
    protected Filter createFilter() {
      return new PrefixFilter(Map.of(
        CloudEventV1.ID, "abcde",
        CloudEventV1.SOURCE, "http:",
        CloudEventV1.TYPE, "com.g",
        CloudEventV1.DATASCHEMA, "/api/",
        CloudEventV1.DATACONTENTTYPE, "testC",
        CloudEventV1.SUBJECT, "testS"
        ));
    }

    @Override
    protected CloudEvent createEvent() {
      return event();
    }
  }

  public class PrefixFilterAllContextAttributes3CharsBenchmark extends FilterBenchmark {

    @Override
    protected Filter createFilter() {
      return new PrefixFilter(Map.of(
        CloudEventV1.ID, "abc",
        CloudEventV1.SOURCE, "htt",
        CloudEventV1.TYPE, "com",
        CloudEventV1.DATASCHEMA, "/ap",
        CloudEventV1.DATACONTENTTYPE, "tes",
        CloudEventV1.SUBJECT, "tes"
      ));
    }

    @Override
    protected CloudEvent createEvent() {
      return event();
    }
  }

  public class PrefixFilterLongNoBenchmark extends FilterBenchmark {

    @Override
    protected Filter createFilter() {
      return new PrefixFilter(Map.of(
        CloudEventV1.ID, "qwertyuiopasdfghjklzxcvbnm",
        CloudEventV1.SOURCE, "qwertyuiopasdfghjklzxcvbnm"
      ));
    }

    @Override
    protected CloudEvent createEvent() {
      return event();
    }
  }

  public class PrefixFilterMediumNoBenchmark extends FilterBenchmark {

    @Override
    protected Filter createFilter() {
      return new PrefixFilter(Map.of(
        CloudEventV1.ID, "qwertyuiopa",
        CloudEventV1.SOURCE, "qwertyuiopa"
      ));
    }

    @Override
    protected CloudEvent createEvent() {
      return event();
    }
  }

  public class PrefixFilterShortNoBenchmark extends FilterBenchmark {

    @Override
    protected Filter createFilter() {
      return new PrefixFilter(Map.of(
        CloudEventV1.ID, "qwe",
        CloudEventV1.SOURCE, "qwe"
      ));
    }

    @Override
    protected CloudEvent createEvent() {
      return event();
    }
  }
}
