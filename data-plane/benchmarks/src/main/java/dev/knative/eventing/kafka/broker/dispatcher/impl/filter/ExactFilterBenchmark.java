package dev.knative.eventing.kafka.broker.dispatcher.impl.filter;

import dev.knative.eventing.kafka.broker.dispatcher.Filter;
import dev.knative.eventing.kafka.broker.dispatcher.impl.filter.subscriptionsapi.ExactFilter;
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
        CloudEventV1.DATASCHEMA,"/api/schema",
        CloudEventV1.DATACONTENTTYPE, "testContentType",
        CloudEventV1.SUBJECT, "testSubject"
      ));
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
        CloudEventV1.SOURCE, "qwertyuiopasdfghjklzxcvbnm"
      ));
    }

    @Override
    protected CloudEvent createEvent() {
      return event();
    }
  }
}
