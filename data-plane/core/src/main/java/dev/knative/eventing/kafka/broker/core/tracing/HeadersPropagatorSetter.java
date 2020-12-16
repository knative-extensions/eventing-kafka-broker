/*
 * Copyright 2020 The Knative Authors
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
package dev.knative.eventing.kafka.broker.core.tracing;

import static net.logstash.logback.argument.StructuredArguments.keyValue;

import io.opentelemetry.context.propagation.TextMapPropagator.Setter;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeadersPropagatorSetter implements Setter<BiConsumer<String, String>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(HeadersPropagatorSetter.class);

  @Override
  public void set(
      final BiConsumer<String, String> carrier,
      @Nonnull final String key,
      @Nonnull final String value) {
    if (carrier == null) {
      return;
    }

    LOGGER.debug("Set {} {}", keyValue("key", key), keyValue("value", value));

    carrier.accept(key, value);
  }
}
