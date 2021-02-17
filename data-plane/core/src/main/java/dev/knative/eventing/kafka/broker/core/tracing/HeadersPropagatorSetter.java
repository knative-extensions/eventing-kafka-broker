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
package dev.knative.eventing.kafka.broker.core.tracing;

import io.opentelemetry.context.propagation.TextMapPropagator.Setter;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

public class HeadersPropagatorSetter implements Setter<BiConsumer<String, String>> {

  private static final Logger logger = LoggerFactory.getLogger(HeadersPropagatorSetter.class);

  @Override
  public void set(final BiConsumer<String, String> carrier, @Nonnull final String key, @Nonnull final String value) {
    if (carrier == null) {
      return;
    }

    logger.debug("Set {} {}", keyValue("key", key), keyValue("value", value));

    carrier.accept(key, value);
  }
}
