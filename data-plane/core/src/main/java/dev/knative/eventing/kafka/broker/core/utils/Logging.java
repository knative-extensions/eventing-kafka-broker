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
package dev.knative.eventing.kafka.broker.core.utils;

import java.util.function.BiFunction;

public class Logging {

  private static final BiFunction<String, Object, Object> keyValueLoaded = loadKeyValue();

  /**
   * Try to load key value class, otherwise if class not found then fallback to a string appender
   */
  private static BiFunction<String, Object, Object> loadKeyValue() {
    try {
      return net.logstash.logback.argument.StructuredArguments::keyValue;
    } catch (Throwable ignore) { // Class not found!
      return (k, v) -> k + "=" + v;
    }
  }

  public static Object keyValue(String key, Object value) {
    return keyValueLoaded.apply(key, value);
  }

}
