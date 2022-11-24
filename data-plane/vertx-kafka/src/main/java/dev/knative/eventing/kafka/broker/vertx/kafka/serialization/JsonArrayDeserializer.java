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

/*
 * Copied from https://github.com/vert-x3/vertx-kafka-client
 *
 * Copyright 2016 Red Hat Inc.
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
package dev.knative.eventing.kafka.broker.vertx.kafka.serialization;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Kafka deserializer for raw bytes in a buffer
 */
public class JsonArrayDeserializer implements Deserializer<JsonArray> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public JsonArray deserialize(String topic, byte[] data) {
    if (data == null)
      return null;

    return Buffer.buffer(data).toJsonArray();
  }

  @Override
  public void close() {
  }
}
