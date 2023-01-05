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
import io.vertx.core.json.JsonObject;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

/**
 * @author <a href="mailto:matzew@apache.org">Matthias Wessendorf</a>
 */
public class VertxSerdes extends Serdes {

  static public Serde<Buffer> Buffer() {
    return new BufferSerde();
  }

  static public Serde<JsonArray> JsonArray() {
    return new JsonArraySerde();
  }

  static public Serde<JsonObject> JsonObject() {
    return new JsonObjectSerde();
  }


  static public final class BufferSerde extends WrapperSerde<Buffer> {
    public BufferSerde() {
      super(new BufferSerializer(), new BufferDeserializer());
    }
  }

  static public final class JsonArraySerde extends WrapperSerde<JsonArray> {
    public JsonArraySerde() {
      super(new JsonArraySerializer(), new JsonArrayDeserializer());
    }
  }

  static public final class JsonObjectSerde extends WrapperSerde<JsonObject> {
    public JsonObjectSerde() {
      super(new JsonObjectSerializer(), new JsonObjectDeserializer());
    }
  }

  static public <T> Serde<T> serdeFrom(Class<T> type) {
    if (Buffer.class.isAssignableFrom(type)) {
      return (Serde<T>) Buffer();
    }

    if (JsonArray.class.isAssignableFrom(type)) {
      return (Serde<T>) JsonArray();
    }

    if (JsonObject.class.isAssignableFrom(type)) {
      return (Serde<T>) JsonObject();
    }

    // delegate to look up default Kafka SerDes:
    return Serdes.serdeFrom(type);
  }

}
