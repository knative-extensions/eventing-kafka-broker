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
package dev.knative.eventing.kafka.broker.dispatcher.impl.consumer;

import com.google.common.base.Charsets;
import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.KeyDeserializer.KEY_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

public class KeyDeserializerTest {

  @Test
  public void shouldDeserializeAsUTF8StringWhenNoKeyTypeSpecified() {
    final var deserializer = new KeyDeserializer();

    final var configs = new HashMap<String, String>();
    deserializer.configure(configs, true);

    final var data = new byte[]{1, 2, 3};
    final var got = deserializer.deserialize("t1", data);

    assertThat(got).isInstanceOf(String.class);
    assertThat(got.toString()).isEqualTo(new String(data, Charsets.UTF_8));
  }

  @Test
  public void shouldDeserializeAsUTF8StringWhenUnrecognizedKeyTypeSpecified() {
    final var deserializer = new KeyDeserializer();

    final var configs = new HashMap<String, Object>();
    configs.put(KEY_TYPE, DataPlaneContract.KeyType.UNRECOGNIZED);
    deserializer.configure(configs, true);

    final var data = new byte[]{1, 2, 3};
    final var got = deserializer.deserialize("t1", data);

    assertThat(got).isInstanceOf(String.class);
    assertThat(got).isEqualTo(new String(data, Charsets.UTF_8));
  }

  @Test
  public void shouldDeserializeAsUTF8StringWhenStringKeyTypeSpecified() {
    final var deserializer = new KeyDeserializer();

    final var configs = new HashMap<String, Object>();
    configs.put(KEY_TYPE, DataPlaneContract.KeyType.String);
    deserializer.configure(configs, true);

    final var data = new byte[]{1, 2, 3};
    final var got = deserializer.deserialize("t1", data);

    assertThat(got).isInstanceOf(String.class);
    assertThat(got).isEqualTo(new String(data, Charsets.UTF_8));
  }

  @Test
  public void shouldDeserializeDouble() {
    final var deserializer = new KeyDeserializer();

    final var configs = new HashMap<String, Object>();
    configs.put(KEY_TYPE, DataPlaneContract.KeyType.Double);
    deserializer.configure(configs, true);

    final var data = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
    final var got = deserializer.deserialize("t1", data);

    assertThat(got).isInstanceOf(Double.class);
    assertThat(got).isEqualTo(new DoubleDeserializer().deserialize("t1", data));
  }

  @Test
  public void shouldDeserializeFloat() {
    final var deserializer = new KeyDeserializer();

    final var configs = new HashMap<String, Object>();
    configs.put(KEY_TYPE, DataPlaneContract.KeyType.Double);
    deserializer.configure(configs, true);

    final var data = new byte[]{1, 2, 3, 4};
    final var got = deserializer.deserialize("t1", data);

    assertThat(got).isInstanceOf(Float.class);
    assertThat(got).isEqualTo(new FloatDeserializer().deserialize("t1", data));
  }

  @Test
  public void shouldDeserializeAsUTF8StringWhenWrongSizeForDouble() {
    final var deserializer = new KeyDeserializer();

    final var configs = new HashMap<String, Object>();
    configs.put(KEY_TYPE, DataPlaneContract.KeyType.Double);
    deserializer.configure(configs, true);

    final var data = new byte[]{1, 2, 3, 4, 5, 6, 7};
    final var got = deserializer.deserialize("t1", data);

    assertThat(got).isInstanceOf(String.class);
    assertThat(got).isEqualTo(new StringDeserializer().deserialize("t1", data));
  }


  @Test
  public void shouldDeserializeInteger() {
    final var deserializer = new KeyDeserializer();

    final var configs = new HashMap<String, Object>();
    configs.put(KEY_TYPE, DataPlaneContract.KeyType.Integer);
    deserializer.configure(configs, true);

    final var data = new byte[]{1, 2, 3, 4};
    final var got = deserializer.deserialize("t1", data);

    assertThat(got).isInstanceOf(Integer.class);
    assertThat(got).isEqualTo(new IntegerDeserializer().deserialize("t1", data));
  }

  @Test
  public void shouldDeserializeAsUTF8StringWhenWrongSizeForInteger() {
    final var deserializer = new KeyDeserializer();

    final var configs = new HashMap<String, Object>();
    configs.put(KEY_TYPE, DataPlaneContract.KeyType.Integer);
    deserializer.configure(configs, true);

    final var data = new byte[]{1, 2, 3};
    final var got = deserializer.deserialize("t1", data);

    assertThat(got).isInstanceOf(String.class);
    assertThat(got).isEqualTo(new StringDeserializer().deserialize("t1", data));
  }


  @Test
  public void shouldDeserializeByteArray() {
    final var deserializer = new KeyDeserializer();

    final var configs = new HashMap<String, Object>();
    configs.put(KEY_TYPE, DataPlaneContract.KeyType.ByteArray);
    deserializer.configure(configs, true);

    final var data = new byte[]{1, 2, 3, 4};
    final var got = deserializer.deserialize("t1", data);

    assertThat(got).isInstanceOf(byte[].class);
    assertThat(got).isEqualTo(data);
  }
}
