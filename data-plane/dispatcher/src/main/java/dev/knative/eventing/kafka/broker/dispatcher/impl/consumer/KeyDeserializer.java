package dev.knative.eventing.kafka.broker.dispatcher.impl.consumer;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;

/**
 * Deserializer for the key of Kafka records.
 * <p>
 * It uses the configuration value associated with {@link KeyDeserializer.KEY_TYPE} to deserialize the key of a consumer record.
 * <p>
 * Internally, it uses Kafka client provided deserializers for deserialization.
 * <p>
 * - When {@code KEY_TYPE = String} it uses {@link StringDeserializer}.
 * <p>
 * - When {@code KEY_TYPE = Integer} it uses {@link IntegerDeserializer} and falls back to {@link StringDeserializer}
 * if the {@code byte[]} is not 4 bytes long.
 * <p>
 * - When {@code KEY_TYPE = Double} it uses {@link FloatDeserializer} if the key is 4 bytes long,
 * {@link DoubleDeserializer} if the key is 8 bytes long and falls back to {@link StringDeserializer} in any other case.
 * <p>
 * - When {@code KEY_TYPE = ByteArray} it just returns the key.
 * <p>
 * - When {@code KEY_TYPE = UNRECOGNIZED or unspecified} it uses {@link StringDeserializer}.
 */
public class KeyDeserializer implements Deserializer<Object> {

  public static final String KEY_TYPE = "cloudevent.key.deserializer.type";

  private DataPlaneContract.KeyType keyType;

  private FloatDeserializer floatDeserializer;
  private DoubleDeserializer doubleDeserializer;
  private StringDeserializer stringDeserializer;
  private IntegerDeserializer integerDeserializer;

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    if (isKey && configs.containsKey(KEY_TYPE)) {
      final var keyType = configs.get(KEY_TYPE);
      if (keyType != null) {
        this.keyType = (DataPlaneContract.KeyType) keyType;
      }
    }

    floatDeserializer = new FloatDeserializer();
    floatDeserializer.configure(configs, isKey);
    doubleDeserializer = new DoubleDeserializer();
    doubleDeserializer.configure(configs, isKey);
    stringDeserializer = new StringDeserializer();
    stringDeserializer.configure(configs, isKey);
    integerDeserializer = new IntegerDeserializer();
    integerDeserializer.configure(configs, isKey);
  }

  @Override
  public Object deserialize(final String topic, final byte[] data) {
    if (keyType == null) {
      return stringDeserializer.deserialize(topic, data);
    }
    return switch (keyType) {
      case Double -> deserializeFloatingPoint(topic, data);
      case Integer -> deserializeInteger(topic, data);
      case ByteArray -> data;
      default -> stringDeserializer.deserialize(topic, data);
    };
  }

  private Object deserializeInteger(String topic, byte[] data) {
    if (data.length == 4) {
      return integerDeserializer.deserialize(topic, data);
    }
    // Fall back to string deserializer.
    return stringDeserializer.deserialize(topic, data);
  }

  private Object deserializeFloatingPoint(String topic, byte[] data) {
    if (data.length == 4) {
      return floatDeserializer.deserialize(topic, data);
    }
    if (data.length == 8) {
      return doubleDeserializer.deserialize(topic, data);
    }
    // Fall back to string deserializer.
    return stringDeserializer.deserialize(topic, data);
  }
}
