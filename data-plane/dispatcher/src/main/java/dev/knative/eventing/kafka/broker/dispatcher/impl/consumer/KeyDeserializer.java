package dev.knative.eventing.kafka.broker.dispatcher.impl.consumer;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class KeyDeserializer implements Deserializer<Object> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public Object deserialize(String topic, byte[] data) {
    return null;
  }

  @Override
  public Object deserialize(String topic, Headers headers, byte[] data) {
    return null;
  }
}
