package dev.knative.eventing.kafka.broker.dispatcher;

import static java.util.Objects.requireNonNull;

import dev.knative.eventing.kafka.broker.core.utils.BaseEnv;
import java.util.function.Function;

public class DispatcherEnv extends BaseEnv {

  public static final String CONSUMER_CONFIG_FILE_PATH = "CONSUMER_CONFIG_FILE_PATH";
  public static final String WEBCLIENT_CONFIG_FILE_PATH = "WEBCLIENT_CONFIG_FILE_PATH";
  public static final String BROKERS_INITIAL_CAPACITY = "BROKERS_INITIAL_CAPACITY";
  public static final String TRIGGERS_INITIAL_CAPACITY = "TRIGGERS_INITIAL_CAPACITY";

  private final String consumerConfigFilePath;
  private final String webClientConfigFilePath;
  private final int brokersInitialCapacity;
  private final int triggersInitialCapacity;

  public DispatcherEnv(Function<String, String> envProvider) {
    super(envProvider);

    this.consumerConfigFilePath = requireNonNull(envProvider.apply(CONSUMER_CONFIG_FILE_PATH));
    this.webClientConfigFilePath = requireNonNull(envProvider.apply(WEBCLIENT_CONFIG_FILE_PATH));
    this.brokersInitialCapacity = Integer.parseInt(requireNonNull(envProvider.apply(BROKERS_INITIAL_CAPACITY)));
    this.triggersInitialCapacity = Integer.parseInt(requireNonNull(envProvider.apply(TRIGGERS_INITIAL_CAPACITY)));
  }

  public String getConsumerConfigFilePath() {
    return consumerConfigFilePath;
  }

  public String getWebClientConfigFilePath() {
    return webClientConfigFilePath;
  }

  public int getBrokersInitialCapacity() {
    return brokersInitialCapacity;
  }

  public int getTriggersInitialCapacity() {
    return triggersInitialCapacity;
  }

  @Override
  public String toString() {
    return "DispatcherEnv{" +
      "consumerConfigFilePath='" + consumerConfigFilePath + '\'' +
      ", webClientConfigFilePath='" + webClientConfigFilePath + '\'' +
      ", brokersInitialCapacity=" + brokersInitialCapacity +
      ", triggersInitialCapacity=" + triggersInitialCapacity +
      "} " + super.toString();
  }
}
