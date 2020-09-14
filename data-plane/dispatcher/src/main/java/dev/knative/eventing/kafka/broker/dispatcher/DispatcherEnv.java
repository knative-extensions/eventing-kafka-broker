package dev.knative.eventing.kafka.broker.dispatcher;

import static java.util.Objects.requireNonNull;

import dev.knative.eventing.kafka.broker.core.utils.BaseEnv;
import java.util.function.Function;

public class DispatcherEnv extends BaseEnv {

  public static final String CONSUMER_CONFIG_FILE_PATH = "CONSUMER_CONFIG_FILE_PATH";
  public static final String WEBCLIENT_CONFIG_FILE_PATH = "WEBCLIENT_CONFIG_FILE_PATH";
  public static final String RESOURCES_INITIAL_CAPACITY = "RESOURCES_INITIAL_CAPACITY";
  public static final String EGRESSES_INITIAL_CAPACITY = "EGRESSES_INITIAL_CAPACITY";

  private final String consumerConfigFilePath;
  private final String webClientConfigFilePath;
  private final int resourcesInitialCapacity;
  private final int egressesInitialCapacity;

  public DispatcherEnv(Function<String, String> envProvider) {
    super(envProvider);

    this.consumerConfigFilePath = requireNonNull(envProvider.apply(CONSUMER_CONFIG_FILE_PATH));
    this.webClientConfigFilePath = requireNonNull(envProvider.apply(WEBCLIENT_CONFIG_FILE_PATH));
    this.resourcesInitialCapacity = Integer.parseInt(requireNonNull(envProvider.apply(RESOURCES_INITIAL_CAPACITY)));
    this.egressesInitialCapacity = Integer.parseInt(requireNonNull(envProvider.apply(EGRESSES_INITIAL_CAPACITY)));
  }

  public String getConsumerConfigFilePath() {
    return consumerConfigFilePath;
  }

  public String getWebClientConfigFilePath() {
    return webClientConfigFilePath;
  }

  public int getResourcesInitialCapacity() {
    return resourcesInitialCapacity;
  }

  public int getEgressesInitialCapacity() {
    return egressesInitialCapacity;
  }

  @Override
  public String toString() {
    return "DispatcherEnv{" +
      "consumerConfigFilePath='" + consumerConfigFilePath + '\'' +
      ", webClientConfigFilePath='" + webClientConfigFilePath + '\'' +
      ", resourcesInitialCapacity=" + resourcesInitialCapacity +
      ", egressesInitialCapacity=" + egressesInitialCapacity +
      "} " + super.toString();
  }
}
