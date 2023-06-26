package dev.knative.eventing.kafka.broker.dispatchervertx;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        dev.knative.eventing.kafka.broker.dispatcher.main.Main.start(args, new VertxConsumerFactory<>());
    }
}
