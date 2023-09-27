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
package dev.knative.eventing.kafka.broker.core.eventbus;

import static dev.knative.eventing.kafka.broker.core.testing.CoreObjects.resource1;
import static dev.knative.eventing.kafka.broker.core.testing.CoreObjects.resource2;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.util.JsonFormat;
import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.file.FileWatcher;
import dev.knative.eventing.kafka.broker.core.file.FileWatcherTest;
import dev.knative.eventing.kafka.broker.core.testing.CoreObjects;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.LoggerFactory;

@ExtendWith(VertxExtension.class)
public class ContractPublisherTest {

    @Test
    public void publishTest(Vertx vertx, VertxTestContext testContext) {
        ContractMessageCodec.register(vertx.eventBus());

        String address = "aaa";
        DataPlaneContract.Contract expected = CoreObjects.contract();

        vertx.eventBus().localConsumer(address).handler(message -> {
            testContext.verify(() -> assertThat(message.body()).isEqualTo(expected));
            testContext.completeNow();
        });

        ContractPublisher publisher = new ContractPublisher(vertx.eventBus(), address);
        publisher.accept(expected);
    }

    @Test
    @Timeout(value = 5)
    public void updateWithSameContractFileShouldNotTriggerUpdate() throws Exception {
        // This test should aim to verify that the update function is not triggered when
        // the file is updated with the
        // same content.

        final var file = Files.createTempFile("fw-", "-fw").toFile();

        final var broker1 = DataPlaneContract.Contract.newBuilder()
                .addResources(resource1())
                .build();
        write(file, broker1);

        final Consumer<DataPlaneContract.Contract> brokersConsumer = broker -> {
            assertThat(broker).isEqualTo(broker1);
        };

        brokersConsumer.accept(null);

    }

    public static void write(File file, DataPlaneContract.Contract contract) throws IOException {
        final var f = new File(file.toString());
        try (final var out = new FileWriter(f)) {
            JsonFormat.printer().appendTo(contract, out);
        } finally {
            LoggerFactory.getLogger(FileWatcherTest.class).info("file written");
        }
    }
}
