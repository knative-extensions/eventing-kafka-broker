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

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Parser;
import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This object publishes all consumed contracts to the event bus.
 * This class requires the codec {@link ContractMessageCodec} registered in the provided event bus.
 */
public class ContractPublisher implements Consumer<DataPlaneContract.Contract>, AutoCloseable {

    private static final DeliveryOptions DELIVERY_OPTIONS = new DeliveryOptions().setLocalOnly(true);

    private static final Logger logger = LoggerFactory.getLogger(ContractPublisher.class);

    private final EventBus eventBus;
    private final String address;

    private final Parser parser = JsonFormat.parser().ignoringUnknownFields();

    private long lastContract;

    public ContractPublisher(EventBus eventBus, String address) {
        this.eventBus = eventBus;
        this.address = address;
        this.lastContract = -1;
    }

    @Override
    public void accept(DataPlaneContract.Contract contract) {
        eventBus.publish(address, contract, DELIVERY_OPTIONS);
    }

    @Override
    public void close() throws Exception {
        this.accept(DataPlaneContract.Contract.newBuilder().build());
    }

    public void updateContract(File newContract) {
        if (Thread.interrupted()) {
            return;
        }
        try (final var fileReader = new FileReader(newContract);
                final var bufferedReader = new BufferedReader(fileReader)) {
            final var contract = this.parseFromJson(bufferedReader);
            if (contract == null) {
                return;
            }
            // The check, which is based only on the generation number, works because the control plane doesn't update
            // the file if nothing changes.
            final var previousLastContract = this.lastContract;
            this.lastContract = contract.getGeneration();
            if (contract.getGeneration() == previousLastContract) {
                logger.debug(
                        "Contract unchanged {} {}",
                        keyValue("generation", contract.getGeneration()),
                        keyValue("lastGeneration", previousLastContract));
                return;
            }
            this.accept(contract);
        } catch (IOException e) {
            logger.warn("Error reading the contract file, retrying...", e);
        }
    }

    public DataPlaneContract.Contract parseFromJson(final Reader content) throws IOException {
        try {
            final var contract = DataPlaneContract.Contract.newBuilder();
            this.parser.merge(content, contract);
            return contract.build();
        } catch (final InvalidProtocolBufferException ex) {
            logger.debug("failed to parse from JSON", ex);
        }
        return null;
    }
}
