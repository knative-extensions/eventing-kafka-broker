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
package dev.knative.eventing.kafka.broker.core.file;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Path;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Objects;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

/**
 * FileWatcher is the class responsible for watching a given file and reports update.
 */
public class FileWatcher implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(FileWatcher.class);

  private final Consumer<DataPlaneContract.Contract> contractConsumer;

  private final WatchService watcher;
  private final File toWatch;
  private long lastContract;

  /**
   * All args constructor.
   *
   * @param watcher          watch service
   * @param contractConsumer updates receiver.
   * @param file             file to watch
   * @throws IOException watch service cannot be registered.
   */
  public FileWatcher(
    final WatchService watcher,
    final Consumer<DataPlaneContract.Contract> contractConsumer,
    final File file)
    throws IOException {

    Objects.requireNonNull(contractConsumer, "provide consumer");
    Objects.requireNonNull(file, "provide file");

    // register the given watch service.
    // Note: this watch a directory and not the single file we're interested in, so that's the
    // reason in #watch() we filter watch service events based on the updated file.

    this.contractConsumer = contractConsumer;
    toWatch = file.getAbsoluteFile();
    logger.info("start watching {}", toWatch);

    Path toWatchParentPath = file.getParentFile().toPath();

    this.watcher = watcher;

    toWatchParentPath.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
    this.lastContract = -1;
  }

  @Override
  public void run() {
    // If the container restarts, the mounted file never gets reconciled, so update as soon as we
    // start watching
    update();

    while (!Thread.interrupted()) {
      var shouldUpdate = false;

      // Note: take() blocks
      WatchKey key;
      try {
        key = watcher.take();
        logger.debug("Contract updates");
      } catch (InterruptedException e) {
        logger.error("Fatal error while watching, closing the watch thread", e);
        break;
      }

      // this should be rare but it can actually happen so check watch key validity
      if (!key.isValid()) {
        logger.warn("Invalid key");
        continue;
      }

      // loop through all watch service events and determine if an update we're interested in
      // has occurred.
      for (final var event : key.pollEvents()) {

        final var kind = event.kind();

        // check if we're interested in the updated file
        if (kind != OVERFLOW) {
          shouldUpdate = true;
          break;
        }

      }

      if (shouldUpdate) {
        update();
      }

      // reset the watch key, so that we receives new events
      key.reset();
    }

    // Close the watcher
    try {
      this.watcher.close();
    } catch (IOException e) {
      logger.warn("Error while closing the file watcher", e);
    }
  }

  private void update() {
    if (Thread.interrupted()) {
      return;
    }
    try (
      final var fileReader = new FileReader(toWatch);
      final var bufferedReader = new BufferedReader(fileReader)) {
      final var contract = parseFromJson(bufferedReader);
      if (contract == null) {
        return;
      }
      // The check, which is based only on the generation number, works because the control plane doesn't update the
      // file if nothing changes.
      final var previousLastContract = this.lastContract;
      this.lastContract = contract.getGeneration();
      if (contract.getGeneration() == previousLastContract) {
        logger.debug("Contract unchanged {}",
          keyValue("generation", contract.getGeneration())
        );
        return;
      }
      contractConsumer.accept(contract);
    } catch (IOException e) {
      logger.warn("Error reading the contract file, retrying...", e);
    }
  }

  private DataPlaneContract.Contract parseFromJson(final Reader content) throws IOException {
    try {

      final var contract = DataPlaneContract.Contract.newBuilder();
      JsonFormat.parser().merge(content, contract);

      return contract.build();

    } catch (final InvalidProtocolBufferException ex) {
      logger.debug("failed to parse from JSON", ex);
    }
    return null;
  }
}
