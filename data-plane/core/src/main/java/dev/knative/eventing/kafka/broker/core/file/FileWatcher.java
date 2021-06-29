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
import java.nio.file.FileSystems;
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
 * This class is responsible for watching a given file and reports update.
 * <p>
 * Using {@link #start()}, this class will create a background thread running the file watcher.
 * You can interrupt such thread with {@link #close()}
 * <p>
 * This class is thread safe, and it cannot start more than one watch at the time.
 */
public class FileWatcher implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(FileWatcher.class);

  private final File toWatch;
  private final Consumer<DataPlaneContract.Contract> contractConsumer;

  private Thread watcherThread;
  private WatchService watcher;
  private long lastContract;

  /**
   * All args constructor.
   *
   * @param contractConsumer updates receiver.
   * @param file             file to watch
   */
  public FileWatcher(File file, Consumer<DataPlaneContract.Contract> contractConsumer) {
    Objects.requireNonNull(file, "provide file");
    Objects.requireNonNull(contractConsumer, "provide consumer");

    this.contractConsumer = contractConsumer;
    this.toWatch = file.getAbsoluteFile();
    this.lastContract = -1;
  }

  /**
   * Start the watcher thread.
   * This is going to create a new deamon thread, which can be stopped using {@link #close()}.
   *
   * @throws IOException           if an error happened while starting to watch
   * @throws IllegalStateException if the watcher is already running
   */
  public void start() throws IOException {
    synchronized (this) {
      if (this.watcherThread != null) {
        throw new IllegalStateException("Watcher thread is already up and running");
      }
      // Start the watcher thread
      this.watcherThread = new Thread(null, this::run, "contract-file-watcher");

      // Start watching
      this.watcher = FileSystems.getDefault().newWatchService();
      toWatch.getParentFile().toPath().register(this.watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
    }

    this.watcherThread.start();
  }

  @Override
  public synchronized void close() throws Exception {
    if (this.watcherThread == null) {
      throw new IllegalStateException("Watcher thread is not running");
    }
    this.watcherThread.interrupt();
    this.watcherThread = null;
  }

  private void run() {
    try {
      // register the given watch service.
      // Note: this watch a directory and not the single file we're interested in, so that's the
      // reason in #watch() we filter watch service events based on the updated file.
      this.toWatch.getParentFile().toPath().register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
    } catch (IOException e) {
      logger.error("Error while starting watching the file", e);
      return;
    }
    logger.info("Started watching {}", toWatch);

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
        break; // Looks good, this means Thread.interrupt was invoked
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
