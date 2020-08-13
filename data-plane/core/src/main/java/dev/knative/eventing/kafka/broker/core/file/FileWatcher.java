/*
 * Copyright 2020 The Knative Authors
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

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import com.google.protobuf.util.JsonFormat;
import dev.knative.eventing.kafka.broker.core.config.BrokersConfig.Brokers;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.WatchService;
import java.util.Objects;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileWatcher is the class responsible for watching a given file and reports update.
 */
public class FileWatcher {

  public enum FileFormat {
    PROTOBUF,
    JSON;

    /**
     * Parse the given string and return a format.
     *
     * @param s file format
     * @return file format.
     */
    public static FileFormat from(final String s) {
      switch (s) {
        case "protobuf":
          return PROTOBUF;
        case "json":
          return JSON;
        default:
          throw new IllegalArgumentException("Unsupported file format: " + s);
      }
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(FileWatcher.class);

  private final Consumer<Brokers> brokersConsumer;

  private final WatchService watcher;
  private final File toWatch;
  private final FileFormat fileFormat;

  /**
   * All args constructor.
   *
   * @param watcher         watch service
   * @param brokersConsumer updates receiver.
   * @param file            file to watch
   * @throws IOException watch service cannot be registered.
   */
  public FileWatcher(
      final WatchService watcher,
      final Consumer<Brokers> brokersConsumer,
      final File file,
      final FileFormat fileFormat)
      throws IOException {

    Objects.requireNonNull(brokersConsumer, "provide consumer");
    Objects.requireNonNull(file, "provide file");
    Objects.requireNonNull(fileFormat, "provide fileFormat");

    // register the given watch service.
    // Note: this watch a directory and not the single file we're interested in, so that's the
    // reason in #watch() we filter watch service events based on the updated file.

    this.brokersConsumer = brokersConsumer;
    this.fileFormat = fileFormat;
    toWatch = file.getAbsoluteFile();
    logger.info("start watching {}", toWatch);

    Path toWatchParentPath = file.getParentFile().toPath();

    this.watcher = watcher;

    toWatchParentPath.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
  }

  /**
   * Start watching.
   *
   * @throws InterruptedException see {@link WatchService#take()}
   */
  public void watch() throws InterruptedException {
    // If the container restarts, the mounted file never gets reconciled, so update as soon as we
    // start watching
    update();

    while (true) {
      var shouldUpdate = false;

      // Note: take() blocks
      final var key = watcher.take();
      logger.info("broker updates");

      // this should be rare but it can actually happen so check watch key validity
      if (!key.isValid()) {
        logger.warn("invalid key");
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
  }

  private void update() {
    switch (fileFormat) {
      case JSON:
        parseInJson();
        break;
      case PROTOBUF:
        parseInProtobuf();
        break;
      default:
        throw new IllegalArgumentException("Unsupported config format: " + fileFormat);
    }
  }

  private void parseInProtobuf() {
    try (final var in = new FileInputStream(toWatch);
        final var bufferedInputStream = new BufferedInputStream(in)) {

      if (bufferedInputStream.available() <= 0) {
        return;
      }

      brokersConsumer.accept(Brokers.parseFrom(bufferedInputStream));

    } catch (final Exception ex) {
      logger.warn("failed to parse in Protocol Buffer format", ex);
    }
  }

  private void parseInJson() {
    try (
        final var fileReader = new FileReader(toWatch);
        final var bufferedReader = new BufferedReader(fileReader)) {

      final var brokers = Brokers.newBuilder();
      JsonFormat.parser().merge(bufferedReader, brokers);

      brokersConsumer.accept(brokers.build());

    } catch (final Exception ex) {
      logger.warn("failed to parse in JSON format", ex);
    }
  }
}
