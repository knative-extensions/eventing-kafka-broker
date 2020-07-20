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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import dev.knative.eventing.kafka.broker.core.config.BrokersConfig.Brokers;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchService;
import java.util.Objects;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileWatcher is the class responsible for watching a given file and reports update.
 */
public class FileWatcher {

  private static final Logger logger = LoggerFactory.getLogger(FileWatcher.class);

  private final Consumer<Brokers> brokersConsumer;

  private final WatchService watcher;
  private final Path toWatchParentPath;
  private final Path toWatchPath;
  private final File toWatch;

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
      final File file)
      throws IOException {

    Objects.requireNonNull(brokersConsumer, "provide consumer");
    Objects.requireNonNull(file, "provide file");

    // register the given watch service.
    // Note: this watch a directory and not the single file we're interested in, so that's the
    // reason in #watch() we filter watch service events based on the updated file.

    this.brokersConsumer = brokersConsumer;
    toWatch = file.getAbsoluteFile();
    logger.info("start watching {}", toWatch);

    toWatchPath = file.toPath();
    toWatchParentPath = file.getParentFile().toPath();

    this.watcher = watcher;

    toWatchParentPath.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
  }

  /**
   * Start watching.
   *
   * @throws IOException          see {@link BufferedInputStream#readAllBytes()}.
   * @throws InterruptedException see {@link WatchService#take()}
   */
  public void watch() throws IOException, InterruptedException {

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

        final WatchEvent<Path> ev = cast(event);
        final var child = toWatchParentPath.resolve(ev.context());
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

  private void update() throws IOException {
    try (
        final var fileReader = new FileReader(toWatch);
        final var bufferedReader = new BufferedReader(fileReader)) {
      parseFromJson(bufferedReader);
    }
  }

  private void parseFromJson(final Reader content) throws IOException {
    try {

      final var brokers = Brokers.newBuilder();
      JsonFormat.parser().merge(content, brokers);

      brokersConsumer.accept(brokers.build());

    } catch (final InvalidProtocolBufferException ex) {
      logger.warn("failed to parse from JSON", ex);
    }
  }

  @SuppressWarnings("unchecked")
  static <T> WatchEvent<T> cast(WatchEvent<?> event) {
    return (WatchEvent<T>) event;
  }
}
