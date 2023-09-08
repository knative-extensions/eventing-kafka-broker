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

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Watches a directory for changes to TLS secrets. */
public class SecretWatcher implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(SecretWatcher.class);

  private final String dir; // directory to watch
  private final WatchService watcher; // watch service
  private final Runnable updateAction; // action to run when a change is detected

  public SecretWatcher(String dir, Runnable updateAction) throws IOException {
    this.dir = dir;
    this.updateAction = updateAction;
    this.watcher = FileSystems.getDefault().newWatchService();

    private static String KEY_FILE = "tls.key";
    private static String CRT_FILE = "tls.crt";

    Path path = Path.of(dir);
    path.register(watcher, StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE);
  }

  @Override
  public void run() {
    try {
      WatchKey key;
      while ((key = watcher.take()) != null) {
        for (WatchEvent<?> event : key.pollEvents()) {
          Path changed = (Path) event.context();
          Path fullChangedPath = Path.of(dir, changed.toString());
          if (Files.isSymbolicLink(fullChangedPath) || changed.endsWith(KEY_FILE) || changed.endsWith(CRT_FILE)) {
            logger.info("Detected change to secret {}", changed);
            updateAction.run();
          }
        }
        key.reset();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.info("Watcher exception", e);
    } finally {
      this.stop();
    }
  }

  // stop the watcher
  public void stop() {
    try {
      watcher.close();
    } catch (IOException e) {
      logger.error("Failed to close secret watcher", e);
    }
  }
}
