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

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for watching a given file and reports update or execute a trigger function.
 * <p>
 * Using {@link #start()}, this class will create a background thread running
 * the file watcher.
 * You can interrupt such thread with {@link #close()}
 * <p>
 * This class is thread safe, and it cannot start more than one watch at the
 * time.
 */
public class FileWatcher implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(FileWatcher.class);

    private final File toWatch;
    private Runnable triggerFunction;

    private Thread watcherThread;
    private WatchService watcher;

    private final CountDownLatch waitRunning;

    /**
     * All args constructor.
     *
     * @param triggerFunction is triggered whenever there is a file change.
     * @param file             file to watch
     */
    public FileWatcher(File file, Runnable triggerFunction) {
        Objects.requireNonNull(file, "provide file");
        Objects.requireNonNull(triggerFunction, "provide trigger function");

        this.triggerFunction = triggerFunction;
        this.toWatch = file.getAbsoluteFile();
        this.waitRunning = new CountDownLatch(1);
    }

    public Thread getWatcherThread() {
        return this.watcherThread;
    }

    /**
     * Start the watcher thread.
     * This is going to create a new deamon thread, which can be stopped using
     * {@link #close()}.
     *
     * @throws IOException           if an error happened while starting to watch
     * @throws IllegalStateException if the watcher is already running
     */
    public CountDownLatch start() throws IOException {
        synchronized (this) {
            if (this.watcherThread != null) {
                throw new IllegalStateException("Watcher thread is already up and running");
            }

            // Start watching
            this.watcher = FileSystems.getDefault().newWatchService();
            toWatch.getParentFile().toPath().register(this.watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);

            // Start the watcher thread
            this.watcherThread = new Thread(null, this::run, "contract-file-watcher");
        }

        this.watcherThread.start();

        return this.waitRunning;
    }

    @Override
    public synchronized void close() throws Exception {
        if (this.watcherThread == null) {
            throw new IllegalStateException("Watcher thread is not running");
        }
        this.watcherThread.interrupt();
        this.watcherThread = null;
    }

    public void run() {
        try {
            // register the given watch service.
            // Note: this watches a directory and not the single file we're interested in, so
            // that's the reason we filter watch service events based on the updated file.
            this.toWatch.getParentFile().toPath().register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
        } catch (IOException e) {
            logger.error("Error while starting watching the file", e);
            return;
        }
        logger.info("Started watching {}", toWatch);

        this.waitRunning.countDown();

        triggerFunction.run();

        while (!Thread.interrupted()) {

            WatchKey key;
            try {
                key = watcher.take();
                logger.debug("Contract updates");
            } catch (InterruptedException e) {
                break; // Thread.interrupt was invoked
            }

            // Check the watch key's validity
            if (!key.isValid()) {
                logger.warn("Invalid key");
                continue;
            }

            // Loop through all watch service events
            for (final var event : key.pollEvents()) {
                final var kind = event.kind();

                // We check if the event's context (the file) matches our target file
                if (kind != OVERFLOW) {
                    triggerFunction.run();
                    break;
                }
            }

            // Reset the watch key to receive new events
            key.reset();
        }

        // Close the watcher
        try {
            this.watcher.close();
        } catch (IOException e) {
            logger.warn("Error while closing the file watcher", e);
        }
    }
}
