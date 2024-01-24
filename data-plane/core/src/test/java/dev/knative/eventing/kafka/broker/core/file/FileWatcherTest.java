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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;

public class FileWatcherTest {

    private File tempFile;
    private FileWatcher fileWatcher;

    @BeforeEach
    public void setUp() throws Exception {
        // Create a temporary file for testing purposes
        tempFile = Files.createTempFile("test", ".txt").toFile();
    }

    @AfterEach
    public void tearDown() throws Exception {
        fileWatcher.close();
        Files.deleteIfExists(tempFile.toPath());
    }

    @Test
    public void testFileModification() throws Exception {
        // Set up a counter to track how many times the trigger function is called
        AtomicInteger counter = new AtomicInteger(0);

        fileWatcher = new FileWatcher(tempFile, () -> {
            counter.incrementAndGet();
        });
        fileWatcher.start().await();

        // Modify the file
        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("Test Data");
        }

        // Await until the trigger function is called twice: 1 is for the initial file
        // read, and 1 is for the file modification
        await().until(() -> counter.get() == 2);
    }

    @Test
    public void testFileNoUpdate() throws Exception {
        // Set up a counter to track how many times the trigger function is called
        AtomicInteger counter = new AtomicInteger(0);

        fileWatcher = new FileWatcher(tempFile, () -> {
            counter.incrementAndGet();
        });
        fileWatcher.start().await();

        // Await until the trigger function is called once: 1 is for the initial file
        // read
        await().until(() -> counter.get() == 1);
    }
}
