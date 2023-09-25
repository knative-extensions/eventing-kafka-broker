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

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import org.junit.jupiter.api.*;

public class FileWatcherTest {

    private File tempFile;
    private FileWatcher fileWatcher;
    private volatile boolean wasTriggered = false;

    @BeforeEach
    public void setUp() throws Exception {
        // Create a temporary file for testing purposes
        tempFile = Files.createTempFile("test", ".txt").toFile();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (fileWatcher != null && fileWatcher.getWatcherThread() != null) {
            fileWatcher.close();
        }
        Files.deleteIfExists(tempFile.toPath());
    }

    @Test
    public void testStart() throws Exception {
        fileWatcher = new FileWatcher(tempFile, () -> {});
        assertDoesNotThrow(fileWatcher::start);
    }

    @Test
    public void testClose() throws Exception {
        fileWatcher = new FileWatcher(tempFile, () -> {});
        fileWatcher.start();
        assertDoesNotThrow(fileWatcher::close);
    }

    @Test
    public void testFileModification() throws Exception {

        fileWatcher = new FileWatcher(tempFile, () -> wasTriggered = true);
        fileWatcher.start();

        // Modify the file
        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("Test Data");
        }

        // Sleep for a duration to allow the FileWatcher to detect changes
        Thread.sleep(5000); // sleep for 5 seconds. Adjust as necessary.

        assertTrue(wasTriggered, "Trigger function was not called upon file modification");
    }
}
