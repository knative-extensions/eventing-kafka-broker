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

package dev.knative.eventing.kafka.broker.core.features;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FeaturesConfigTest {

    private File featuresDir;

    @BeforeEach
    public void setup() throws IOException {
        featuresDir = Files.createTempDirectory("features").toFile();

        Files.createFile(Path.of(featuresDir.getAbsolutePath(), FeaturesConfig.KEY_AUTHENTICATION_OIDC))
                .toFile();
    }

    @AfterEach
    public void cleanup() throws IOException {
        Files.walk(featuresDir.toPath())
                .filter(Files::isRegularFile)
                .map(Path::toFile)
                .forEach(File::delete);

        featuresDir.delete();
    }

    @Test
    public void testFeaturesConfigAuthenticationOIDC() throws IOException {
        FeaturesConfig fc = new FeaturesConfig(featuresDir.getAbsolutePath());
        Assertions.assertFalse(fc.isAuthenticationOIDC(), "should be false, if feature file is empty");

        try (FileWriter writer =
                new FileWriter(Paths.get(featuresDir.getAbsolutePath(), FeaturesConfig.KEY_AUTHENTICATION_OIDC)
                        .toString())) {
            writer.write("enabled");
        }

        fc = new FeaturesConfig(featuresDir.getAbsolutePath());
        Assertions.assertTrue(fc.isAuthenticationOIDC(), "should be true, if feature is enabled");
    }
}
