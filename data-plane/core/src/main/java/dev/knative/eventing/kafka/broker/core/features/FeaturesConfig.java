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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class FeaturesConfig {

    private final String DISABLED = "disabled";
    private final String ENABLED = "enabled";

    public static final String KEY_AUTHENTICATION_OIDC = "authentication-oidc";

    private final Map<String, String> features;

    public FeaturesConfig(String path) throws IOException {
        features = new HashMap<>();
        String[] keys = {
            KEY_AUTHENTICATION_OIDC,
        };

        for (String key : keys) {
            Path filePath = Paths.get(path, key);
            if (Files.exists(filePath)) {
                features.put(key, Files.readString(filePath));
            }
        }
    }

    public boolean isAuthenticationOIDC() {
        return isEnabled(KEY_AUTHENTICATION_OIDC);
    }

    private boolean isEnabled(String key) {
        return features.getOrDefault(key, DISABLED).toLowerCase().equals(ENABLED);
    }
}
