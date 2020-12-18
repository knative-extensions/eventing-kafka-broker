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

package dev.knative.eventing.kafka.broker.core.security;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

class Stores {

  private static final Logger logger = LoggerFactory.getLogger(Stores.class);

  static final String KEYSTORE_FILENAME = "/keystore";
  static final String TRUSTSTORE_FILENAME = "/truststore";

  static final String DIR_PATH_ENV_KEY = "STORES_PATH";
  /* non final for testing */
  static String dirPathFormat = System.getenv(DIR_PATH_ENV_KEY) + "%s";

  public static Future<Void> write(@Nonnull final Vertx vertx,
                            @Nonnull final Credentials credentials) {

    final var truststorePath = credentials.truststorePath();
    final var keystorePath = credentials.keystorePath();
    if (keystorePath == null || truststorePath == null) {
      return Future.failedFuture("Truststore or keystore path is null");
    }

    final var parent = keystorePath.substring(0, keystorePath.lastIndexOf('/'));

    return vertx.fileSystem()
      .mkdirs(parent)
      .compose(ignored -> CompositeFuture
        .all(
          writeStore(vertx, credentials.truststore(), truststorePath),
          writeStore(vertx, credentials.keystore(), keystorePath)
        )
        .mapEmpty()
      );
  }

  static String keystorePath(final String dir) {
    return String.format(dirPathFormat, dir) + KEYSTORE_FILENAME;
  }

  static String truststorePath(final String dir) {
    return String.format(dirPathFormat, dir) + TRUSTSTORE_FILENAME;
  }

  private static Future<?> writeStore(final Vertx vertx,
                                      final String store,
                                      final String truststorePath) {
    if (store != null) {
      return vertx.fileSystem().writeFile(truststorePath, Buffer.buffer(store));
    }
    return Future.failedFuture("Null store content for path: " + truststorePath);
  }
}
