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

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class StoresTest {

  @Test
  public void shouldWriteCStoreFiles(final Vertx vertx, final VertxTestContext context) throws InterruptedException {

    final var tempDir = System.getProperty("java.io.tmpdir") + "/" + Math.abs(new Random(System.currentTimeMillis()).nextInt());
    Stores.dirPathFormat = tempDir + "%s";

    final var subDir = "/namespace/name";
    final var truststorePath = Stores.truststorePath(subDir);
    final var truststoreContent = "abc-truststore";
    final var keystorePath = Stores.keystorePath(subDir);
    final var keystoreContent = "abc-keystore";

    final var credentials = mock(Credentials.class);
    when(credentials.truststore()).thenReturn(truststoreContent);
    when(credentials.truststorePath()).thenReturn(Stores.truststorePath("/namespace/name"));
    when(credentials.keystore()).thenReturn(keystoreContent);
    when(credentials.keystorePath()).thenReturn(Stores.keystorePath("/namespace/name"));

    final var waitWrite = new CountDownLatch(1);

    Stores.write(vertx, credentials)
      .onFailure(context::failNow)
      .onSuccess(v -> context.verify(waitWrite::countDown));

    waitWrite.await(10, TimeUnit.SECONDS);

    context.verify(() -> {
      assertThat(new String(Files.readAllBytes(Path.of(truststorePath)))).isEqualTo(truststoreContent);
      assertThat(new String(Files.readAllBytes(Path.of(keystorePath)))).isEqualTo(keystoreContent);

      context.completeNow();
    });
  }
}
