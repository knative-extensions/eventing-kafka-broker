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
package dev.knative.eventing.kafka.broker.receiver.impl;

import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.TextNode;
import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.ReactiveKafkaProducer;
import dev.knative.eventing.kafka.broker.core.eventbus.ContractMessageCodec;
import dev.knative.eventing.kafka.broker.core.eventbus.ContractPublisher;
import dev.knative.eventing.kafka.broker.core.eventtype.EventTypeListerFactory;
import dev.knative.eventing.kafka.broker.core.metrics.Metrics;
import dev.knative.eventing.kafka.broker.core.reconciler.impl.ResourcesReconcilerMessageHandler;
import dev.knative.eventing.kafka.broker.core.security.AuthProvider;
import dev.knative.eventing.kafka.broker.core.testing.CloudEventSerializerMock;
import dev.knative.eventing.kafka.broker.receiver.MockReactiveKafkaProducer;
import dev.knative.eventing.kafka.broker.receiver.impl.auth.OIDCDiscoveryConfigListener;
import dev.knative.eventing.kafka.broker.receiver.impl.handler.ControlPlaneProbeRequestUtil;
import dev.knative.eventing.kafka.broker.receiver.impl.handler.IngressRequestHandlerImpl;
import dev.knative.eventing.kafka.broker.receiver.main.ReceiverEnv;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.v1.CloudEventBuilder;
import io.cloudevents.http.vertx.VertxMessageFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.LoggerFactory;

@ExtendWith(VertxExtension.class)
public class ReceiverVerticleTest {

    // Set up the logger that is connected to ReceiverVerticle
    private static final Logger logger = (Logger) LoggerFactory.getLogger(ReceiverVerticle.class);
    private ListAppender<ILoggingEvent> listAppender = new ListAppender<>();

    private static final int TIMEOUT = 10;
    private static final int PORT = 8082;
    private static final int TLS_PORT = 8443;
    private static final String HOST = "localhost";
    private static final String SECRET_VOLUME_PATH = "src/test/resources";
    private static final String TLS_CRT_FILE_PATH = SECRET_VOLUME_PATH + "/tls.crt";
    private static final String TLS_KEY_FILE_PATH = SECRET_VOLUME_PATH + "/tls.key";

    private static WebClient webClient;

    private static MockProducer<String, CloudEvent> mockProducer;
    private static IngressProducerReconcilableStore store;

    static {
        BackendRegistries.setupBackend(
                new MicrometerMetricsOptions()
                        .setMicrometerRegistry(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT))
                        .setRegistryName(Metrics.METRICS_REGISTRY_NAME),
                null);
    }

    private PrometheusMeterRegistry registry;

    @BeforeEach
    public void setUpHTTP(final Vertx vertx, final VertxTestContext testContext) {
        ContractMessageCodec.register(vertx.eventBus());

        webClient = WebClient.create(vertx);
        ReceiverVerticleTest.mockProducer =
                new MockProducer<>(true, new StringSerializer(), new CloudEventSerializerMock());
        ReactiveKafkaProducer<String, CloudEvent> producer = new MockReactiveKafkaProducer<>(mockProducer);

        store = new IngressProducerReconcilableStore(
                AuthProvider.noAuth(), new Properties(), properties -> producer, mock(EventTypeListerFactory.class));

        registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

        final var httpServerOptions = new HttpServerOptions();
        httpServerOptions.setPort(PORT);
        httpServerOptions.setHost(HOST);

        final var httpsServerOptions = new HttpServerOptions();
        httpsServerOptions.setPort(TLS_PORT);
        httpsServerOptions.setHost(HOST);
        httpsServerOptions.setSsl(true);
        httpsServerOptions.setPemKeyCertOptions(
                new PemKeyCertOptions().setCertPath(TLS_KEY_FILE_PATH).setKeyPath(TLS_CRT_FILE_PATH));

        final var env = mock(ReceiverEnv.class);
        when(env.getLivenessProbePath()).thenReturn("/healthz");
        when(env.getReadinessProbePath()).thenReturn("/readyz");
        final var verticle = new ReceiverVerticle(
                env,
                httpServerOptions,
                httpsServerOptions,
                v -> store,
                new IngressRequestHandlerImpl(registry, ((event, lister, reference) -> null)),
                SECRET_VOLUME_PATH,
                mock(OIDCDiscoveryConfigListener.class));
        vertx.deployVerticle(verticle, testContext.succeeding(ar -> testContext.completeNow()));

        // Connect to the logger in ReceiverVerticle
        listAppender.start();
        logger.addAppender(listAppender);
    }

    @BeforeEach
    public void cleanUp() {
        mockProducer.clear();
    }

    @AfterEach
    public void tearDownEach() {
        registry.close();
    }

    @AfterAll
    public static void tearDown() {
        webClient.close();
    }

    @ParameterizedTest
    @MethodSource({"getValidNonValidEvents"})
    public void shouldProduceMessagesReceivedConcurrently(
            final TestCase tc, final Vertx vertx, final VertxTestContext context) throws InterruptedException {

        final var checkpoints = context.checkpoint(1);
        final var countDown = new CountDownLatch(1);

        new ContractPublisher(vertx.eventBus(), ResourcesReconcilerMessageHandler.ADDRESS).accept(tc.getContract());

        tc.requestSender
                .apply(webClient.post(PORT, HOST, tc.path))
                .onFailure(context::failNow)
                .onSuccess(response -> vertx.setTimer(
                        1000,
                        ignored -> context.verify(() -> {
                            assertThat(response.statusCode())
                                    .as("verify path: " + tc.path)
                                    .isEqualTo(tc.responseStatusCode);

                            final var expectedCount = (double) tc.badRequestCount + (double) tc.produceEventCount;

                            final var defaultCounter =
                                    Counter.builder(Metrics.EVENTS_COUNT).register(Metrics.getRegistry());
                            Counter counter = defaultCounter;
                            try {
                                if (expectedCount > 0) {
                                    counter = registry.get(Metrics.EVENTS_COUNT).counters().stream()
                                            .reduce((a, b) -> b) // get last element
                                            .orElse(defaultCounter);
                                }
                            } catch (MeterNotFoundException ignored1) {
                            }

                            assertThat(counter.count())
                                    .describedAs("Counter: "
                                            + StreamSupport.stream(
                                                            counter.measure().spliterator(), false)
                                                    .map(Measurement::toString)
                                                    .collect(Collectors.joining()))
                                    .isEqualTo(expectedCount);

                            if (tc.expectedDispatchLatency) {
                                final var eventDispatchLatency =
                                        registry.get(Metrics.EVENT_DISPATCH_LATENCY).summaries().stream()
                                                .reduce((a, b) -> b)
                                                .get();

                                assertThat(eventDispatchLatency.max()).isGreaterThan(0);
                                assertThat(eventDispatchLatency.totalAmount()).isGreaterThan(0);
                                assertThat(eventDispatchLatency.mean()).isGreaterThan(0);
                            }

                            checkpoints.flag();
                            countDown.countDown();
                        })));

        countDown.await(TIMEOUT, TimeUnit.SECONDS);

        if (mockProducer.history().size() > 0) {
            assertThat(mockProducer.history()).containsExactlyInAnyOrder(tc.record);
        }
    }

    // Write the test to verify that when the secret is updated
    @Test
    public void secretFileUpdated() throws InterruptedException {
        // Modified the secret file in the test resources folder
        File tlsCrtFile = new File(TLS_CRT_FILE_PATH);
        File tlsKeyFile = new File(TLS_KEY_FILE_PATH);

        // Have a copy of the original TLS cert content by using Files.readString
        String original_TLS_Cert = "";
        String original_TLS_Key = "";
        try {
            original_TLS_Cert = Files.readString(tlsCrtFile.toPath());
            original_TLS_Key = Files.readString(tlsKeyFile.toPath());
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Write the new CA cert to the file
        String new_TLS_Cert =
                """
-----BEGIN CERTIFICATE-----
MIIDmDCCAoCgAwIBAgIUZx4ztTK7wyEpRYKkKqM9+oFr+PwwDQYJKoZIhvcNAQEL
BQAwJzELMAkGA1UEBhMCVVMxGDAWBgNVBAMMD0V4YW1wbGUtUm9vdC1DQTAeFw0y
MzA3MTcxNDI1MzhaFw0yNjA1MDYxNDI1MzhaMG0xCzAJBgNVBAYTAlVTMRIwEAYD
VQQIDAlZb3VyU3RhdGUxETAPBgNVBAcMCFlvdXJDaXR5MR0wGwYDVQQKDBRFeGFt
cGxlLUNlcnRpZmljYXRlczEYMBYGA1UEAwwPbG9jYWxob3N0LmxvY2FsMIIBIjAN
BgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyq0tbWj3zb/lhcykAAXlc8RVVPiZ
898NxNV1od3XvFUFRYkQP9DU/3nE/5DxDQbQmfTlov50WbgSgQxt9GR7iC3lheOm
B3ODaA0p3C7bBg7LeUvtrhvPyHITDI9Aqy8cUO5XHVgbTceW7XOvcmju/DVpm9Id
iSpEEPMT2GsuLQ2rVvNupIccYRe0NhZly7l27AAkf5y1G2Yd9Oklt+gOPNPB+afH
/eFlYRrKokp58Kt1eyDNAwaYV8arEKIapU2AQheZTZQSBOi/tFCc7oKFQOmO9sFf
HEuQfCVd8TZJ2vb7qdiLVlgTDwjVYmUkfkxR7JJ/feDacyfjGkqYd1bngQIDAQAB
o3YwdDAfBgNVHSMEGDAWgBQGanp895VYiwZNv+X+JJ7GWjQtWTAJBgNVHRMEAjAA
MAsGA1UdDwQEAwIE8DAaBgNVHREEEzARgglsb2NhbGhvc3SHBH8AAAEwHQYDVR0O
BBYEFOlfLUC1MJOOjGRWfVzHQYA+Iya4MA0GCSqGSIb3DQEBCwUAA4IBAQACCgdN
Sj+W39W+8JdHpBU/fw1wwNDB4SyIyxAgPXp8TWiOwoo3ozcALP44ab4jP9b+Etlm
yNMNdayOf42SCZUhihO4PKiiqDgolDQfYaZbiIEXJ/xaXtao5SxyBPY77eXtXN/+
E7/TOWQ5U7qJYd7H5vqhlFk6fn7s6WKkue8ELUrWh8r3THASXUsa8xzxHu0nsp2v
SsbYyR0vyrGE4yvComvl75Igw6jY70cswWdyThGKV6ZLip2BrjLQlFhr3IZN5tbg
rHxaoqIen8NYjNpBdJDInPMFZshZSx1lAzw6uwP4OuM5WQHgYEk7V+TkOU3osqgD
5bOo/SpCokC166Ym
-----END CERTIFICATE-----""";

        String new_TLS_key =
                """
-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDKrS1taPfNv+WF
zKQABeVzxFVU+Jnz3w3E1XWh3de8VQVFiRA/0NT/ecT/kPENBtCZ9OWi/nRZuBKB
DG30ZHuILeWF46YHc4NoDSncLtsGDst5S+2uG8/IchMMj0CrLxxQ7lcdWBtNx5bt
c69yaO78NWmb0h2JKkQQ8xPYay4tDatW826khxxhF7Q2FmXLuXbsACR/nLUbZh30
6SW36A4808H5p8f94WVhGsqiSnnwq3V7IM0DBphXxqsQohqlTYBCF5lNlBIE6L+0
UJzugoVA6Y72wV8cS5B8JV3xNkna9vup2ItWWBMPCNViZSR+TFHskn994NpzJ+Ma
Sph3VueBAgMBAAECggEABqD+ZN8zh6u4lpU4YfXPaOdpgRN2eZb4jNEMfWRTm4nO
V9VhTi0G4mo5qsAzWiE4bminoBqhdJPEKytcZ0toDO6vXJ8y/XhmOl9/2H9B06Nl
PUzh87leJOiyPc1rqI2sZ+s7ty58CiG2ioKnoN7UvjQDBcEsDSHwQvuoUQJEat3C
IxK792JWumgDzJWy2YOVGbcYKSLRV5q+IEdIH7SG5NDTMTs+x+os0azGLe/X+++o
bhbXlYhMnA3X1XQ9yiJaK4QfwEc+i3YFNjRuD/cS8dv/dPji9F6eS7HewscW+f5f
EoUMSSvO6FqYQt1S7jD9kDzQ8TaKlA/pRrVumTs96QKBgQDR2spitSuPm0K+wLMj
5gj8OyM4eW0pePQQHYNy4UdHu9EZ/w88WNQCKF1RKbAcBECxns97oI6WskAumzpm
1jqa6Ofe2y8k1Vc6t3PTqVU98ms59M4ifb0aq+entzp3FIQAOaO/2x5+NaUrA/kp
6EX4IG5UNFv1+J6dpzI+Il/8bQKBgQD3Pk9/pjrAsigO4qrN54x1CsVorrSIxwp6
4RUI486gAZx7bKjn8hcnKnT/U+9Z8ui4i318kuly4nDSpvW8e4PXVLt9fTQKa/4f
BtFoizTu0PqAmntljbVBZ9a1QN5puc7BxCbYO/md6BQ68dlXnu6NuPys/E8dIuAO
ndOVbj5C5QKBgQClWVUaDVHzZwxiLId6A6iUxSvtNY/Tm6ACip6mB+cYGF6bsyKY
FA2IXbGZX9WJXbhzu4QUDuAK0QxNLLYJjUbEBDuelulAhnCirSWwYr3tf3MJSWCa
QKSdvVFcDr0cUqfnXYMuikIug6pOiGTspj1rUnJcGp1S48Bmy/SEjKVAyQKBgEcA
8QnCrlrKjzB/LfhGCBNQzZKboaMqLjtNyqGr8poG/G6BrRw3bSjFS6ZL75AQb38Y
KCiPdFWW7DnC0w2XFyzO261VOI3Jp8g3SApS+Behkl8+fjOS97vZ21JgV79bKiKB
d3pf9va/QJgQ/o7oSLAQsRfoubuvWVM5RhtC9sR1AoGBAMUpJh9LOJvaCFTJ51SZ
ERr1RMt6Q13ssG7ytGQIsJ7PSoRJMBN1ZrEYANAuJU5n3V8AxWcKIjV3hoXc2yg6
TfwO4tuRZ0hSBe+POhImNGNVJ71yylGDFJc/21KMRxXi1IIKVr0qjhv0IQcvDDN2
QCDcINom+skQGHJlbPdrpwNW
-----END PRIVATE KEY-----""";

        try {
            FileWriter tlsCrtWriter = new FileWriter(tlsCrtFile);
            tlsCrtWriter.write(new_TLS_Cert);
            tlsCrtWriter.close();

            FileWriter tlsKeyWriter = new FileWriter(tlsKeyFile);
            tlsKeyWriter.write(new_TLS_key);
            tlsKeyWriter.close();
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

        // Sleep 1 sec to make sure the file is updated
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Write the old TLS cert and key back
        try {
            FileWriter tlsCrtWriter = new FileWriter(tlsCrtFile);
            tlsCrtWriter.write(original_TLS_Cert);
            tlsCrtWriter.close();

            FileWriter tlsKeyWriter = new FileWriter(tlsKeyFile);
            tlsKeyWriter.write(original_TLS_Key);
            tlsKeyWriter.close();
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

        // Check the logger to see if the message "The secret volume is updated" is printed by using
        // logger
        List<ILoggingEvent> logList = listAppender.list;
        assertEquals("Succeeded to update TLS key pair", logList.get(0).getMessage());
    }

    private static List<TestCase> getValidNonValidEvents() {
        return Arrays.asList(
                new TestCase(
                        null,
                        "/broker-ns/broker-name1",
                        ceRequestSender(new CloudEventBuilder()
                                .withSubject("subject")
                                .withSource(URI.create("/hello"))
                                .withType("type")
                                .withId("1234")
                                .build()),
                        NOT_FOUND.code(),
                        DataPlaneContract.Resource.newBuilder()
                                .setUid("1")
                                .addTopics("topic")
                                .setIngress(
                                        DataPlaneContract.Ingress.newBuilder().setPath("/broker-ns/broker-name"))
                                .build()),
                new TestCase(
                        null,
                        "/broker-ns/broker-name/hello",
                        ceRequestSender(new CloudEventBuilder()
                                .withSubject("subject")
                                .withSource(URI.create("/hello"))
                                .withType("type")
                                .withId("1234")
                                .build()),
                        NOT_FOUND.code(),
                        DataPlaneContract.Resource.newBuilder()
                                .setUid("1")
                                .addTopics("topic")
                                .setIngress(
                                        DataPlaneContract.Ingress.newBuilder().setPath("/broker-name/hello"))
                                .build()),
                new TestCase(
                        null,
                        "/broker-ns/h/hello",
                        ceRequestSender(new CloudEventBuilder()
                                .withSubject("subject")
                                .withSource(URI.create("/hello"))
                                .withType("type")
                                .withId("1234")
                                .build()),
                        NOT_FOUND.code(),
                        DataPlaneContract.Resource.newBuilder()
                                .setUid("1")
                                .addTopics("topic")
                                .setIngress(
                                        DataPlaneContract.Ingress.newBuilder().setPath("/h/hello"))
                                .build()),
                new TestCase(
                        new ProducerRecord<>(
                                "topic-name-1",
                                null,
                                new io.cloudevents.core.v03.CloudEventBuilder()
                                        .withSubject("subject")
                                        .withSource(URI.create("/hello"))
                                        .withType("type")
                                        .withId("1234")
                                        .build()),
                        "/broker-ns/broker-name1",
                        ceRequestSender(new io.cloudevents.core.v03.CloudEventBuilder()
                                .withSubject("subject")
                                .withSource(URI.create("/hello"))
                                .withType("type")
                                .withId("1234")
                                .build()),
                        ACCEPTED.code(),
                        DataPlaneContract.Resource.newBuilder()
                                .setUid("1")
                                .addTopics("topic-name-1")
                                .setIngress(
                                        DataPlaneContract.Ingress.newBuilder().setPath("/broker-ns/broker-name1"))
                                .build()),
                new TestCase(
                        null,
                        "/broker-ns/broker-name",
                        request -> request.sendBuffer(Buffer.buffer("this is not a cloud event")),
                        BAD_REQUEST.code(),
                        DataPlaneContract.Resource.newBuilder()
                                .setUid("1")
                                .addTopics("topic")
                                .setIngress(
                                        DataPlaneContract.Ingress.newBuilder().setPath("/broker-ns/broker-name"))
                                .build()),
                new TestCase(
                        null,
                        "/broker-ns/broker-name/hello",
                        request -> request.sendBuffer(Buffer.buffer("this is not a cloud event")),
                        NOT_FOUND.code(),
                        DataPlaneContract.Resource.newBuilder()
                                .setUid("1")
                                .addTopics("topic")
                                .setIngress(
                                        DataPlaneContract.Ingress.newBuilder().setPath("/broker-ns/broker-name"))
                                .build()),
                new TestCase(
                        null,
                        "/broker-ns/broker-name3",
                        request -> {
                            final var objectMapper = new ObjectMapper();
                            final var objectNode = objectMapper.createObjectNode();
                            objectNode.set("hello", new FloatNode(1.24f));
                            objectNode.set("data", objectMapper.createObjectNode());
                            request.headers().set("Content-Type", "application/json");
                            return request.sendBuffer(Buffer.buffer(objectNode.toString()));
                        },
                        BAD_REQUEST.code(),
                        DataPlaneContract.Resource.newBuilder()
                                .setUid("1")
                                .addTopics("topic")
                                .setIngress(
                                        DataPlaneContract.Ingress.newBuilder().setPath("/broker-ns/broker-name3"))
                                .build()),
                new TestCase(
                        null,
                        "/broker-ns/broker-name4",
                        request -> {
                            final var objectMapper = new ObjectMapper();
                            final var objectNode = objectMapper.createObjectNode();
                            objectNode.set("specversion", new TextNode("1.0"));
                            objectNode.set("type", new TextNode("my-type"));
                            objectNode.set("source", new TextNode("my-source"));
                            objectNode.set("data", objectMapper.createObjectNode());
                            request.headers().set("Content-Type", "application/json");
                            return request.sendBuffer(Buffer.buffer(objectNode.toString()));
                        },
                        BAD_REQUEST.code(),
                        DataPlaneContract.Resource.newBuilder()
                                .setUid("1")
                                .addTopics("topic")
                                .setIngress(
                                        DataPlaneContract.Ingress.newBuilder().setPath("/broker-ns/broker-name4"))
                                .build()),
                new TestCase(
                        new ProducerRecord<>(
                                "topic-name-42",
                                null,
                                new io.cloudevents.core.v1.CloudEventBuilder()
                                        .withSubject("subject")
                                        .withSource(URI.create("/hello"))
                                        .withType("type")
                                        .withId("1234")
                                        .build()),
                        "/broker-ns/broker-name5",
                        ceRequestSender(new io.cloudevents.core.v1.CloudEventBuilder()
                                .withSubject("subject")
                                .withSource(URI.create("/hello"))
                                .withType("type")
                                .withId("1234")
                                .build()),
                        ACCEPTED.code(),
                        DataPlaneContract.Resource.newBuilder()
                                .setUid("1")
                                .addTopics("topic-name-42")
                                .setIngress(
                                        DataPlaneContract.Ingress.newBuilder().setPath("/broker-ns/broker-name5"))
                                .build()),
                new TestCase(
                        new ProducerRecord<>("topic-name-42", null, null),
                        "/broker-ns/broker-name5",
                        probeRequestSender(null),
                        OK.code(),
                        DataPlaneContract.Resource.newBuilder()
                                .setUid("1")
                                .addTopics("topic-name-42")
                                .setIngress(
                                        DataPlaneContract.Ingress.newBuilder().setPath("/broker-ns/broker-name5"))
                                .build()),
                new TestCase(
                        new ProducerRecord<>("topic-name-42", null, null),
                        "/broker-ns/broker-name5",
                        probeRequestSender(null),
                        NOT_FOUND.code(),
                        DataPlaneContract.Resource.newBuilder().build()));
    }

    private static Function<HttpRequest<Buffer>, Future<HttpResponse<Buffer>>> ceRequestSender(final CloudEvent event) {
        return request -> VertxMessageFactory.createWriter(request).writeBinary(event);
    }

    private static Function<HttpRequest<Buffer>, Future<HttpResponse<Buffer>>> probeRequestSender(
            final CloudEvent event) {
        return request -> request.method(HttpMethod.GET)
                .putHeader(
                        ControlPlaneProbeRequestUtil.PROBE_HEADER_NAME, ControlPlaneProbeRequestUtil.PROBE_HEADER_VALUE)
                .send();
    }

    static final class TestCase {

        final ProducerRecord<String, CloudEvent> record;
        final String path;
        final Function<HttpRequest<Buffer>, Future<HttpResponse<Buffer>>> requestSender;
        final int responseStatusCode;
        final DataPlaneContract.Resource resource;
        final int badRequestCount;
        final int produceEventCount;
        final boolean expectedDispatchLatency;

        TestCase(
                final ProducerRecord<String, CloudEvent> record,
                final String path,
                final Function<HttpRequest<Buffer>, Future<HttpResponse<Buffer>>> requestSender,
                final int responseStatusCode,
                final DataPlaneContract.Resource resource) {

            this.path = path;
            this.requestSender = requestSender;
            this.responseStatusCode = responseStatusCode;
            this.record = record;
            this.resource = DataPlaneContract.Resource.newBuilder(resource).build();

            int badRequestCount = 0;
            int produceEventsCount = 0;
            if (responseStatusCode == BAD_REQUEST.code()) {
                badRequestCount = 1;
            } else if (responseStatusCode == ACCEPTED.code()) {
                produceEventsCount = 1;
            }

            this.expectedDispatchLatency = responseStatusCode == ACCEPTED.code();

            this.badRequestCount = badRequestCount;
            this.produceEventCount = produceEventsCount;
        }

        DataPlaneContract.Contract getContract() {
            return DataPlaneContract.Contract.newBuilder()
                    .addResources(this.resource)
                    .build();
        }

        @Override
        public String toString() {
            return "TestCase{" + "path='" + path + '\'' + ", responseStatusCode=" + responseStatusCode + '}';
        }
    }
}
