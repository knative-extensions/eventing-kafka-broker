# Copyright 2020 The Knative Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ARG JAVA_IMAGE
ARG BASE_IMAGE

FROM ${JAVA_IMAGE} as builder

ARG APP_DIR
ARG APP_JAR

WORKDIR /app

# https://github.com/AdoptOpenJDK/openjdk-docker/issues/260
RUN apt-get update && apt-get install -y binutils

COPY pom.xml .
COPY .editorconfig .
COPY core/pom.xml core/pom.xml
COPY receiver/pom.xml receiver/pom.xml
COPY dispatcher/pom.xml dispatcher/pom.xml
COPY contract/pom.xml contract/pom.xml
COPY tests/pom.xml tests/pom.xml
COPY ./mvnw .
COPY ./.mvn/wrapper .mvn/wrapper
COPY docker/generate_jdk.sh .

RUN ./mvnw install -pl=${APP_DIR} -am -DskipTests -Deditorconfig.skip --no-transfer-progress

COPY . .

RUN ./mvnw package -pl=${APP_DIR} -am -DskipTests -Deditorconfig.skip --no-transfer-progress

RUN ./generate_jdk.sh /app/${APP_DIR}/target/${APP_JAR}

FROM ${BASE_IMAGE} as running

ARG APP_DIR
ARG APP_JAR

# Create appuser and directories
RUN groupadd -g 999 appuser && useradd -r -u 999 -g appuser appuser && \
  mkdir /tmp/vertx-cache && chown -R appuser:appuser /tmp/vertx-cache && \
  mkdir /app

# Copy jar and jdk inside /app
COPY --from=builder /app/jdk /app/jdk
COPY --from=builder /app/${APP_DIR}/target/${APP_JAR} /app/app.jar
RUN chown -R appuser:appuser /app

# Set appuser and configure PATH
USER appuser
WORKDIR /app
ENV PATH="/app/jdk/bin:${PATH}"
