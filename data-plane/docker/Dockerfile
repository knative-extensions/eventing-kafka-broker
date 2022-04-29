#
# Copyright © 2018 Knative Authors (knative-dev@googlegroups.com)
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
#

ARG JAVA_IMAGE
ARG BASE_IMAGE

FROM ${JAVA_IMAGE} as builder

ARG APP_DIR
ARG APP_JAR

WORKDIR /build

COPY pom.xml .
COPY .editorconfig .
COPY core/pom.xml core/pom.xml
COPY receiver/pom.xml receiver/pom.xml
COPY dispatcher/pom.xml dispatcher/pom.xml
COPY contract/pom.xml contract/pom.xml
COPY ./mvnw .
COPY ./.mvn/wrapper .mvn/wrapper
COPY docker/generate_jdk.sh .

# Install dependencies. Note: don't build a single submodule (recever or dispatcher) since it just slows down
# consecutive builds.
RUN ./mvnw install -am -DskipTests -Drelease -Dlicense.skip -Deditorconfig.skip --no-transfer-progress

COPY . .

RUN ./mvnw package -pl=${APP_DIR} -Drelease -am -DskipTests -Deditorconfig.skip --no-transfer-progress

RUN ./generate_jdk.sh /build/${APP_DIR}/target/${APP_JAR}

RUN cp /build/${APP_DIR}/target/${APP_JAR} /app/app.jar

FROM ${BASE_IMAGE} as running

COPY --from=builder /app /app

# Configure PATH
# ENV PATH="/app/jdk/bin:${PATH}"
