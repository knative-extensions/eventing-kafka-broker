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

FROM quay.io/openshift-knative/17-jdk-centos7 as builder

WORKDIR /build

COPY /data-plane/pom.xml .
COPY /data-plane/.editorconfig .
COPY /data-plane/core/pom.xml core/pom.xml
COPY /data-plane/receiver/pom.xml receiver/pom.xml
COPY /data-plane/dispatcher/pom.xml dispatcher/pom.xml
COPY /data-plane/contract/pom.xml contract/pom.xml
COPY /data-plane/mvnw .
COPY /data-plane/.mvn/wrapper .mvn/wrapper
COPY /data-plane/docker/generate_jdk.sh .

# Install dependencies. Note: don't build a single submodule (recever or dispatcher) since it just slows down
# consecutive builds.
RUN ./mvnw install -am -DskipTests -Drelease -Dlicense.skip -Deditorconfig.skip --no-transfer-progress

COPY /data-plane/ .

RUN ./mvnw package -pl=receiver -Drelease -am -DskipTests -Deditorconfig.skip --no-transfer-progress

RUN ./generate_jdk.sh /build/receiver/target/receiver-1.0-SNAPSHOT.jar

RUN cp /build/receiver/target/receiver-1.0-SNAPSHOT.jar /app/app.jar

# We use the generated JDK from the "builder" image, so we can just go with the ubi-minimal
FROM registry.access.redhat.com/ubi8/ubi-minimal:latest as running

COPY --from=builder /app /app

# Configure PATH
ENV PATH="/app/jdk/bin:${PATH}"
