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

FROM maven:3.6.3-openjdk-14 as builder

WORKDIR /app

COPY pom.xml .
COPY core/pom.xml core/pom.xml
COPY receiver/pom.xml receiver/pom.xml
COPY dispatcher/pom.xml dispatcher/pom.xml
COPY generated/pom.xml generated/pom.xml
COPY checkstyle/suppression.xml checkstyle/suppression.xml

RUN mvn install

COPY . .

RUN mvn package

FROM adoptopenjdk:14-jre-hotspot as running
RUN groupadd -g 999 appuser && useradd -r -u 999 -g appuser appuser
RUN mkdir /tmp/vertx-cache && \
      chown -R appuser:appuser /tmp/vertx-cache && \
      mkdir /app && \
      chown -R appuser:appuser /app
USER appuser
WORKDIR /app
COPY --from=builder /app/dispatcher/target/dispatcher-1.0-SNAPSHOT.jar /app/app.jar
