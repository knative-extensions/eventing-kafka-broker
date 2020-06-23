FROM maven:3.6.3-openjdk-14 as builder

WORKDIR /app

COPY . .

RUN mvn clean verify -B
