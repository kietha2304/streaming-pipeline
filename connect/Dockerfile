# Debezium Connect image + the Confluent S3 sink plugin, so one Connect cluster runs both
# the Postgres source (Debezium) and the S3 sink.

FROM alpine:3.20 AS s3-plugin
ARG S3_SINK_VERSION=10.5.17
RUN apk add --no-cache curl unzip \
 && curl -fsSL -o /tmp/s3-sink.zip \
      "https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-s3/versions/${S3_SINK_VERSION}/confluentinc-kafka-connect-s3-${S3_SINK_VERSION}.zip" \
 && mkdir /plugins \
 && unzip -q /tmp/s3-sink.zip -d /plugins

FROM quay.io/debezium/connect:2.7.3.Final
# Every folder under /kafka/connect is loaded as a plugin
COPY --from=s3-plugin /plugins/ /kafka/connect/
