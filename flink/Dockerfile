FROM flink:1.20.1-scala_2.12-java17

# Flink ships without the Kafka connector; drop the SQL "fat jar" into lib/
RUN wget -q -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar
