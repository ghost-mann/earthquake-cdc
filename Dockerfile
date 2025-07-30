# Start with Confluent's official Kafka Connect image, which includes 'confluent-hub'.
FROM confluentinc/cp-kafka-connect:7.5.0

ENV DEBEZIUM_VERSION=2.5.0.Final
RUN mkdir -p /usr/share/java/debezium-connector-postgres && \
    wget -O /tmp/debezium.tar.gz https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz && \
    tar -xvf /tmp/debezium.tar.gz -C /usr/share/java/debezium-connector-postgres && \
    rm /tmp/debezium.tar.gz