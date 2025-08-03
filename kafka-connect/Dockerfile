# Start with Confluent's official Kafka Connect image, version 7.5.0
FROM confluentinc/cp-kafka-connect:7.5.0

# Switch to 'root' user to install system packages and Kafka connectors.
USER root

# Install all necessary utilities for manual downloads: wget, tar (for Debezium), and unzip (for JDBC).
# Combining into a single layer is more efficient.
RUN microdnf install -y wget tar unzip && microdnf clean all

# --- Part 1: Manually install the Debezium community connector ---
# This is the most reliable way for non-Confluent connectors.
ENV DEBEZIUM_VERSION=2.5.0.Final
RUN mkdir -p /usr/share/confluent-hub-components/debezium-connector-mysql && \
    wget -O /tmp/debezium-mysql.tar.gz https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/${DEBEZIUM_VERSION}/debezium-connector-mysql-${DEBEZIUM_VERSION}-plugin.tar.gz && \
    tar -xvf /tmp/debezium-mysql.tar.gz -C /usr/share/confluent-hub-components/debezium-connector-mysql && \
    rm /tmp/debezium-mysql.tar.gz

# --- Part 2: Manually install the Confluent JDBC connector (bypassing the buggy confluent-hub client) ---
# We use this method because the confluent-hub tool in the base image is failing to find the component.
ENV JDBC_SINK_VERSION=10.7.7
RUN wget -O /tmp/jdbc-sink.zip "https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc/kafka-connect-jdbc/versions/${JDBC_SINK_VERSION}/confluentinc-kafka-connect-jdbc-${JDBC_SINK_VERSION}.zip" && \
    # The -d flag correctly places the unzipped contents into the component directory for the classpath scanner.
    unzip /tmp/jdbc-sink.zip -d /usr/share/confluent-hub-components/ && \
    rm /tmp/jdbc-sink.zip

# Switch back to the non-root 'appuser' for running Kafka Connect, following security best practices.
USER appuser