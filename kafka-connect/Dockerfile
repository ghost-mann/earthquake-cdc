# Start with Confluent's official Kafka Connect image, which includes 'confluent-hub'.
FROM confluentinc/cp-kafka-connect:7.5.0

# --- Install Connectors using the confluent-hub utility ---

# Set versions for clarity and easy updates.
# The Confluent JDBC connector is a general-purpose sink for any JDBC-compliant database.
ENV JDBC_SINK_VERSION=10.7.6

# Use confluent-hub to install the connectors.
# This is the recommended, idiomatic way to add connectors to this image.
# It automatically handles placing the connector JARs in the correct plugin path.
RUN confluent-hub install confluentinc/kafka-connect-jdbc:${JDBC_SINK_VERSION} --no-prompt
    
RUN confluent-hub install debezium/debezium-connector-mysql:latest --no-prompt