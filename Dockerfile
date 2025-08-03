# Use the same version as your other Confluent Platform components for compatibility
FROM confluentinc/cp-kafka-connect:7.5.0

# Install the necessary connectors using the confluent-hub utility
# Note: You may want to check for the latest versions of these connectors on Confluent Hub

# 1. Install the Debezium connector for MySQL (which works for MariaDB)
RUN confluent-hub install debezium/debezium-connector-mysql:2.4.1 --no-prompt

# 2. Install the JDBC Sink connector to write to PostgreSQL
RUN confluent-hub install confluentinc/kafka-connect-jdbc:10.7.6 --no-prompt