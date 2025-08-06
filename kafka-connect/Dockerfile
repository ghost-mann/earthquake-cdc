# Start with Confluent's official Kafka Connect image. 7.5.0 is a good choice.
FROM confluentinc/cp-kafka-connect:7.5.0

# Copy the local connector zip files into the container's temporary directory.
COPY confluentinc-kafka-connect-jdbc-10.8.4.zip /tmp/jdbc.zip
COPY debezium-debezium-connector-mysql-3.1.2.zip /tmp/debezium.zip

# --- THE FIX: PART 1 ---
# Switch to the ROOT user temporarily to gain permissions for package installation.
USER root

#
# The main installation RUN command will now execute as root.
#
RUN set -x && \
    microdnf install -y unzip && \
    mkdir -p /usr/share/confluent-hub-components/jdbc-connector && \
    mkdir -p /usr/share/confluent-hub-components/debezium-mysql-connector && \
    unzip /tmp/jdbc.zip -d /usr/share/confluent-hub-components/jdbc-connector && \
    unzip /tmp/debezium.zip -d /usr/share/confluent-hub-components/debezium-mysql-connector && \
    rm /tmp/*.zip && \
    microdnf clean all

# --- THE FIX: PART 2 ---
# Switch back to the non-privileged 'appuser' for security.
# This ensures the container runs as a non-root user.
USER appuser