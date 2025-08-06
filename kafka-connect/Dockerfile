# Start with Confluent's official Kafka Connect image. 7.5.0 is a good choice.
FROM confluentinc/cp-kafka-connect:7.5.0

# Copy the local connector zip files into the container's temporary directory.
COPY confluentinc-kafka-connect-jdbc-10.8.4.zip /tmp/jdbc.zip
COPY debezium-debezium-connector-mysql-3.1.2.zip /tmp/debezium.zip

# Switch to the ROOT user temporarily to gain permissions for package installation.
USER root

#
# SIMPLIFIED & CORRECTED RUN COMMAND:
# This unzips the archives directly into the components directory. Kafka Connect
# will scan this path and find the subdirectory for each connector automatically.
#
RUN set -x && \
    microdnf install -y unzip && \
    unzip /tmp/jdbc.zip -d /usr/share/confluent-hub-components/ && \
    unzip /tmp/debezium.zip -d /usr/share/confluent-hub-components/ && \
    rm /tmp/*.zip && \
    microdnf clean all

# Switch back to the non-privileged 'appuser' for security.
USER appuser