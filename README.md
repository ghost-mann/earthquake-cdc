### Real-Time Earthquake CDC Pipeline Project

-- spark-submit --jars mariadb-java-client-3.3.1.jar main.py

### mariadb credentials
mariadb -u root -p 
password


endpoint > mariadb > kafka > postgresql > grafana

1.**Ingestion Service** (e.g., Python Script): A process that continuously fetches new data from the Earthquake API and writes it to MariaDB. MariaDB becomes our primary, operational database.
2.**MariaDB + Binlog**: Your source database, but now configured with binary logging enabled. This log is a record of all data changes.
3.**Debezium Connector** (Source): A Kafka Connect plugin that reads MariaDB's binlog, converts each change into a structured JSON message, and sends it to a Kafka topic.
4.**Apache Kafka**: The central nervous system. A distributed, durable message broker that decouples the source (Debezium) from the sink (the PostgreSQL writer).
5.**Kafka Connect JDBC Sink** (Sink): Another Kafka Connect plugin that subscribes to the Kafka topic, reads the change events, and applies them to the target PostgreSQL database.


### registering source connector
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" \
http://localhost:8083/connectors/ \
-d @debezium-mariadb-source-config.json

### registering sink connector
