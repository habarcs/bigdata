FROM flink:1.20.0

# download postgresql JDBC and flink connectors
RUN curl --output /opt/flink-sql-connector-postgresql.jar "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-postgres-cdc/3.2.1/flink-sql-connector-postgres-cdc-3.2.1.jar"
RUN curl --output /opt/flink-sql-connector-kafka.jar "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar"

# install python3 and pip3
RUN apt-get update -y && \
apt-get install -y python3 python3-pip python3-dev && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

# install PyFlink
COPY requirements.txt /
RUN pip3 install -r /requirements.txt

# copy python files
COPY __main__.py /opt/