# FROM flink:1.20.0

# # download postgresql JDBC and flink connectors
# RUN curl --output /opt/flink-connector-jdbc.jar "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar"
# RUN curl --output /opt/flink-sql-connector-kafka.jar "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar"
# RUN curl --output /opt/postgresql.jar "https://jdbc.postgresql.org/download/postgresql-42.7.4.jar"

# # install python3 and pip3
# RUN apt update && \
#     apt install -y software-properties-common && \
#     add-apt-repository ppa:deadsnakes/ppa && \
#     apt update && \
#     apt install -y python3-pip && \
#     apt install -y python3.8 python3.8-distutils && \
#     python3.8 -m pip install pip3.8

# # install PyFlink
# COPY requirements.txt /
# RUN python3.8 -m pip3.8 install -r /requirements.txt

# # copy python files
# COPY __main__.py /opt/
# COPY swing.py /opt/

FROM flink:1.20.0

# Set working directory
WORKDIR /opt

# Download PostgreSQL JDBC and Flink connectors
RUN curl --output flink-connector-jdbc.jar "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar" && \
    curl --output flink-sql-connector-kafka.jar "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar" && \
    curl --output postgresql.jar "https://jdbc.postgresql.org/download/postgresql-42.7.4.jar"

# Install required tools and dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    software-properties-common \
    wget \
    build-essential \
    libffi-dev \
    libssl-dev \
    zlib1g-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
    
RUN python3.8 --version


# Install Python 3.8.10 manually
RUN wget https://www.python.org/ftp/python/3.8.10/Python-3.8.10.tgz && \
    tar xvf Python-3.8.10.tgz && \
    cd Python-3.8.10 && \
    ./configure --enable-optimizations && \
    make -j$(nproc) && \
    make altinstall && \
    cd .. && rm -rf Python-3.8.10 Python-3.8.10.tgz

# Upgrade pip and install Python dependencies
COPY requirements.txt /opt/
RUN python3.8 -m ensurepip && \
    python3.8 -m pip install --upgrade pip && \
    python3.8 -m pip install --no-cache-dir -r requirements.txt

# Copy Python files
COPY __main__.py swing.py /opt/

# Set default command
CMD ["python3.8", "/opt/__main__.py"]
