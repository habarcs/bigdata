# Use the Bitnami Spark base image
FROM bitnami/spark:3.3.4

# Switch to root user
USER root

# Install Python 3.8 manually without using add-apt-repository
RUN apt-get update && apt-get install -y \
    wget \
    build-essential \
    zlib1g-dev \
    libffi-dev \
    libssl-dev \
    libbz2-dev \
    libreadline-dev \
    libsqlite3-dev && \
    wget https://www.python.org/ftp/python/3.8.16/Python-3.8.16.tgz && \
    tar -xvf Python-3.8.16.tgz && \
    cd Python-3.8.16 && \
    ./configure --enable-optimizations && \
    make && make install && \
    cd .. && rm -rf Python-3.8.16 Python-3.8.16.tgz && \
    ln -sf /usr/local/bin/python3.8 /usr/bin/python && \
    ln -sf /usr/local/bin/pip3.8 /usr/bin/pip

# Install Python dependencies from requirements.txt
COPY requirements.txt /opt/requirements.txt
RUN pip install --no-cache-dir -r /opt/requirements.txt

# Copy the PostgreSQL JDBC driver to the Spark jars directory
COPY postgresql-42.7.4.jar /opt/bitnami/spark/jars/postgresql-42.7.4.jar

# Copy the application scripts
COPY spark_api.py /app/spark_api.py
COPY spark.py /app/spark.py

# Expose the Flask API port
EXPOSE 4040

# Set the default command to run the Flask API
CMD ["python", "/app/spark_api.py"]
