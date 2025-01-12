# spark/Dockerfile
FROM bitnami/spark:3.3.4

USER root

# Install Python 3.8 manually without using add-apt-repository
RUN apt-get update && apt-get install -y \
    wget \
    build-essential \
    cron \
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

# Install PySpark
RUN pip install --no-cache-dir pyspark==3.3.4

# Copy Postgres JDBC driver to the Spark jars directory
COPY postgresql-42.7.4.jar /opt/bitnami/spark/jars/postgresql-42.7.4.jar

# Copy requirements.txt
COPY requirements.txt /opt/requirements.txt

# Install Python packages from requirements.txt
RUN pip install --no-cache-dir -r /opt/requirements.txt

# Copy PySpark script into /app
COPY spark.py /app/spark.py

# Add the cron job
RUN echo "*/5 * * * * /usr/bin/spark-submit --jars /opt/spark/jars/postgresql-42.7.4.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4 /app/spark.py > /var/log/spark-job.log 2>&1" > /etc/cron.d/spark-cron && \
    chmod 0644 /etc/cron.d/spark-cron && \
    crontab /etc/cron.d/spark-cron

# Ensure cron runs in the foreground
CMD ["cron", "-f"]
