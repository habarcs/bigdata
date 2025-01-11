# spark/Dockerfile
FROM bitnami/spark:3.3.4

USER root

# Install Python 3.8 and dependencies
RUN apt-get update && apt-get install -y \
    python3.8 \
    python3.8-distutils \
    python3-pip && \
    apt-get clean && \
    ln -sf /usr/bin/python3.8 /usr/bin/python3 && \
    ln -sf /usr/bin/pip3 /usr/bin/pip


# (Optional) Install any python libraries you might need
RUN pip3 install --no-cache-dir pyspark==3.3.4

# Copy Postgres JDBC driver
COPY postgresql-42.7.4.jar /opt/spark/jars/

# Copy your PySpark script into /app
COPY spark.py /app/spark.py

# Ensure correct permissions
RUN chmod +x /app/spark.py

# Set the working directory
WORKDIR /opt

# Set the virtual environment as the default Python
ENV PATH="/opt/venv/bin:$PATH"


# but we will override the CMD in docker-compose
CMD ["/bin/bash"]

