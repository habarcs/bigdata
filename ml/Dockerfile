# Use the Bitnami Spark base image
FROM bitnami/spark:3.3.4

# Install Python dependencies from requirements.txt
COPY requirements.txt /opt/requirements.txt
RUN pip install --no-cache-dir -r /opt/requirements.txt

# Copy the PostgreSQL JDBC driver to the Spark jars directory
COPY postgresql-42.7.4.jar /opt/bitnami/spark/jars/postgresql-42.7.4.jar

# Copy the application scripts
COPY spark_api.py /app/spark_api.py
COPY spark.py /app/spark.py

# Expose the Flask API port
EXPOSE 9003

# Set the default command to run the Flask API
CMD ["python", "/app/spark_api.py"]
