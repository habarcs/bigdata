# Use the official Apache Flink image
FROM apache/flink:1.20.0

# Install Python 3.8 and dependencies
RUN apt-get update && apt-get install -y \
    python3.8 \
    python3.8-distutils \
    python3-pip && \
    apt-get clean && \
    ln -sf /usr/bin/python3.8 /usr/bin/python3 && \
    ln -sf /usr/bin/pip3 /usr/bin/pip

# Set up a virtual environment for Python 3.8
RUN pip install virtualenv && \
    virtualenv /opt/venv && \
    /opt/venv/bin/pip install --upgrade pip

# Copy requirements.txt and install Python dependencies
COPY requirements.txt /opt/requirements.txt
RUN /opt/venv/bin/pip install -r /opt/requirements.txt

# Copy your Python script into the container
COPY swing.py /opt/swing.py

# Set the working directory
WORKDIR /opt

# Set the virtual environment as the default Python
ENV PATH="/opt/venv/bin:$PATH"
