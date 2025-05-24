FROM bitnami/spark:3.4.1

USER root

# Install Python + pip
RUN apt-get update && apt-get install -y python3 python3-pip

# Install required Python packages
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt
