# app-code/Dockerfile
# app-code/Dockerfile
FROM python:3.9-slim

WORKDIR /app

# Copy requirements and install dependencies
# FIX 1: We must specify 'app-code/' because the files are in a subfolder of the build context.
COPY app-code/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Python scripts
# FIX 2: We must specify 'app-code/' for both scripts.
COPY app-code/producer.py .
COPY app-code/consumer.py .

# Set the entry point (this will be overridden by Kubernetes command)
ENTRYPOINT ["python"]