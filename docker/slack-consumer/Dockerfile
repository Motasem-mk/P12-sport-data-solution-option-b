# docker/slack-consumer/Dockerfile
FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /app

# Install system dependencies if needed (kept minimal here)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies directly
# You can pin versions if you want, e.g. kafka-python==2.0.2, requests==2.32.3, etc.
RUN pip install --no-cache-dir \
    kafka-python \
    requests \
    python-dotenv

# Copy the Slack consumer code into the image
COPY slack_consumer.py .

# Default command
CMD ["python", "slack_consumer.py"]
