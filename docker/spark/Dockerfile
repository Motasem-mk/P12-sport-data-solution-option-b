# # docker/spark/Dockerfile
FROM bitnamilegacy/spark:3.5.1

USER root

# CHANGED: install CA certificates (and Java CA store) + update them
# This is the most common fix for Maven Central SSLHandshakeException inside Java/Spark.
RUN apt-get update && apt-get install -y --no-install-recommends \
      curl \
      ca-certificates \
      ca-certificates-java \
      openssl \
  && update-ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# Keep your Python deps installation (unchanged)
COPY requirements.txt /tmp/requirements.txt
RUN /opt/bitnami/python/bin/pip install --no-cache-dir -r /tmp/requirements.txt

USER 1001
