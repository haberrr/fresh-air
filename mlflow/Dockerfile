FROM ghcr.io/mlflow/mlflow:v2.2.2

# Install tini
ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

# Install additional libraries
RUN pip install --no-cache-dir psycopg2-binary google-cloud-storage

# Expose port
EXPOSE 8000

# Prepare start script
COPY run_server.sh ./
RUN chmod +x run_server.sh

# Run
ENTRYPOINT ["/tini", "--", "./run_server.sh"]
