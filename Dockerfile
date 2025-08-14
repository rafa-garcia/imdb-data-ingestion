FROM postgres:16

# Install curl for downloading datasets
RUN apt-get update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

# Copy SQL initialization files
COPY sql/ /docker-entrypoint-initdb.d/

# Set environment variables for better performance
ENV POSTGRES_INITDB_ARGS="--data-checksums"
