FROM python:3.8-alpine

RUN apk add --no-cache \
    gcc \
    g++ \
    python3-dev \
    musl-dev \
    linux-headers

WORKDIR /opt/bumblebee

COPY . .
RUN ["pip", "install", "-r", "requirements.txt"]
RUN ["pip", "install", "--no-deps", "opentelemetry-sdk", "opentelemetry-distro", "opentelemetry-exporter-otlp", "opentelemetry-instrumentation-system-metrics", "--no-cache-dir"]
ENTRYPOINT  [ "python3", "/opt/bumblebee/main.py"]



