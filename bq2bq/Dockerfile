FROM python:3.8-alpine

WORKDIR /opt/bumblebee

COPY . .
RUN ["pip", "install", "-r", "requirements.txt"]
RUN ["pip", "install", "opentelemetry-sdk", "opentelemetry-distro", "opentelemetry-exporter-otlp", "opentelemetry-instrumentation-system-metrics", "--no-cache-dir"]
ENTRYPOINT  [ "python3", "/opt/bumblebee/main.py"]



