FROM python:3.8-alpine

WORKDIR /opt/bumblebee

COPY . .
RUN ["pip", "install", "-r", "requirements.txt"]

ENTRYPOINT  [ "python3", "/opt/bumblebee/main.py"]
