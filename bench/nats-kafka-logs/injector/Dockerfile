FROM python:3.12-slim
RUN pip install --no-cache-dir "nats-py>=2.9,<3" "confluent-kafka>=2.5,<3"
COPY inject.py /inject.py
ENTRYPOINT ["python", "/inject.py"]
