FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    openssh-client \
    curl \
    git \
    build-essential \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir pika==1.3.2 PyYAML==6.0 requests boto3==1.35.95

RUN mkdir -p /app/declarations /app/outputs/dags /app/outputs/spark_apps

CMD ["python", "s3.py"]