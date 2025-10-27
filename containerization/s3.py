#!/usr/bin/env python3
import pika, json, os, subprocess, sys, yaml
from datetime import datetime

from upload_to_s3 import upload_outputs_to_s3


def save_yaml(dag_name, data):
    os.makedirs("declarations", exist_ok=True)
    path = f"declarations/{dag_name}.yaml"

    base = {
        "spark_dag_name": dag_name,
        "spark_namespace": data.get("namespace", "devops"),
        "schedule_interval": data.get("schedule_interval", "@once"),
        "task_order": [t["task_id"] for t in data.get("tasks", [])],
        "tasks": []
    }

    for t in data.get("tasks", []):
        base["tasks"].append({
            "task_id": t["task_id"],
            "params": {
                "job_name": t["task_id"],
                "spark_application_file": t.get("spark_application_file", dag_name),
                "driverCores": str(t.get("driverCores", 1)),
                "driverMemory": str(t.get("driverMemory", "1G")),
                "executorCores": str(t.get("executorCores", 1)),
                "executorMemory": str(t.get("executorMemory", "1G")),
                "executorInstances": str(t.get("executorInstances", 1)),
                "custSA": t.get("custSA", "devops")
            }
        })

    with open(path, "w") as f:
        yaml.safe_dump(base, f)

    print(f"[+] YAML saved: {path}")
    return path


def run_dag_builder(declaration_path):
    try:
        subprocess.run(["python3", "quinto.py"], check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"[❌] DAG builder failed: {e}")
        return False


def callback(ch, method, properties, body):
    data = json.loads(body)
    now = datetime.now()
    print(f"[x] Received at {now} -> {json.dumps(data, indent=2)}")

    dag_name = data["dag_name"]
    declaration_path = save_yaml(dag_name, data)

    success = run_dag_builder(declaration_path)
    if success:
        print(f"[✔] DAG generated for {dag_name}")

        print("[☁️] Uploading outputs to S3...")
        upload_outputs_to_s3(
            bucket_name=os.getenv("S3_BUCKET"),
            endpoint_url=os.getenv("S3_ENDPOINT_URL"),
            access_key=os.getenv("S3_ACCESS_KEY"),
            secret_key=os.getenv("S3_SECRET_KEY"),
        )

        print("[🧹] Cleaning up local files...")
        subprocess.run(["rm", "-rf", "outputs"], check=True)
        subprocess.run(["rm", "-rf", "declarations"], check=True)
        os.makedirs("outputs", exist_ok=True)
        os.makedirs("declarations", exist_ok=True)

        print(f"[✅] Cleanup complete for {dag_name}")

    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    rabbit_host = os.getenv('RABBITMQ_HOST', 'localhost')
    rabbit_port = int(os.getenv('RABBITMQ_PORT', 5672))
    rabbit_user = os.getenv('RABBITMQ_USER', 'guest')
    rabbit_pass = os.getenv('RABBITMQ_PASS', 'guest')
    rabbit_queue = os.getenv('RABBITMQ_QUEUE_DAG', 'dag_builder')

    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=rabbit_host,
        port=rabbit_port,
        credentials=pika.PlainCredentials(rabbit_user, rabbit_pass)
    ))

    channel = connection.channel()
    channel.queue_declare(queue=rabbit_queue, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=rabbit_queue, on_message_callback=callback)

    print(f"[*] Waiting for DAG build messages on queue '{rabbit_queue}' ...")
    channel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted")
        sys.exit(0)