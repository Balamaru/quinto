# DAG Builder

Dag builder to build a dag based on yaml value as input, references by [QuintoAndar](https://medium.com/quintoandar-tech-blog/building-data-pipelines-effortlessly-with-a-dag-builder-for-apache-airflow-2f5f307fb781), created dags need to store to dags folder that airflow can read it, if airflow installed in kubernetes by helm, dags can store in airflow-scheduler pod at directory **/opt/airflow/dags**, that directory can syncronize with git repo (gitsync) or s3 bucket (objisync).

There're 2 kind how to run, 
## 1. Manually Run 
Write your own dag value in **/declarations/** (name of declaration and task must be unique), and running with
```bash
python main.py
```
Look outpus in **/output/**, thers is a dag and spark application would be added, then need to move the outputs to the airflow dags repository.

## 2. Containerization
In this kind of run, there're some feature added
- Dinamically declaration file where can be written by using RabbitMQ queue
- Output of dag and sparkapplication file automate store to s3 bucket
- Created dag and sparkapplication who stored in s3 bucket will be automate delete

By using this kind of run, user need to have
- Own a s3 bucket it self
- Syncronous beetwen dags folder with s3 bucket (running airflow with objisync)
- Running RabbitMQ

Run with Docker
```sh
docker run --detach --name quinto --restart=unless-stopped --env 'RABBITMQ_HOST=127.0.0.1' --env 'RABBITMQ_PORT=5672' --env 'RABBITMQ_USER=guest' --env 'RABBITMQ_PASS=guest' --env 'RABBITMQ_QUEUE_DAG=dag_builder' --env 'S3_BUCKET=dags' --env 'S3_ENDPOINT_URL=https://s3-url' --env 'S3_ACCESS_KEY=AKIA.......' --env 'S3_SECRET_KEY=sEbHbjGHB............' balamaru/quinto:1.1
```

Run in kubernetes
```sh
kubectl apply -f containerization/quinto-builder.yaml
```

RabbitMQ payload
```json
{
  "dag_name": "demo",
  "namespace": "spark",
  "schedule_interval": "@daily",
  "task_order": ["demo"],
  "tasks": [
    {
      "task_id": "demo",
      "spark_application_file": "demo",
      "driverCores": 1,
      "driverMemory": "1G",
      "executorCores": 1,
      "executorMemory": "2G",
      "executorInstances": 1,
      "custSA": "spark"
    }
  ]
}
```