# DAG Builder

Dag builder to build a dag based on yaml value as input, references by [QuintoAndar](https://medium.com/quintoandar-tech-blog/building-data-pipelines-effortlessly-with-a-dag-builder-for-apache-airflow-2f5f307fb781), created dags need to store to dags folder that airflow can read it, if airflow installed in kubernetes by helm, dags can store in airflow-scheduler pod at directory **/opt/airflow/dags**, that directory can syncronize with git repo (gitsync) or s3 bucket (objisync).

Write your own dag value in **/declarations/** (name of declaration and task must be unique), and running with
```bash
python main.py
```

Look outpus in **/output/**, thers is a dag and spark application would be added.