from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago
from jinja2 import StrictUndefined

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
}

dag = DAG(
    "steven-flow",
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
    template_undefined=StrictUndefined,
)

class CustomSparkKubernetesOperator(SparkKubernetesOperator):
    template_ext = ('.yaml', '.yaml.j2')
    template_fields = ('application_file',)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

extract = CustomSparkKubernetesOperator(
    task_id="extract",
    namespace="devops",
    application_file="extract.yaml.j2",
    kubernetes_conn_id="kubernetes_default",
    in_cluster=True,
    do_xcom_push=False,
    get_logs=True,
    log_events_on_failure=True,
    params={
        "job_name": "extract",
        "driverCores": 1,
        "driverMemory": "1G",
        "executorCores": 1,
        "executorMemory": "1G",
        "executorInstances": 1,
        "custSA": "devops"
    },
    dag=dag,
)
transform1 = CustomSparkKubernetesOperator(
    task_id="transform1",
    namespace="devops",
    application_file="transform1.yaml.j2",
    kubernetes_conn_id="kubernetes_default",
    in_cluster=True,
    do_xcom_push=False,
    get_logs=True,
    log_events_on_failure=True,
    params={
        "job_name": "transform1",
        "driverCores": 2,
        "driverMemory": "2G",
        "executorCores": 2,
        "executorMemory": "2G",
        "executorInstances": 2,
        "custSA": "devops"
    },
    dag=dag,
)
transform2 = CustomSparkKubernetesOperator(
    task_id="transform2",
    namespace="devops",
    application_file="transform2.yaml.j2",
    kubernetes_conn_id="kubernetes_default",
    in_cluster=True,
    do_xcom_push=False,
    get_logs=True,
    log_events_on_failure=True,
    params={
        "job_name": "transform2",
        "driverCores": 2,
        "driverMemory": "2G",
        "executorCores": 2,
        "executorMemory": "2G",
        "executorInstances": 2,
        "custSA": "devops"
    },
    dag=dag,
)
store = CustomSparkKubernetesOperator(
    task_id="store",
    namespace="devops",
    application_file="store.yaml.j2",
    kubernetes_conn_id="kubernetes_default",
    in_cluster=True,
    do_xcom_push=False,
    get_logs=True,
    log_events_on_failure=True,
    params={
        "job_name": "store",
        "driverCores": 1,
        "driverMemory": "1G",
        "executorCores": 1,
        "executorMemory": "1G",
        "executorInstances": 1,
        "custSA": "devops"
    },
    dag=dag,
)

# Dependencies
extract >> [transform1, transform2] >> store