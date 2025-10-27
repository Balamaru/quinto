import yaml
import os
from jinja2 import Environment, FileSystemLoader

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = os.path.join(BASE_DIR, "templates")
DECLARATIONS_DIR = os.path.join(BASE_DIR, "declarations")
OUTPUT_DAG_DIR = os.path.join(BASE_DIR, "outputs/dags")
OUTPUT_SPARKAPP_DIR = os.path.join(BASE_DIR, "outputs/spark_apps")

os.makedirs(OUTPUT_DAG_DIR, exist_ok=True)
os.makedirs(OUTPUT_SPARKAPP_DIR, exist_ok=True)

# Load Jinja2 environment
env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))

dag_template = env.get_template("dag_template.py.j2")
sparkapp_template = env.get_template("sparkapplication.yaml.j2")

for yaml_file in os.listdir(DECLARATIONS_DIR):
    if not yaml_file.endswith(".yaml"):
        continue

    with open(os.path.join(DECLARATIONS_DIR, yaml_file)) as f:
        dag_def = yaml.safe_load(f)

    dag_name = dag_def["spark_dag_name"]

    # ✅ Generate DAG
    dag_output = dag_template.render(**dag_def)
    with open(os.path.join(OUTPUT_DAG_DIR, f"{dag_name}.py"), "w") as f:
        f.write(dag_output)
    print(f"✅ DAG generated: {dag_name}.py")

    # ✅ Generate SparkApplications (one per task)
    for task in dag_def["tasks"]:
        task_yaml = sparkapp_template.render(params=task["params"])
        task_file_name = f"{task['params']['spark_application_file']}.yaml"
        with open(os.path.join(OUTPUT_SPARKAPP_DIR, task_file_name), "w") as f:
            f.write(task_yaml)
        print(f"  └─ SparkApplication: {task_file_name}")
