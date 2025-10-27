import yaml
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

# Direktori utama
BASE_DIR = Path(__file__).parent
DECLARATIONS_DIR = BASE_DIR / "declarations"
DAGS_DIR = BASE_DIR / "dags"
TEMPLATE_FILE = "dag_template.py.j2"

# Pastikan folder output ada
DAGS_DIR.mkdir(exist_ok=True)

# Setup Jinja2
env = Environment(loader=FileSystemLoader(str(BASE_DIR)), trim_blocks=True, lstrip_blocks=True)
template = env.get_template(TEMPLATE_FILE)

# Loop semua file YAML
for yaml_file in DECLARATIONS_DIR.glob("*.yaml"):
    with open(yaml_file) as f:
        data = yaml.safe_load(f)

    # Konversi nilai int untuk field tertentu
    for task in data["tasks"]:
        for key in ["driverCores", "executorCores", "executorInstances"]:
            try:
                task["params"][key] = int(task["params"][key])
            except ValueError:
                pass

    # Render template
    output = template.render(**data)

    # Simpan file hasil generate
    output_path = DAGS_DIR / f"{data['spark_dag_name']}.py"
    output_path.write_text(output)

    print(f"✅ Generated DAG: {output_path}")
