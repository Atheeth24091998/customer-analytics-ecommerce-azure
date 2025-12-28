from pathlib import Path

folders = [
    "data/bronze",
    "data/silver",
    "data/gold",
    "notebooks",
    "src/api",
    "src/models",
    "scripts",
    "models",
    "tests",
    "config",
    "powerbi",
    "logs"
]

files = [
    "src/api/main.py",
    "src/data_ingestion.py",
    "src/preprocessing.py",
    "src/gold_layer.py",
    "src/azure_upload.py",
    "scripts/run_pipeline.py",
    "scripts/train_models.py",
    "config/settings.yaml",
    "config/logging.conf",
    "Dockerfile",
    "docker-compose.yml",
    "Makefile",
    "requirements.txt",
    "README.md",
    ".gitignore"
]

for folder in folders:
    Path(folder).mkdir(parents=True, exist_ok=True)

for file in files:
    Path(file).touch(exist_ok=True)

