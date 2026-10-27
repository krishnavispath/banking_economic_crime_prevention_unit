from airflow import DAG
from airflow.operators.empty import EmptyOperator

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator
)

from datetime import datetime

# =========================================================
# GCP CONFIG
# =========================================================

PROJECT_ID = "project-03f569c3-1269-4daa-8bb" 
REGION = "us-central1"  
CLUSTER_NAME = "demo-cluster"
COMPOSER_BUCKET = "us-central1-bankingproj-3b235843-bucket" ##CHANGE THIS TO YOUR COMPOSER BUCKET NAME

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "e2-standard-2",
        "disk_config": {
            "boot_disk_type": "pd-standard",
            "boot_disk_size_gb": 50,
        },
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "e2-standard-2",
        "disk_config": {
            "boot_disk_type": "pd-standard",
            "boot_disk_size_gb": 50,
        }
    }
}
# =========================================================
# PYSPARK FILE
# =========================================================

PYSPARK_URI = (
    f"gs://{COMPOSER_BUCKET}/data/pipeline/CrimeFlowSpark.py"
    )
# =========================================================
# DATAPROC JOB
# =========================================================

PYSPARK_JOB = {

    "reference": {
        "project_id": PROJECT_ID
    },

    "placement": {
        "cluster_name": CLUSTER_NAME
    },

    "pyspark_job": {
        "main_python_file_uri": PYSPARK_URI
    }
}

# =========================================================
# DAG
# =========================================================

with DAG(
    dag_id="crimeflow_pipeline_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["crimeflow", "dataproc", "pyspark"]
) as dag:

    start_task = EmptyOperator(
        task_id="start_pipeline"
    )

    create_cluster = DataprocCreateClusterOperator(
    task_id="create_cluster",
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    cluster_config=CLUSTER_CONFIG,
    )

    run_pipeline = DataprocSubmitJobOperator(
        task_id="run_crimeflow_pipeline",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID

    )

    delete_cluster = DataprocDeleteClusterOperator(
    task_id="delete_cluster",
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    trigger_rule="all_done",   # Deletes even if job fails
    )

    end_task = EmptyOperator(
        task_id="end_pipeline"
    )

    start_task >> create_cluster >> run_pipeline >> delete_cluster >> end_task

