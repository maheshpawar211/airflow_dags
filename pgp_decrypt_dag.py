from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

# Variables (store secrets in Secret Manager or Airflow connections in production)
GCS_INPUT_BUCKET = Variable.get("gcs_input_bucket")
GCS_INPUT_FILE = Variable.get("gcs_input_file")   # e.g., "large_file.pgp"
GCS_OUTPUT_BUCKET = Variable.get("gcs_output_bucket")
GCS_OUTPUT_FILE = Variable.get("gcs_output_file") # e.g., "large_file.csv"
PGP_PASSPHRASE = Variable.get("pgp_passphrase")   # or use Secret Manager

# KubernetesPodOperator will run in a separate pod with gpg installed
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0
}

with DAG(
    dag_id="pgp_decrypt_large_file",
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
) as dag:

    decrypt_task = KubernetesPodOperator(
        task_id="pgp_decrypt_streaming",
        name="pgp-decrypt-pod",
        namespace="default",
        image="google/cloud-sdk:slim",  # lightweight image with gsutil; we'll install gpg
        cmds=["bash", "-c"],
        arguments=[
            f"""
            apt-get update && apt-get install -y gnupg && \
            echo "Downloading encrypted file from GCS..." && \
            gsutil cp gs://{GCS_INPUT_BUCKET}/{GCS_INPUT_FILE} /tmp/input.pgp && \
            echo "Decrypting file in streaming mode..." && \
            gpg --batch --yes --pinentry-mode loopback \
                --passphrase "{PGP_PASSPHRASE}" \
                --output /tmp/output_file \
                --decrypt /tmp/input.pgp && \
            echo "Uploading decrypted file to GCS..." && \
            gsutil cp /tmp/output_file gs://{GCS_OUTPUT_BUCKET}/{GCS_OUTPUT_FILE}
            """
        ],
        resources={
            "request_memory": "4Gi",
            "request_cpu": "2",
            "limit_memory": "8Gi",
            "limit_cpu": "4"
        },
        get_logs=True,
        is_delete_operator_pod=True  # clean up pod after run
    )

    decrypt_task







gsutil cat gs://$GCS_INPUT_BUCKET/$GCS_INPUT_FILE | \
gpg --batch --yes --pinentry-mode loopback --passphrase "$PGP_PASSPHRASE" \
| gsutil cp - gs://$GCS_OUTPUT_BUCKET/$GCS_OUTPUT_FILE
