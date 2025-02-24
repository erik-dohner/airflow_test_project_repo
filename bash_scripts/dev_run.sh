gcloud run deploy airflow-job \
    --image gcr.io/dbt-test/airflow-dev-image \
    --platform managed \
    --region us-central1 \
    --no-allow-unauthenticated \
    --memory=2Gi \
    --timeout=900 \
    --command "airflow" \
    --args "tasks", "test", "pipeline_run", "fetch_responses", "2025-02-20"
