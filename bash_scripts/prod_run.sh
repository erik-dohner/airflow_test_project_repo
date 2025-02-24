gcloud run deploy airflow-job \
    --image gcr.io/dbt-test/airflow-prod-image \
    --service-account dbt-user@dbt-test-449821.iam.gserviceaccount.com \
    --region us-central1 \
    --platform managed \
    --no-allow-unauthenticated \
    --memory=2Gi \
    --timeout=900