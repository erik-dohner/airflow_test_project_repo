gcloud run deploy airflow-job \
    --image gcr.io/dbt-test/airflow-dev-image \
    --platform managed \
    --region us-central1 \
    --no-allow-unauthenticated \
    --memory=2Gi \
    --timeout=900 \
    --command "airflow" \
    --args "tasks", "test", "pipeline_run", "fetch_responses", "2025-02-20" \
    --set-env-vars=SM_API_TOKEN=duVhzwcSRhDNfxxVlxPa2r.-DByisgtPf2xR00KnvMgTx8Rv6P1wmQUWQok2WzEvvSiORoYWJi9IMiif2mLdr3Bk2ThK5udOxbSxOow9X9YNrzoQ-yYOOZJhPyIG4zFv, \
SM_API_URL=https://api.surveymonkey.com/v3, \
AC_API_KEY=857047432b24d896ad0389e6ab2e3c858d2c9b37180acdb2effcc134f6ba163b94b1e2d3, \
AC_API_URL=https://lyricoperaofchicago.api-us1.com/api/3, \
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/gcp_service_account.json
