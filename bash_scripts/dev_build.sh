docker build \
    --build-arg APP_ENV_BUILD_ARG=dev \
    --build-arg GCP_CREDENTIALS_BUILD_ARG="/opt/airflow/gcp_service_account.json" \
    -t gcr.io/dbt-test/airflow-dev-image .