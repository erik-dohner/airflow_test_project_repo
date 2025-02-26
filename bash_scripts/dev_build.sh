docker build \
    --no-cache \
    --platform linux/amd64 \
    --build-arg APP_ENV_BUILD_ARG=dev \
    -t gcr.io/dbt-test/airflow-dev-image .