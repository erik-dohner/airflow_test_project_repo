docker build \
    --build-arg APP_ENV_BUILD_ARG=prod \
    -t gcr.io/dbt-test/airflow-prod-image .
