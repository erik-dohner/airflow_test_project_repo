#airflow_test_project_repo

Need to install
    1. Docker
    2. Google CLI
    3. VS Code
    4. GitHub



Step to get docker image to GCP
    1. authenticate Google Cloud Registry for Dockert --> RUN gcloud auth configure-docker
    2. RUN dev_build.sh
    3. RUN docker push gcr.io/dbt-test/airflow-dev-image
