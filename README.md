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


Steps to run on another local machine 
    set env variables manually
        ENV DBT_PROFILES_DIR="dbt/profiles.yml"
        ENV DBT_PROJECT_DIR="dbt/dbt_pipeline/dbt_project.yml"
        ENV GOOGLE_APPLICATION_CREDENTIALS=gcp_service_account.json


Preparing for public sharing
- remember to add these files to .gitignore
    - sm_api_config.json
    - survey_details.json
    - activecampaign_api_config.json
