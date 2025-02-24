gcloud scheduler jobs create http airflow-trigger \
    --schedule "0 8 * * *" \ 
    # Need to input URL once I have create the cloud run service
    # args are 
    # - service name
    # - random id
    # - region
    # - .a.run.app = domain specific to Google Cloud Run
    # - trigger = the name of the endpoint assocaited with the flask function to start up the webserver
    --uri "https://[SERVICE_NAME]-[RANDOM_ID]-[REGION].a.run.app/trigger" \
    --http-method=POST \
    --oidc-service-account-email "dbt-user@dbt-test-449821.iam.gserviceaccount.com"