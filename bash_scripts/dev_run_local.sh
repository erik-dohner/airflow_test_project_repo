docker run -it --platform linux/amd64 \
    -e SM_API_TOKEN=duVhzwcSRhDNfxxVlxPa2r.-DByisgtPf2xR00KnvMgTx8Rv6P1wmQUWQok2WzEvvSiORoYWJi9IMiif2mLdr3Bk2ThK5udOxbSxOow9X9YNrzoQ-yYOOZJhPyIG4zFv \
    -e SM_API_URL=https://api.surveymonkey.com/v3 \
    -e AC_API_KEY=857047432b24d896ad0389e6ab2e3c858d2c9b37180acdb2effcc134f6ba163b94b1e2d3 \
    -e AC_API_URL=https://lyricoperaofchicago.api-us1.com/api/3 \
    -e GOOGLE_APPLICATION_CREDENTIALS="/opt/airflow/gcp_service_account.json" \
    gcr.io/dbt-test/airflow-dev-image
