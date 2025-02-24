# select the base image for airflow
FROM apache/airflow:2.7.2

# Set the working directory in the container
WORKDIR /opt/airflow

# Copy necessary files into the container
# this will move everything from the airflow_test_project folder into the containers airflow folder
# the dags folder will then be in the default spot, as the airflow config file that comes with the basic 
# image above will expect to find the dags: opt/airflow/dags
COPY . /opt/airflow/

# Capture the build-args set from the respective build.sh script
ARG APP_ENV_BUILD_ARG
ARG GCP_CREDENTIALS_BUILD_ARG

# Asign args to env. In prod environment, gcp credentials won't exist, defaults to ''
ENV APP_ENV=${APP_ENV_BUILD_ARG}
ENV GOOGLE_APPLICATION_CREDENTIALS=${GCP_CREDENTIALS_BUILD_ARG}
ENV FLASK_APP="api_library_package/api_library/flask_app.py"
ENV DBT_PROFILES_DIR="dbt/profiles.yml"
ENV DBT_PROJECT_DIR="dbt/dbt_pipeline/dbt_project.yml"

# For debugging: Print out the APP_ENV env specified during image build 
RUN echo "Building for environment: $APP_ENV"
RUN if [ "${APP_ENV}" = "dev" ]; then \
        echo "Using GOOGLE_APPLICATION_CREDENTIALS found at ${GOOGLE_APPLICATION_CREDENTIALS}" \
    else \
        echo "GOOGLE_APPLICATION_CREDENTIALS not found, will use GCP service account metadata from cloud run" \
    fi

# Install dependencies 
RUN pip install -r api_library_package/requirements.txt

# Expose the port that Flask will run on
EXPOSE 8080

# Set the command to occur at run time. Using 'CMD' here, so that it can be overridden during testing
CMD [ "flask", "run", "--host=0.0.0.0", "--port=8080" ]
