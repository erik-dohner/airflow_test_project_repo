# select the base image for airflow
FROM python:3.9-slim

COPY . /opt/airflow/

RUN pip install /opt/airflow/api_library_package/.

WORKDIR /opt/airflow


# Capture the build-args set from the respective build.sh script
ARG APP_ENV_BUILD_ARG

# Asign args to env. In prod environment, gcp credentials won't exist, defaults to ''
ENV APP_ENV=${APP_ENV_BUILD_ARG}
ENV FLASK_APP="api_library_package/api_library/flask_app.py"
ENV DBT_PROFILES_DIR="/opt/airflow/dbt"
ENV DBT_PROJECT_DIR="/opt/airflow/dbt/dbt_pipeline"
ENV AIRFLOW_HOME=/opt/airflow

# For debugging: Print out the APP_ENV env specified during image build 
RUN echo "Building for environment: $APP_ENV"

# Initialize the database only if needed (e.g., for first-time setup)
RUN airflow db init || true  

# Set the default command to open a bash shell
CMD ["bash"]

# # Expose the port that Flask will run on
# EXPOSE 8080

# # Set the command to occur at run time. Using 'CMD' here, so that it can be overridden during testing
# CMD [ "flask", "run", "--host=0.0.0.0", "--port=8080" ]
