import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta,datetime
from api_library import fetch_responses, parse_responses, bq_laod, fetch_sm_poor_fair, add_tags # add in sentiment_analysis function


# Define the function to run dbt --> this is a work around for running bash cmds in airflow locally on Windows (not in docker)
def run_dbt():
    try:
        # Run the dbt command using subprocess
        result = subprocess.run(
            ["dbt", "run"],  # Command to run dbt
            check=True,       # Raise an error if the command fails
            stdout=subprocess.PIPE,  # Capture standard output
            stderr=subprocess.PIPE   # Capture standard error
        )
        print(result.stdout.decode())  # Print the standard output from dbt run
        print(result.stderr.decode())  # Print any error message
    except subprocess.CalledProcessError as e:
        print(f"Error running dbt: {e}")
        print(e.stderr.decode())  # Print the error message

default_args= {
    'owner': 'erik_dohner',
    'start_date': datetime(2025, 2, 1),
    'retries': 1, 
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='pipeline_run',
    default_args=default_args,
    description='Fetch survey data from SurveyMoneky API and load to BigQuery',
    schedule_interval='@daily'
) as load_dag:

    
    fetch_responses_task = PythonOperator(
        task_id='fetch_responses', 
        python_callable=fetch_responses
    )
    
    parse_responses_task = PythonOperator(
        task_id='parse_responses',
        python_callable=parse_responses
    )
    
    bq_load_task = PythonOperator(
        task_id='bq_load',
        python_callable=bq_laod
    )
    
    # dbt_run_task = BashOperator(
    #     task_id='dbt_run',       
    #     bash_command='cmd.exe /c "dbt run"'
    # )
    
    dbt_run_task = PythonOperator(
        task_id='dbt_run', 
        python_callable=run_dbt
    )
    
    fetch_sm_poor_fair_task = PythonOperator(
        task_id='fetch_sm_poor_fair',
        python_callable=fetch_sm_poor_fair
    )
    
    add_tags_task = PythonOperator(
        task_id='add_tags',
        python_callable=add_tags,
        op_kwargs={
            'tag': 6636, # specific tag_id for the desired case: 'sm_poor_fair'
            'blob_name': 'sm_poor_fair_emails'
        }
    )
    
    # sentiment_analysis_task = PythonOperator(
    #     task_id='sa',
    #     python_callable=sentiment_analysis
    # )
    

    # remove_blobs_from_gcp_storage





fetch_responses_task >> parse_responses_task >> bq_load_task >> dbt_run_task >> fetch_sm_poor_fair_task >> add_tags_task