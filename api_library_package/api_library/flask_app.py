import subprocess
from flask import Flask

app = Flask(__name__)

@app.route('/trigger', methods=['POST'])
def trigger():
    # Bash command to trigger the Airflow DAG
    dag_trigger_command = "airflow tasks test pipeline_run fetch_responses 2025-02-19"
    
    # for production, we will use this command 
    # "airflow dags trigger load"
    
    # Run the command using subprocess
    try:
        subprocess.run(dag_trigger_command,
                       shell=True, # enables commands to be written as bash scripts 
                       check=True # enables errors to be raised, will be caught in the except block
                       ) 
        return "DAG triggered successfully!", 200
    except subprocess.CalledProcessError as e:
        return f"Failed to trigger DAG: {e}", 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080)
