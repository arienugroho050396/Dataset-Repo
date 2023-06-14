from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

main_dag_id = 'log_cleansing'

args = {
'owner': 'AirflowMonitoring',
'start_date': datetime(2023,3,1),
'root' : '/opt/airflow/logs/dag_id=DEV_LPDWH_Payment_Transaction_Nett_2/*'
}

with DAG(
    main_dag_id,
    schedule_interval='@once',
    default_args=args
) as dag:
    log_cleansing = BashOperator(
        task_id='cleansing',
        bash_command="find {} -type f -mtime +1 -delete".format(dag.default_args['root'])
    )

    log_cleansing