## Version 0.2
## Requires config.json and funs.py

import datetime, json, os
from itertools import groupby

from airflow import DAG
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

from funs import get_load_id, generate, deliver, backlog_check

DEFAULT_ARGS = {
    "owner": "akash",
    "depends_on_past": False,
    "email": ["akash.jena"],
    "email_on_failure": True,
    "email_on_retry": False,
}


with DAG(
    dag_id='dynamic_tasks',
    schedule_interval='00 01 * * *',
    start_date=datetime.datetime.now(),
    catchup=False,
    tags=[],
    params={"project": "dynamic-delete"},
) as dag:

    curr_dir = os.path.dirname(os.path.abspath(__file__))
    master_config = json.load(open(f'{curr_dir}/config.json', 'r'))

    entities = [list(value) for key, value in groupby(sorted([entity for entity in master_config['entities'] if entity['active']], key= lambda entity: entity['entity_order']), key= lambda entity: entity['entity_order'])]

    Begin = PythonOperator(task_id='Begin', python_callable=get_load_id, dag=dag)
    Backlog = PythonOperator(task_id='Backlog', python_callable=backlog_check, dag=dag)
    End = DummyOperator(task_id='End')

    # wait_time = 2000
    # overall_time = 2100
    # num_cores = 2
    # no_of_threads = num_cores * (1 + wait_time/(overall_time-wait_time))

    MAX_THREAD = int(Variable.get(key='MAX_THREAD'))
    FOLDER_YEAR = Variable.get(key='FOLDER_YEAR')
    API_YEAR = Variable.get(key='API_YEAR')
    USER = Variable.get(key='USER')
    PASSWORD = Variable.get(key='PASSWORD')
    EXECUTION_DATE = Variable.get(key='EXECUTION_DATE', default_var='{{ ds_nodash }}')
    PROCESSING_DATE = EXECUTION_DATE if EXECUTION_DATE != '' else '{{ ds_nodash }}'

    error_pct = master_config['error_threshold_pct']
    error_no = master_config['error_threshold_count']
    error_value = 5
    error_pct_bool = True
    if error_pct != 0: error_value = error_pct
    elif error_no != 0:
        error_value = error_no
        error_pct_bool = False

    level_count = 1
    stage_count = 1
    pre_stage_complete_task = None
    for level in entities:
        with TaskGroup('Level' + str(level_count)) as taskgroup:
            current_level_delivery_tasks = []
            stage_complete_task = DummyOperator(task_id ='Stage2-Level' + str(level_count) + '-Complete')
            # entity_count = len(level)
            # no_of_threads = MAX_THREAD/entity_count
            for level_entity in level:
                generation_task = PythonOperator(task_id=level_entity['entity_name'] + '_Generation', python_callable=generate,op_kwargs={'entity': level_entity, 'loadId': 1, 's3_folder_year': FOLDER_YEAR, 'error_value': error_value, 'error_pct_bool': error_pct_bool, 'loadDate': PROCESSING_DATE}, dag=dag)
                # Variable.set(level_entity, "1" ) 
                # if Variable.get(level_entity) == "1":
                delivery_task = PythonOperator(task_id=level_entity['entity_name'] + '_Delivery', python_callable=deliver,op_kwargs={'entity': level_entity, 'loadId': 1, 'no_of_threads' : MAX_THREAD, 'year': API_YEAR, 'user': USER, 'password': PASSWORD, 'error_value': error_value, 'error_pct_bool': error_pct_bool}, dag=dag)
                    # Variable.set(level_entity, "0")
                generation_task >> delivery_task
                delivery_task >> stage_complete_task
                current_level_delivery_tasks.append(delivery_task)
            if pre_stage_complete_task:
                for current_level_delivery_task in current_level_delivery_tasks:
                    pre_stage_complete_task >> current_level_delivery_task
            pre_stage_complete_task = stage_complete_task
        
        Begin >> Backlog >> taskgroup
        taskgroup >> End
        level_count = level_count + 1
