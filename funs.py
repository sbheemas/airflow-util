import time, os

def get_load_id():
    return 1

def generate(entity, loadId, s3_folder_year, error_value, error_pct_bool, loadDate):
    print(loadId)
    time.sleep(5)
    # if entity['entity_name'] == 'Student': raise
    return

def deliver(entity, loadId, no_of_threads, year, user, password, error_value, error_pct_bool):
    print(loadId)
    time.sleep(5)
    return

def backlog_check():
    total_list = []
    dag_id = 'dynamic_tasks'
    start_date = '2022-08-11 11:03:36'
    end_date = '2022-08-11 11:08:36'
    clear_command = f"airflow tasks clear -f -s '{start_date}' -e '{end_date}' {dag_id} -y"
    output = os.system(clear_command)
    print(output)
    # raise
    return total_list