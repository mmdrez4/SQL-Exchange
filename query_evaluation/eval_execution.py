import json
import sqlite3
import time
from copy import deepcopy
from os import listdir, makedirs
from os.path import join, isdir, isfile, dirname, abspath
from tqdm import tqdm
from func_timeout import func_timeout, FunctionTimedOut



settings_path = 'evaluation_settings.json'
with open(settings_path, 'r') as f:
    SETTINGS = json.load(f)

EVAL_CONFIG = SETTINGS['evaluation']
DATASET_NAME = EVAL_CONFIG['dataset_name']
MODEL_NAME = EVAL_CONFIG['model_dir']

if EVAL_CONFIG["method"] == "zeroshot":
    EVAL_CONFIG['generated_queries_directory'] = EVAL_CONFIG['generated_queries_directory_zeroshot']
    EVAL_CONFIG['result_directory'] = EVAL_CONFIG['result_directory_zeroshot']
    EVAL_CONFIG['summary_directory'] = EVAL_CONFIG['summary_directory_zeroshot']

DATABASES_DIR = f"{EVAL_CONFIG['raw_datasets_directory']}/{DATASET_NAME}/dev_databases/"
print(f"Databases Directory: {DATABASES_DIR}")
INPUT_DIR = f"{EVAL_CONFIG['result_directory']}/{DATASET_NAME}/{MODEL_NAME}/"

OUTPUT_DIR = f"{EVAL_CONFIG['result_directory']}/{DATASET_NAME}/{MODEL_NAME}/"
SUMMARY_DIR = f"{EVAL_CONFIG['summary_directory']}/{DATASET_NAME}/{MODEL_NAME}/execution_summary"

DATA_CONFIG = SETTINGS['data']
TARGET_DATABASES = DATA_CONFIG['target_databases']
SOURCE_DATABASES = DATA_CONFIG['source_databases']


makedirs(OUTPUT_DIR, exist_ok=True)
makedirs(SUMMARY_DIR, exist_ok=True)

def safe_execute_sql(sql, db_path):
    conn = sqlite3.connect(db_path, timeout=180)
    cur = conn.cursor()
    cur.execute(sql)
    result = cur.fetchmany(51)
    conn.close()
    return result

def check_execution_validity_with_result(sql, db_path, db_id, query_file, timeout=30):
    try:
        result = func_timeout(timeout, safe_execute_sql, args=(sql, db_path))
        return ("Success", result)
    except FunctionTimedOut as e:
        return ("Timeout", f"Timeout in {db_id}/{query_file}: {e}")
    except Exception as e:
        return ("Error", f"Error in {db_id}/{query_file}: {e}")

def get_target_db_path(db_id):
    return join(DATABASES_DIR, f'{db_id}/{db_id}.sqlite')


def get_target_db(db_id):
    return sqlite3.connect(join(DATABASES_DIR, f'{db_id}/{db_id}.sqlite'), timeout=180)

def main():
    print('--- Query Evaluation ---')
    print(f"Dataset: {DATASET_NAME}")
    print(f"Model: {MODEL_NAME}")

    db_ids = [f for f in listdir(INPUT_DIR) if isdir(join(INPUT_DIR, f))]
    if len(TARGET_DATABASES) > 0:
        db_ids = [db_id for db_id in db_ids if db_id in TARGET_DATABASES]

    print(f"Databases: {(db_ids)}")
    db_ids = tqdm(db_ids, position=0, desc='Database', leave=False, ascii=' ▖▘▝▗▚▞█')
    print(f"Input Directory: {INPUT_DIR}")
    full_summary = {'success': 0, 'empty': 0, 'error': 0, 'null_query': 0, 'total': 0, 'success_run_rate': 0, 'success_result_rate': 0}

    for db_id in db_ids:

        print(f"Processing database: {db_id}")
        summary_0 = {'success': 0, 'empty': 0, 'error': 0, 'null_query': 0, 'total': 0, 'success_run_rate': 0, 'success_result_rate': 0}
        query_files = [f for f in listdir(join(INPUT_DIR, db_id)) if isfile(join(INPUT_DIR, db_id, f)) and not f.startswith('.')]
        query_files = tqdm(query_files, position=1, desc='Query File', leave=False, ascii=' ▖▘▝▗▚▞█')
        summary_1_full = []

        for query_file in query_files:

            if len(SOURCE_DATABASES) > 0 and query_file not in SOURCE_DATABASES:
                continue

            tqdm.write(f"Processing query file: {query_file}")
            data = []
            summary_1 = {'source_db_id': query_file[len('response_'):-len('.json')], 'success': 0, 'empty': 0, 'error': 0, 'null_query': 0, 'total': 0, 'success_run_rate': 0, 'success_result_rate': 0}
            with open(join(INPUT_DIR, db_id, query_file), 'r') as f:
                queries = json.load(f)
                tqdm.write(f"Loaded {len(queries)} queries from {query_file}")
                for query in tqdm(queries, position=2, desc='Query', leave=False, ascii=' ▖▘▝▗▚▞█'):
                    start = time.time()
                    source_db_id = query['source_db_id']
                    current_data = deepcopy(query)
                    sql = query.get('target_query')

                    if sql:
                        try:
                            # conn = get_target_db(db_id)
                            db_path = get_target_db_path(db_id)
                            status, result = check_execution_validity_with_result(sql, db_path, db_id, query_file, timeout=30)
                        except Exception as e:
                            print(isdir(DATABASES_DIR))
                            print(isdir(join(DATABASES_DIR, f'{db_id}/{db_id}.sqlite')))
                            tqdm.write(f"Error connecting to database {db_id}: {e}")
                            exit(1)
                        try:

                            if status == "Timeout":
                                current_data['execution_result'] = result
                                current_data['result_status'] = 'Error (Timeout)'
                                summary_1['error'] += 1
                            elif status == "Error":
                                current_data['execution_result'] = result
                                current_data['result_status'] = 'Error'
                                summary_1['error'] += 1
                            else:
                                row_count = len(result)
                                current_data['execution_result'] = result[:50] if row_count > 50 else result
                                if row_count == 0:
                                    current_data['result_status'] = 'Empty'
                                    summary_1['empty'] += 1
                                elif row_count > 50:
                                    current_data['result_status'] = 'Success (more than 50 rows)'
                                    summary_1['success'] += 1
                                else:
                                    current_data['result_status'] = 'Success'
                                    summary_1['success'] += 1

                        except Exception as e:
                            current_data['execution_result'] = f"Error in {db_id}/{query_file}: {e}"
                            current_data['result_status'] = 'Error'
                            summary_1['error'] += 1
                    else:
                        current_data['execution_result'] = 'Null_Query'
                        current_data['result_status'] = 'Null_Query'
                        summary_1['null_query'] += 1

                    summary_1['total'] += 1
                    data.append(current_data)

            summary_1['success_result_rate'] = summary_1['success'] / summary_1['total'] if summary_1['total'] > 0 else 0
            summary_1['success_run_rate'] = (summary_1['success'] + summary_1['empty']) / summary_1['total'] if summary_1['total'] > 0 else 0
            summary_1_full.append(summary_1)

            makedirs(join(OUTPUT_DIR, db_id), exist_ok=True)
            with open(join(OUTPUT_DIR, db_id, query_file), 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)

            summary_0['success'] += summary_1['success']
            summary_0['empty'] += summary_1['empty']
            summary_0['error'] += summary_1['error']
            summary_0['null_query'] += summary_1['null_query']
            summary_0['total'] += summary_1['total']

        summary_0['success_result_rate'] = summary_0['success'] / summary_0['total'] if summary_0['total'] > 0 else 0
        summary_0['success_run_rate'] = (summary_0['success'] + summary_0['empty']) / summary_0['total'] if summary_0['total'] > 0 else 0

        with open(join(SUMMARY_DIR, f'{db_id}.json'), 'w', encoding='utf-8') as f:
            json.dump(summary_1_full, f, indent=4)

        makedirs(join(SUMMARY_DIR, 'summary'), exist_ok=True)
        with open(join(SUMMARY_DIR, 'summary', f'{db_id}.json'), 'w', encoding='utf-8') as f:
            json.dump(summary_0, f, indent=4)

        full_summary['success'] += summary_0['success']
        full_summary['empty'] += summary_0['empty']
        full_summary['error'] += summary_0['error']
        full_summary['null_query'] += summary_0['null_query']
        full_summary['total'] += summary_0['total']

    full_summary['success_result_rate'] = full_summary['success'] / full_summary['total'] if full_summary['total'] > 0 else 0
    full_summary['success_run_rate'] = (full_summary['success'] + full_summary['empty']) / full_summary['total'] if full_summary['total'] > 0 else 0

    makedirs(join(SUMMARY_DIR, 'full_summary'), exist_ok=True)
    with open(join(SUMMARY_DIR, 'full_summary', 'full_summary.json'), 'w', encoding='utf-8') as f:
        json.dump(full_summary, f, indent=4)

if __name__ == '__main__':
    main()
