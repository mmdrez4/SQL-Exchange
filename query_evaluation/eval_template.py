import re
import json
from copy import deepcopy
from os import listdir, makedirs
from os.path import join, isfile, isdir, dirname, abspath
from tqdm import tqdm


settings_path = 'evaluation_settings.json'
with open(settings_path, 'r') as f:
    SETTINGS = json.load(f)

EVAL_CONFIG = SETTINGS['evaluation']
DATASET_NAME = EVAL_CONFIG['dataset_name']
MODEL_NAME = EVAL_CONFIG['model_dir']

BASE_DIR = dirname(abspath(__file__)) + '/'
GENERATED_QUERIES_DIRECTORY = f"{EVAL_CONFIG['generated_queries_directory']}/{DATASET_NAME}/{MODEL_NAME}/"
RESULT_DIRECTORY = f"{EVAL_CONFIG['result_directory']}/{DATASET_NAME}/{MODEL_NAME}/"
SUMMARY_DIRECTORY = f"{EVAL_CONFIG['summary_directory']}/{DATASET_NAME}/{MODEL_NAME}/template_summary"

DATA_CONFIG = SETTINGS['data']
TARGET_DATABASES = DATA_CONFIG['target_databases']
SOURCE_DATABASES = DATA_CONFIG['source_databases']

for d in [RESULT_DIRECTORY, SUMMARY_DIRECTORY]:
    makedirs(d, exist_ok=True)

def retain_structure(query, sqlite_keywords, sqlite_functions):
    query = re.sub(r"'[^']*'", '', query)
    query = re.sub(r'"[^"]*"', '', query)
    query = re.sub(r'`[^`]*`', '', query)
    query = re.sub(r"\[[^\]]*\]", '', query)
    query = query.upper()
    query = re.sub(r'>=|<=|:|/|\+|-|<|>', '=', query)

    table_keywords = r'(FROM|JOIN|INTO|UPDATE|TABLE)'
    reserved = r'(' + '|'.join(sqlite_keywords) + r')'
    table_pattern = rf'\b{table_keywords}\s+{reserved}\b(?!\s*\()'
    query = re.sub(table_pattern, r'\1 ', query)

    pattern = r'(?<!\.)\b(?:' + '|'.join(sqlite_keywords) + r')\b|\b(?:' + '|'.join(sqlite_functions) + r')(?=\s*\()|[(),*/+=\-<>]'
    return ' '.join(re.findall(pattern, query))

def check_query_templates(data, sqlite_keywords, sqlite_functions):
    return {
        'source_template': retain_structure(data["source_query"], sqlite_keywords, sqlite_functions),
        'target_template': retain_structure(data["target_query"], sqlite_keywords, sqlite_functions)
    }

def check_if_query_is_generated(data):
    return all(data.get(field) for field in ["target_query", "target_question"])

def main():
    sqlite_keywords = [
        "ABORT", "ACTION", "ADD", "AFTER", "ALL", "ALTER", "ALWAYS", "ANALYZE", "AND", "ASC", "ATTACH", "AUTOINCREMENT",
        "BEFORE", "BEGIN", "BETWEEN", "BY", "CASCADE", "CASE", "CAST", "CHECK", "COLLATE", "COLUMN", "COMMIT", "CONFLICT",
        "CONSTRAINT", "CREATE", "CROSS", "CURRENT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "DATABASE",
        "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE", "DESC", "DETACH", "DISTINCT", "DO", "DROP", "EACH", "ELSE", "END",
        "ESCAPE", "EXCEPT", "EXCLUDE", "EXCLUSIVE", "EXISTS", "EXPLAIN", "FAIL", "FILTER", "FIRST", "FOLLOWING", "FOR",
        "FOREIGN", "FROM", "FULL", "GENERATED", "GLOB", "GROUP", "GROUPS", "HAVING", "IF", "IGNORE", "IMMEDIATE", "IN",
        "INDEX", "INDEXED", "INITIALLY", "INNER", "INSERT", "INSTEAD", "INTERSECT", "INTO", "IS", "ISNULL", "JOIN", "KEY",
        "LAST", "LEFT", "LIKE", "LIMIT", "MATCH", "MATERIALIZED", "NATURAL", "NO", "NOT", "NOTHING", "NOTNULL", "NULL",
        "NULLS", "OF", "OFFSET", "ON", "OR", "ORDER", "OTHERS", "OUTER", "OVER", "PARTITION", "PLAN", "PRAGMA", "PRECEDING",
        "PRIMARY", "QUERY", "RAISE", "RANGE", "RECURSIVE", "REFERENCES", "REGEXP", "REINDEX", "RELEASE", "RENAME",
        "REPLACE", "RESTRICT", "RETURNING", "RIGHT", "ROLLBACK", "ROW", "ROWS", "SAVEPOINT", "SELECT", "SET", "TABLE",
        "TEMP", "TEMPORARY", "THEN", "TIES", "TO", "TRANSACTION", "TRIGGER", "UNBOUNDED", "UNION", "UNIQUE", "UPDATE",
        "USING", "VACUUM", "VALUES", "VIEW", "VIRTUAL", "WHEN", "WHERE", "WINDOW", "WITH", "WITHOUT"
    ]
    sqlite_functions = [
        "AVG", "COUNT", "MAX", "MIN", "SUM", "GROUP_CONCAT", "SUBSTR", "TRIM", "LTRIM", "RTRIM", "LENGTH", "REPLACE",
        "UPPER", "LOWER", "INSTR", "COALESCE", "IFNULL", "IIF", "NULLIF", "DATE", "TIME", "DATETIME", "JULIANDAY",
        "STRFTIME", "ABS", "RANDOM", "ROUND"
    ]

    db_ids = [f for f in listdir(GENERATED_QUERIES_DIRECTORY) if isdir(join(GENERATED_QUERIES_DIRECTORY, f))]
    db_ids = tqdm(db_ids, position=0, desc='Database', leave=False, ascii=' ▖▘▝▗▚▞█')
    full_summary = {'not_generated_query': 0, 'success': 0, 'error': 0, 'total': 0, 'success_rate': 0}
    not_generated_results = []

    for db_id in db_ids:

        if len(TARGET_DATABASES) > 0 and db_id not in TARGET_DATABASES:
            continue

        summary_0 = {'not_generated_query': 0, 'success': 0, 'error': 0, 'total': 0, 'success_rate': 0}
        query_files = [f for f in listdir(join(GENERATED_QUERIES_DIRECTORY, db_id)) if isfile(join(GENERATED_QUERIES_DIRECTORY, db_id, f)) and f.endswith('.json')]
        summary_1_full = []

        for query_file in query_files:

            if len(SOURCE_DATABASES) > 0 and query_file not in SOURCE_DATABASES:
                continue


            data = []
            summary_1 = {'source_db_id': query_file[len('response_'):-len('.json')], 'not_generated_query': 0, 'success': 0, 'error': 0, 'total': 0, 'success_rate': 0}

            with open(join(GENERATED_QUERIES_DIRECTORY, db_id, query_file), 'r') as f:
                queries = json.load(f)
                for query in queries:
                    current_data = deepcopy(query)
                    if not check_if_query_is_generated(query):
                        current_data.update({
                            'is_generated': 'False',
                            'same_template': 'False',
                            'source_query_template': 'null',
                            'target_query_template': 'null'
                        })
                        summary_1['error'] += 1
                        summary_1['not_generated_query'] += 1
                        not_generated_results.append(current_data)
                    else:
                        current_data['is_generated'] = 'True'
                        res = check_query_templates(query, sqlite_keywords, sqlite_functions)
                        current_data.update({
                            'source_query_template': res['source_template'],
                            'target_query_template': res['target_template'],
                            'same_template': str(res['source_template'] == res['target_template']),
                        })
                        if res['source_template'] == res['target_template']:
                            summary_1['success'] += 1
                        else:
                            summary_1['error'] += 1

                    summary_1['total'] += 1
                    data.append(current_data)

            summary_1['success_rate'] = summary_1['success'] / summary_1['total'] if summary_1['total'] > 0 else 0
            summary_1_full.append(summary_1)
            summary_0['total'] += summary_1['total']
            summary_0['success'] += summary_1['success']
            summary_0['error'] += summary_1['error']
            summary_0['not_generated_query'] += summary_1['not_generated_query']

            makedirs(join(RESULT_DIRECTORY, db_id), exist_ok=True)
            with open(join(RESULT_DIRECTORY, db_id, query_file), 'w') as f:
                json.dump(data, f, indent=4)

        summary_0['success_rate'] = summary_0['success'] / summary_0['total'] if summary_0['total'] > 0 else 0
        with open(join(SUMMARY_DIRECTORY, f'{db_id}.json'), 'w') as f:
            json.dump(summary_1_full, f, indent=4)
        makedirs(join(SUMMARY_DIRECTORY, 'summary'), exist_ok=True)
        with open(join(SUMMARY_DIRECTORY, 'summary', f'{db_id}.json'), 'w') as f:
            json.dump(summary_0, f, indent=4)

        full_summary['total'] += summary_0['total']
        full_summary['success'] += summary_0['success']
        full_summary['error'] += summary_0['error']
        full_summary['not_generated_query'] += summary_0['not_generated_query']

    full_summary['success_rate'] = full_summary['success'] / full_summary['total'] if full_summary['total'] > 0 else 0
    makedirs(join(SUMMARY_DIRECTORY, 'full_summary'), exist_ok=True)
    with open(join(SUMMARY_DIRECTORY, 'full_summary', 'full_summary.json'), 'w') as f:
        json.dump(full_summary, f, indent=4)
    with open(join(SUMMARY_DIRECTORY, 'full_summary', 'not_generated_results.json'), 'w') as f:
        json.dump(not_generated_results, f, indent=4)

if __name__ == '__main__':
    main()
