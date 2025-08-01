import json
import re
import os
from os import listdir, makedirs
from os.path import isfile, isdir, join, dirname, abspath
from copy import deepcopy
from tqdm import tqdm
from time import time
import google.generativeai as genai

from time import sleep

from utils import fix_comma

settings_path = 'evaluation_settings.json'
with open(settings_path, 'r') as f:
    SETTINGS = json.load(f)

EVAL_CONFIG = SETTINGS['evaluation']
MODEL_CONFIG = SETTINGS['model']

DATABASE_NAME = EVAL_CONFIG['dataset_name']
MODEL_NAME = EVAL_CONFIG['model_dir']

INPUT_DIR = f"{EVAL_CONFIG['result_directory']}/{DATABASE_NAME}/{MODEL_NAME}/"
SCHEMA_FILE = join('data', f'{DATABASE_NAME}/schemas.json')
PROMPT_DIR = join(EVAL_CONFIG["prompt_directory"], '')
PROMPT_FILE = join(PROMPT_DIR, EVAL_CONFIG['prompt_file'])
EXAMPLES_FILE = join(PROMPT_DIR, EVAL_CONFIG['examples_file'])

RESULT_DIR = f"{EVAL_CONFIG['result_directory']}/{DATABASE_NAME}/{MODEL_NAME}/"
SUMMARY_DIR = f"{EVAL_CONFIG['summary_directory']}/{DATABASE_NAME}/{MODEL_NAME}/semantic_summary"
LLM_DIR = EVAL_CONFIG["llm_response_directory"]

DATA_CONFIG = SETTINGS['data']
TARGET_DATABASES = DATA_CONFIG['target_databases']
SOURCE_DATABASES = DATA_CONFIG['source_databases']

MAX_TRY = EVAL_CONFIG['max_retry_per_prompt']

makedirs(RESULT_DIR, exist_ok=True)
makedirs(SUMMARY_DIR, exist_ok=True)


GOOGLE_API_KEY=os.getenv('GOOGLE_API_KEY')

genai.configure(api_key=GOOGLE_API_KEY)


def initialize_model(model_name: str, model_version: str, system_instruction: str = None):
    if 'gemini' not in model_name:
        raise ValueError(f"Only 'gemini' model is supported now. Got: {model_name}")
    return initialize_gemini_model(model_version, system_instruction)


def initialize_gemini_model(version: str, system_instruction: str = None):
    generation_config = {
        "temperature": MODEL_CONFIG['temperature'],
        "top_p": MODEL_CONFIG['top_p'],
        "top_k": MODEL_CONFIG['google']['top_k']
    }
    return genai.GenerativeModel(
        version,
        generation_config=generation_config,
        system_instruction=system_instruction if system_instruction else None
    )


def get_response_gemini(use_system_instruction, prompt_parts: list[str], model: genai.GenerativeModel, model_type: str) -> dict:
    begin = time()
    try:
        if model_type in ['gemini-1.0-pro', 'gemini-pro']:
            response = model.generate_content(''.join(prompt_parts), stream=(model_type == 'gemini-1.0-pro'))
            if hasattr(response, 'resolve'):
                response.resolve()
            text = response.text
        else:
            prompt = prompt_parts[1] if use_system_instruction else ''.join(prompt_parts)
            response = model.generate_content(prompt)
            text = response.text
        reason = str(response.candidates[0].finish_reason).upper()
        if reason in ["4", "RECITATION"]:
            return {"response": "RECITATION", "prompt_token_count": None, "response_token_count": None, "time_taken": None}
        if reason in ["2", "MAX_TOKENS"]:
            return {"response": "MAX_TOKEN", "prompt_token_count": None, "response_token_count": None, "time_taken": None}
        return {
            "response": text,
            "prompt_token_count": model.count_tokens(''.join(prompt_parts)).total_tokens,
            "response_token_count": model.count_tokens(text).total_tokens,
            "time_taken": time() - begin
        }
    except Exception as e:
        tqdm.write(f"Gemini error: {e}")
        exit(1)
        return {"response": "RECITATION", "prompt_token_count": None, "response_token_count": None, "time_taken": None}


def get_examples(file_path) -> str:
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read()


def main():
    db_ids = [f for f in listdir(INPUT_DIR) if isdir(join(INPUT_DIR, f))]
    if len(TARGET_DATABASES) > 0:
        db_ids = [db_id for db_id in db_ids if db_id in TARGET_DATABASES]

    full_summary = {'meaningful_nl_question': 0, 'correct_sql_mapping': 0, 'total': 0}
    not_generated_results = []

    base_prompt = open(PROMPT_FILE, 'r', encoding='utf-8').read()
    base_prompt += get_examples(EXAMPLES_FILE)

    model_name = MODEL_CONFIG['model_name']
    model_version = MODEL_CONFIG['model_version']
    use_system_instruction = MODEL_CONFIG.get('use_system_instruction', True)
    model = initialize_model(model_name, model_version, base_prompt)

    with open(SCHEMA_FILE, 'r') as f:
        schemas = json.load(f)

    for db_id in tqdm(db_ids, desc="Database", ascii=' ▖▘▝▗▚▞█'):

        schema = schemas.get(db_id)
        summary_db = {'meaningful_nl_question': 0, 'correct_sql_mapping': 0, 'total': 0}

        tqdm.write(f"Processing: {db_id}")

        query_files = [f for f in listdir(join(INPUT_DIR, db_id)) if isfile(join(INPUT_DIR, db_id, f)) and f.endswith('.json') and not f.startswith('.') and not f.endswith('llm.json')]
        for query_file in query_files:
            source_db_id = query_file[len('response_'):-len('.json')]
            if len(SOURCE_DATABASES) > 0 and source_db_id not in SOURCE_DATABASES:
                continue

            query_path = join(INPUT_DIR, db_id, query_file)
            if not isfile(query_path):
                continue

            try:
                with open(query_path, 'r') as f:
                    queries = json.load(f)
            except:
                print(f"Invalid JSON format in {query_path}")
                continue

            current_questions = [
                {"nl_question": q["target_question"], "sql_query": q["target_query"]}
                for q in queries if "target_question" in q and "target_query" in q
            ]

            prompt_parts = [base_prompt,
                                "\n\n## Rate the following questions and sql:\n\n"
                                + "# Source schema\n"
                                + f"\"{schema}\"\n\n\n"
                                + "# Input pairs\n"
                                + json.dumps(current_questions, indent=4)
                                + f"\n\n# Output:\n\n"
                                ]
            
            last_error = None
            response = None
            error_count = 0
            while error_count <= MAX_TRY:
                try:
                    match model_name:
                        case 'gemini':
                            result = get_response_gemini(use_system_instruction, prompt_parts, model, model_version)
                        case _: raise ValueError(f"Unsupported model: {model_name}")

                    response = result["response"]
                    if response not in ["RECITATION", "MAX_TOKEN"]:
                        response_json = response[response.find("["):response.rfind("]") + 1]
                        response_data = json.loads(response_json)
                        if len(response_data) == len(queries):
                            break
                        error_count += 1
                except Exception as e:
                    if "Expecting ',' delimiter" in str(e):
                        comma_fixed, response_fixed = fix_comma(response, join(INPUT_DIR, db_id))
                        if comma_fixed:
                            response_data = json.loads(response_fixed)
                            break
                        error_count += 1
                    elif "Invalid \\escape" in str(e):
                        tqdm.write(f"Invalid escape sequence in response: {e}")
                        error_count += 1
                    else:
                        print(f"Unhandled error: {e}")
                        error_count += 1
                    last_error = e
                        
            if error_count > MAX_TRY:
                tqdm.write(f"Max retries exceeded for {query_file}. Skipping...")
                not_generated_results.append({
                    "db_id": db_id,
                    "source_db_id": source_db_id,
                    "query_file": query_file,
                    "error": str(last_error)
                })
                continue

            sleep(EVAL_CONFIG['sleep_time'])

            output_data = []
            for query, rating in zip(queries, response_data):
                if query.get('is_generated') != "True":
                    continue
                record = deepcopy(query)
                nl_rating = rating.get('clarity_and_alignment_of_NL') or next((v for k, v in rating.items() if k.startswith('clarity')), {})
                sql_rating = rating.get('correctness_of_query') or next((v for k, v in rating.items() if k.startswith('correctness')), {})

                record['meaningful_nl_question'] = nl_rating.get('is_clear_and_meaningful', 'N/A')
                record['correct_target_nl_sql_mapping'] = sql_rating.get('is_correct_mapping', 'N/A')
                record['nl_thought'] = nl_rating.get('thought_process', 'N/A')
                record['sql_thought'] = sql_rating.get('thought_process', 'N/A')

                if record['meaningful_nl_question'].lower() == 'yes':
                    summary_db['meaningful_nl_question'] += 1
                if record['correct_target_nl_sql_mapping'].lower() == 'yes':
                    summary_db['correct_sql_mapping'] += 1

                summary_db['total'] += 1
                output_data.append(record)

            result_db_dir = join(RESULT_DIR, db_id)
            makedirs(result_db_dir, exist_ok=True)
            with open(join(result_db_dir, query_file), 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=4)

            makedirs(join(result_db_dir, LLM_DIR), exist_ok=True)
            with open(join(result_db_dir, LLM_DIR, f'response_{source_db_id}_llm.json'), 'w', encoding='utf-8') as f:
                json.dump(response_data, f, indent=4)

            tqdm.write(f"Processed {query_file}")


        summary_path = join(SUMMARY_DIR, 'summary', f'{db_id}.json')
        makedirs(dirname(summary_path), exist_ok=True)
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary_db, f, indent=4)

        for k in ['meaningful_nl_question', 'correct_sql_mapping', 'total']:
            full_summary[k] += summary_db[k]

    full_summary['meaningfulness_rate'] = full_summary['meaningful_nl_question'] / full_summary['total'] if full_summary['total'] else 0
    full_summary['correct_sql_mapping_rate'] = full_summary['correct_sql_mapping'] / full_summary['total'] if full_summary['total'] else 0


    full_summary_dir = join(SUMMARY_DIR, 'full_summary')
    makedirs(full_summary_dir, exist_ok=True)
    with open(join(full_summary_dir, 'full_summary.json'), 'w', encoding='utf-8') as f:
        json.dump(full_summary, f, indent=4)

    with open(join(full_summary_dir, 'not_generated_results.json'), 'w', encoding='utf-8') as f:
        json.dump(not_generated_results, f, indent=4)


if __name__ == '__main__':
    main()
