import json
from os import listdir, makedirs
from os.path import isfile, join, abspath, dirname, exists
from time import sleep
from tqdm import trange, tqdm
from datetime import datetime
from time import time
import random

from .model import CustomModel
from .stats import Stats
from .utils import *

# Constants settings
BASE_DIRECTORY = dirname(abspath(__file__)) + '/'   # Path to the current file
DEFAULT_ERRORS = {
    "empty_output": False,
    "fields_mismatch": False,
    "db_id_matching": False,
    "recitation": False,
    "max_token": False,
    "invalid_escape": False,
    "json_decode": False,
    "unexpected": False,
}
RETRY_TIMER = 1
MAX_TOKENS_FAIL_REMINDER = 9

def load_settings(settings_dir: str = f'{BASE_DIRECTORY}../mapping_settings.json') -> tuple:
    '''
    Load settings from the mapping_settings.json file.
    Args:
        settings_dir (str): Path to the mapping_settings.json file
    Returns:
        SETTINGS (json): JSON object containing the settings, None if not found or invalid
        error_message (str): Error message if any, None otherwise
    '''
    if not exists(settings_dir):
        return None, "mapping_settings.json file not found"
    
    with open(settings_dir, 'r', encoding='utf-8') as f:
        SETTINGS = json.load(f)
    
    if SETTINGS["model"]["model_name"] == "" or SETTINGS["model"]["model_origin"] == "":
        return None, "Model name or origin not set in mapping_settings.json"

    return SETTINGS, None

def load_prompts(generation_settings: json) -> tuple:
    '''
    Load prompts from the prompts.json file.
    Args:
        settings (json): JSON object containing the settings
    Returns:
        prompts (dict): Dictionary containing the prompts, None if not found or invalid.
        error_message (str): Error message if any, None otherwise
    '''
    prompts = {
        "base": "",
        "system": "",
    }
    prompts_dir = abspath(f'{BASE_DIRECTORY}../{generation_settings["prompt_directory"]}')
    base_prompt_dir = join(prompts_dir, generation_settings["base_prompt_file"])
    system_prompt_dir = join(prompts_dir, generation_settings["system_instruction_file"])

    # Validate if file exists
    if not exists(prompts_dir) or not exists(base_prompt_dir):
        return None, f"{prompts_dir} or {base_prompt_dir} not found"
    if generation_settings["system_instruction_file"] != "" and not exists(system_prompt_dir):
        return None, f"{system_prompt_dir} not found but 'system instruction file' is set in mapping_settings.json. Please set it to empty string if not used."
    
    # Read the base prompt and validate if it is empty
    with open(base_prompt_dir, 'r', encoding='utf-8') as f:
        prompts["base"] = f.read()
    if prompts["base"] == "":
        return None, "base prompt file is empty. Please add a space in the file if intended to be empty."
    
    # Read the system prompt and validate if it is empty
    if generation_settings["system_instruction_file"] != "":
        with open(system_prompt_dir, 'r', encoding='utf-8') as f:
            prompts["system"] = f.read()
        if prompts["system"] == "":
            return None, "system instruction file is empty. Please add a space in the file if intended to be empty."
    
    return prompts, None

def load_samples(target_dataset: str) -> dict | None:
    """
    Load all target samples

    Args:
        target_dataset (str): target_dataset inside settings
    
    Returns:
        all_samples (dict): samples of all dbs, dict keys are the db_ids
    """
    samples_folder = abspath(f'{BASE_DIRECTORY}../{target_dataset}/target_samples')
    # Check if the folder exists
    if not exists(samples_folder):
        return None
    
    sample_files = [f for f in listdir(samples_folder) if (isfile(join(samples_folder, f)) and f.endswith(".json"))]
    # Check if there are sample files
    if not len(sample_files):
        return None
    
    all_samples = dict()
    for sample_file in sample_files:
        file_path = join(samples_folder, sample_file)
        db_id = sample_file[len("sample_"):].split('.')[0]
        with open(file_path, 'r', encoding='utf-8') as f:
            all_samples[db_id] = json.load(f)

    return all_samples

def load_schemas(source_dataset: str, target_dataset: str, source_dbs: list, target_db: str) -> tuple:
    """
    Loads dataset schemas under pipelines
    
    Args:
        source_dataset (str): source dataset name from root dir
        target_dataset (str): target dataset name from root dir
    
    Returns:
        schemas (tuple): 
        - dictionary for both source and target schemas, None if not found, empty, or invalid
        - invalid reason
    """
    source_schemas_file = abspath(f'{BASE_DIRECTORY}../{source_dataset}/schemas.json')
    target_schemas_file = abspath(f'{BASE_DIRECTORY}../{target_dataset}/schemas.json')
    # Check if the folder exists
    if not exists(source_schemas_file) or not exists(target_schemas_file):
        return None, "schemas.json files not found"
    
    schemas = {"source": None, "target": None}
    
    try:
        with open(source_schemas_file, 'r', encoding="utf-8") as f:
            schemas['source'] = json.load(f)

        with open(target_schemas_file, 'r', encoding="utf-8") as f:
            schemas['target'] = json.load(f)

    except json.JSONDecodeError:
        return None, "schemas.json files are not valid JSON"
    
    # Check if the files are empty
    if schemas['source'] == "" or schemas['target'] == "":
        return None, "schemas.json files are empty"
    
    # Check if db_ids are in the schemas
    if (len(source_dbs) != 0 and not set(source_dbs) <= set(schemas['source'].keys())) or target_db not in schemas['target'].keys():
        return None, "one or more db_ids are not found in schemas.json files"
    
    return schemas, None

def prepare_prompt(prompts: dict, questions: list, target_samples: dict, schemas: dict, db_ids: dict, ) -> str:
    return "".join(
        [
            prompts['base'],
            "\n\n## Generate the query for the following query:\n\n",
            "# Source schema\n",
            "{\n",
            f"    \"db_id\": \"{db_ids["source"]}\",\n",
            f"    \"schema\": \"{schemas["source"][db_ids["source"]]}\"\n",
            "}\n\n",
            "# Target schema\n",
            "{\n",
            f"    \"db_id\": \"{db_ids["target"]}\",\n",
            f"    \"schema\": \"{schemas["target"][db_ids["target"]]}\"\n",
            "}\n\n",
            "# Target sample data\n",
            json.dumps(target_samples[db_ids["target"]], indent=4),
            "\n\n# Source query:\n",
            json.dumps(questions, indent=4),
            # f"\n\n#Reminder: Make sure the output is in valid JSON format with correct commas placed.\n\n",
            f"\n\n#Output:\n\n",
        ]
    )

def prepare_questions(directory: str, questions_settings: dict) -> list:
    """
    Prepare the questions for the model
    Args:
        directory (str): The directory to load the questions from
        questions_settings (dict): The settings for the questions
    Returns:
        questions (list): The list of questions
    """
    # Check if the directory exists
    if not exists(directory):
        return None
    
    question_data = None
    try:
        with open(directory, 'r', encoding='utf-8') as f:
            question_data = json.load(f)
    except json.JSONDecodeError:
        return None
    
    # Check if the file is empty
    if not isinstance(question_data, list) or len(question_data) == 0:
        return None

    match questions_settings["source_questions_shuffle_seed"]:
        case -1:
            pass
        case 0:
            random.seed(datetime.now().timestamp())
            random.shuffle(question_data)
        case _:
            random.seed(questions_settings["source_questions_shuffle_seed"])
            random.shuffle(question_data)

    match questions_settings["source_questions_limit"]:
        case -1:
            pass
        # If the limit is 0, return None
        case 0:
            return None
        case _:
            if len(question_data) > questions_settings["source_questions_limit"]:
                question_data = question_data[:questions_settings["source_questions_limit"]]

    return question_data

def write_response(system_prompt: str, prompt: str, response: dict, write_dir: str, current_db: str, json_only: bool = False) -> None:
    '''
    Writes the LLM response to files. Both json and full text prompt and response.
    The json file will be overwritten and the full text will be appended.

    Args:
        system_prompt (str): The system prompt given to model
        prompt (str): The prompt given to model
        response (dict): The response from model.generate
        write_dir (str): The directory to write to
        current_db (str): The id of the current source db for file name
    '''
    if json_only:
        if not exists(write_dir):
            makedirs(write_dir)

        # Write the json object
        with open(join(write_dir, f"response_{current_db}.json"), "w", encoding="utf-8") as f:
            json.dump(response['json'], f, indent=4)
        return

    # Check for directory
    if not exists(join(write_dir, "full/")):
        makedirs(join(write_dir, "full/"))

    # Write the json object
    with open(join(write_dir, f"{current_db}.json"), "w", encoding="utf-8") as f:
        json.dump(response['json'], f, indent=4)

    # Check if the response is empty
    if response['response'] == None or len(response['response']) == 0:
        return
    
    # Write the full chat prompt + response
    with open(join(write_dir, "full/", f"{current_db}.txt"), "a", encoding="utf-8") as f:
        f.write("\n\n")
        f.write(get_divider(f"{datetime.today().strftime('%Y-%m-%d %H:%M:%S')}"))
        if system_prompt != "":
            f.write(f'\n\n{get_divider("system", 50, "-")}\n\n')
            f.write(system_prompt)
        f.write(f'\n\n{get_divider("prompt", 50, "-")}\n\n')
        f.write(prompt)
        f.write(f'\n\n{get_divider("response", 50, "-")}\n\n')
        f.write(response['response'])

def write_stats(level: int, stats: Stats, write_dir: str, current_db: str) -> None:
    '''
    Write the stats to appropriate file according to the level.
    Args:
        level (int): The level of the stats
        stats (Stats): The stats object
        write_dir (str): The directory to write to
        current_db (str): The id of the current source db for file name
    '''
    match level:
        # Overall stats
        case 0:
            if not exists(write_dir):
                makedirs(write_dir)
            with open(join(write_dir, "stats.json"), "w", encoding="utf-8") as f:
                json.dump(stats.get_stats(), f, indent=4)

        # Pipeline stats
        case 1:
            report_dir = join(write_dir, "report/")
            if not exists(report_dir):
                makedirs(report_dir)
            with open(join(report_dir, "stats.json"), "w", encoding="utf-8") as f:
                json.dump(stats.get_stats(), f, indent=4)

            # Write errors if any for skipped prompts and dbs
            # If response is generated, they can be found in the 'full' folder
            if len(stats.skipped_db_prompts) != 0:
                with open(join(report_dir, "errors.json"), "w", encoding="utf-8") as f:
                    json.dump(stats.skipped_db_prompts, f, indent=4)

        # Source db stats
        case 2:
            report_dir = join(write_dir, "report/")
            if not exists(report_dir):
                makedirs(report_dir)
            with open(join(report_dir, "stats_per_db.json"), "a+", encoding="utf-8") as f:
                f.seek(0)
                try:
                    db_stats = json.load(f)
                except json.JSONDecodeError:
                    db_stats = {}
                if current_db not in db_stats.keys():
                    db_stats[current_db] = stats.get_stats()
                else:
                    db_stats[current_db].update(stats.get_stats())
                f.seek(0)
                f.truncate()
                json.dump(db_stats, f, indent=4)

def max_fail_exit(error: str) -> bool:
    '''
    Exit the program if the max fail limit is reached.
    '''
    # TODO: Deal with the error and save all stats
    print(current_time_text("Max fail limit reached", title="System", color="fail"))
    print(current_time_text(f"Error: {error}", title="System", color="fail"))
    print(current_time_text("Exiting...", title="System", color="fail"))
    exit(1)

def run():
    
    # Load settings
    SETTINGS, settings_error = load_settings()
    if settings_error:
        print(current_time_text(settings_error, title="System", color="fail"))
        exit(1)
    
    # Load model
    try:
        model = CustomModel(settings=SETTINGS["model"])
    except Exception as e:
        print(current_time_text("Model initialization failed", title="System", color="fail"))
        print(current_time_text(f"Error: {e}", title="System", color="fail"))
        exit(1)

    GENERATION_SETTINGS = SETTINGS['generation']
    OUTPUT_DIRECTORY = join(
        abspath(f'{BASE_DIRECTORY}../{GENERATION_SETTINGS["output_directory"]}'),
        SETTINGS['model']['model_name'],
        datetime.today().strftime('%Y-%m-%d_%H-%M-%S')
    )
    if not exists(OUTPUT_DIRECTORY):
        makedirs(OUTPUT_DIRECTORY)
    if GENERATION_SETTINGS["json_only_output_directory"]:
        OUTPUT_DIRECTORY_JSON_ONLY = abspath(f'{BASE_DIRECTORY}../{GENERATION_SETTINGS["json_only_output_directory"]}')
        if not exists(OUTPUT_DIRECTORY_JSON_ONLY):
            makedirs(OUTPUT_DIRECTORY_JSON_ONLY)
    else:
        OUTPUT_DIRECTORY_JSON_ONLY = None

    # Copy settings to output directory
    if GENERATION_SETTINGS['copy_settings_to_output']:
        with open(join(OUTPUT_DIRECTORY, "mapping_settings.json"), "w", encoding="utf-8") as f:
            json.dump(SETTINGS, f, indent=4)

    # Create stats 
    overall_stats = Stats({
        "model_origin": SETTINGS["model"]["model_origin"],
        "model_name": SETTINGS["model"]["model_name"]
    })
    pipeline_stats = Stats()
    current_stats = Stats()

    # Max fail limit for generation
    # If the max fail limit is reached, the program will exit
    # This is used to prevent repeating API calls
    MAX_FAIL_LIMIT = SETTINGS["generation"]["max_fail_limit"]
    max_fail_counter = 0
    max_tokens_fail_counter = 0

    # Load prompts
    prompts, prompt_loading_error = load_prompts(SETTINGS["generation"])
    if prompt_loading_error:
        tqdm.write(current_time_text(f"Error: {prompt_loading_error}.\nExiting...", title="System", color="fail"))
        exit(1)
    
    # Pipeline loop
    for pipeline_num, current_data_setting in enumerate(tqdm(SETTINGS["data"], desc="Pipeline", unit="source", leave=False, position=2)):
        tqdm.write(get_divider(f"Pipeline {pipeline_num + 1}"))
        pipeline_stats.reset_stats()
        pipeline_output_directory = join(
            OUTPUT_DIRECTORY,
            f"{current_data_setting['target_dataset'].split('/')[-1]}_{current_data_setting['target_db_id']}"
        )

        # Prepare source db
        question_folder = abspath(f'{BASE_DIRECTORY}../{current_data_setting["source_dataset"]}/questions')
        if not exists(question_folder):
            tqdm.write(current_time_text(f"Question folder '{question_folder}' does not exist. Skipping this pipeline...", title="Pipeline", color="fail"))
            continue

        # Read all question files in the folder
        # Each file should be the same db
        question_files = [f for f in listdir(question_folder) if (isfile(join(question_folder, f)) and f.endswith(".json"))]
        if len(question_files) == 0:
            tqdm.write(current_time_text(f"No question files found in '{question_folder}'. Skipping this pipeline...", title="System", color="fail"))
            continue

        # Load schemas
        schemas, schemas_invalid_reason = load_schemas(current_data_setting['source_dataset'], current_data_setting['target_dataset'], current_data_setting['source_db_ids'], current_data_setting['target_db_id'])
        if schemas_invalid_reason:
            tqdm.write(current_time_text(f"Error: {schemas_invalid_reason}.\nSkipping the pipeline...", title="Pipeline", color="fail"))
            continue

        target_samples = load_samples(current_data_setting['target_dataset'])
        if not target_samples:
            tqdm.write(current_time_text(f"No target sample found for '{current_data_setting['target_dataset']}'.\nSkipping...", title="Pipeline", color="fail"))
            continue

        tqdm.write(current_time_text(f"Pipeline for target {current_data_setting['target_dataset'].split('/')[-1]}_{current_data_setting['target_db_id']} started", title="System", ))

        # Number of source dbs skipped for the pipeline because 'source_db_ids' is not empty in settings
        db_skipping = 0
        # Database 1 to 1 loop
        source_db_files = tqdm(question_files, desc="Source DB", unit="db", leave=False, position=1)
        for source_db_question_file in source_db_files:
            current_stats.reset_stats()
            begin_time = time()
            
            # Load question file
            question_file_path = join(question_folder, source_db_question_file)
            question_data = prepare_questions(question_file_path, current_data_setting)

            # Check if the question data is valid
            if question_data == None:
                tqdm.write(current_time_text(f"Question data in '{question_file_path}' is not valid. Skipping...", title="Source DB", color="warning"))
                continue

            current_db = question_data[0]["db_id"]
            source_db_files.set_description(f"Source DB: {current_db}")
            # Skip if database is not in the list to map
            if len(current_data_setting["source_db_ids"]) != 0 and current_db not in current_data_setting["source_db_ids"]:
                # tqdm.write(current_time_text(f"Database '{current_db}' is not in the list. Skipping...", title="Source DB"))
                db_skipping += 1
                continue

            source_db_files.refresh()

            db_response = {"response": None, "json": []}

            # Questions batch loop
            questions_trange = trange(0, len(question_data), GENERATION_SETTINGS['max_question_length_per_prompt'], desc="Prompts", unit="prompt", leave=False, position=0)
            for i in questions_trange:
                current_questions = [q for q in question_data[i:i+GENERATION_SETTINGS['max_question_length_per_prompt']]]
                if len(current_questions) == 0:
                    tqdm.write(current_time_text(f"The questions file does not have index {i} to {i+GENERATION_SETTINGS['max_question_length_per_prompt']}.\nThis should not occur. Skipping...", title=f"Source DB: {current_db}", color="fail"))
                    break

                base_questions_prompt = prepare_prompt(
                    prompts=prompts,
                    questions=current_questions,
                    target_samples=target_samples,
                    schemas=schemas,
                    db_ids={"source": current_db, "target": current_data_setting['target_db_id']},
                )

                response_dict = None
                unexpected_error = None
                for attempt in range(GENERATION_SETTINGS['max_retry_per_prompt']):
                    errors = DEFAULT_ERRORS.copy()
                    current_stats.add_stats({"request": 1})
                    try:
                        response_dict = model.generate(base_questions_prompt, prompts['system'])
                        # {"response": str, "prompt_token_count": int, "response_token_count": int, "time_taken": float, "finish_reason"}
                    except Exception as e:
                        unexpected_error = str(e)
                        tqdm.write(current_time_text(unexpected_error, title=f"Source DB: {current_db}", color="fail"))
                        errors['unexpected'] = True
                        current_stats.add_unexpected_error(unexpected_error)
                        max_fail_counter += 1
                        if max_fail_counter >= MAX_FAIL_LIMIT:
                            max_fail_exit(unexpected_error, )

                    # TODO: Check for model limit reaching (prompt per minute, day) (request per minute, day)
                    if not errors['unexpected']:
                        db_response["response"] = response_dict['response']
                        write_response(
                            system_prompt=prompts['system'] if model.use_system_instruction else "",
                            prompt=base_questions_prompt,
                            response=db_response,
                            write_dir=pipeline_output_directory,
                            current_db=current_db
                        )
                        # Write the json response only
                        if OUTPUT_DIRECTORY_JSON_ONLY:
                            write_response(
                                system_prompt=prompts['system'] if model.use_system_instruction else "",
                                prompt=base_questions_prompt,
                                response=db_response,
                                write_dir=join(
                                    OUTPUT_DIRECTORY_JSON_ONLY,
                                    current_data_setting['target_dataset'].split('/')[-1],
                                    SETTINGS["model"]["model_name"],
                                    current_data_setting['target_db_id']
                                    ),
                                current_db=current_db,
                                json_only=True
                            )
                        # Only add stats if the response was successful
                        current_stats.add_stats({
                            "time_taken": response_dict['time_taken'],
                            "input_token": response_dict['prompt_token_count'],
                            "output_token": response_dict['response_token_count'],
                            })
                    # else:
                        # Removed calling count_tokens since it might cause the same error
                        # current_stats.add_stats({
                        #     "input_token": model.count_tokens(base_questions_prompt) if not model.use_system_instruction else model.count_tokens(prompts['system'] + base_questions_prompt),
                        # })

                    # Validate and make the response into json if there's no special errors
                    if not errors['unexpected'] and check_special_errors(errors, response_dict['response'], response_dict['finish_reason']):
                        json_bracket = response_dict['response'][response_dict['response'].find("["):response_dict['response'].rfind("]") + 1]
                        response_dict['json'], fixed = validate_json_response(errors, json_bracket)
                        if response_dict['json'] != None:
                            # Check if the response has all the fields required if set
                            if GENERATION_SETTINGS['validation']["fields_checking"] and (check_fields(response_dict['json'], GENERATION_SETTINGS['fields_to_check']) == False):
                                errors['fields_mismatch'] = True
                            # Check if the response has the desired source and target db id
                            if (not errors['fields_mismatch'] == True) and GENERATION_SETTINGS['validation']["db_id_matching"] and (check_db_id(current_db, response_dict['json'], current_data_setting['target_db_id']) > 0):
                                errors['db_id_matching'] = True

                            if errors['fields_mismatch'] == False and errors['db_id_matching'] == False:
                                if fixed:
                                    current_stats.add_stats({"corrected_response": 1})
                                else:
                                    current_stats.add_stats({"success_response": 1})
                                # Break the attempt loop if the response is valid
                                break

                    # Error occurred retrying
                    if not errors['unexpected']:
                        current_stats.add_stats({"error_response": 1})

                    #  Max Tokens reminder
                    if errors["max_token"]:
                        max_tokens_fail_counter += 1
                        if max_tokens_fail_counter >= MAX_TOKENS_FAIL_REMINDER:
                            tqdm.write(current_time_text(f"Reminder: Max token limit reached per response for {max_tokens_fail_counter} times. Please consider decrease questions per prompt.", title=f"Source DB: {current_db}", color="warning"))
                            # Reset the counter
                            max_tokens_fail_counter = 0
                    
                    # Print the errors
                    for key, value in errors.items():
                        if value:
                            tqdm.write(current_time_text(f"Error: {key} occurred. Attempt {attempt + 1}/{GENERATION_SETTINGS['max_retry_per_prompt']}", title=f"Source DB: {current_db}"))
                    
                    # Only prints if the error if not on last attempt
                    if attempt + 1 != GENERATION_SETTINGS['max_retry_per_prompt']:
                        tqdm.write(current_time_text(f"Generation failed. Retrying... Attempt {attempt + 1}/{GENERATION_SETTINGS['max_retry_per_prompt']}", title=f"Source DB: {current_db}"))
                    if errors["unexpected"]:
                        sleep(RETRY_TIMER) # Sleep to prevent rate limiting from the model
                
                # Error was not fixed after retries, skip the prompt
                if sum(errors.values()) != 0:
                    tqdm.write(current_time_text(f"Attempt {GENERATION_SETTINGS['max_retry_per_prompt']}/{GENERATION_SETTINGS['max_retry_per_prompt']} failed. Skipping the prompt.", title=f"Source DB: {current_db}", color="warning"))
                    # Dump errors, skipped prompts, questions
                    pipeline_stats.add_skipped_db_prompt(
                        system_prompt=prompts['system'] if model.use_system_instruction else "",
                        prompt=base_questions_prompt,
                        db_id=current_db,
                        questions=current_questions,
                        errors=errors,
                    )
                    continue

                # Make response none so it won't be append again
                db_response["response"] = None
                db_response["json"].extend(response_dict['json'])
                write_response(
                    system_prompt=prompts['system'] if model.use_system_instruction else "",
                    prompt=base_questions_prompt,
                    response=db_response,
                    write_dir=pipeline_output_directory,
                    current_db=current_db
                )
                if OUTPUT_DIRECTORY_JSON_ONLY:
                    write_response(
                        system_prompt=prompts['system'] if model.use_system_instruction else "",
                        prompt=base_questions_prompt,
                        response=db_response,
                        write_dir=join(
                            OUTPUT_DIRECTORY_JSON_ONLY,
                            current_data_setting['target_dataset'].split('/')[-1],
                            SETTINGS["model"]["model_name"],
                            current_data_setting['target_db_id']
                            ),
                        current_db=current_db,
                        json_only=True
                    )
            current_stats.add_stats({
                "response": current_stats.get_stats()["success_response"] + current_stats.get_stats()["error_response"] + current_stats.get_stats()["corrected_response"],
                "real_time": time() - begin_time,
            })
            
            # Current source db stats
            pipeline_stats.add_stats(current_stats.get_stats())
            write_stats(2, current_stats, pipeline_output_directory, current_db)
            tqdm.write(current_time_text(f"Finished source db '{current_db}'", title="Source DB", color="ok"))
        
        if db_skipping != 0:
            tqdm.write(current_time_text(f"Skipped {db_skipping} source dbs from {current_data_setting["source_dataset"]} because of settings", title="Pipeline", color="blue"))

        # Pipeline stats
        write_stats(1, pipeline_stats, pipeline_output_directory, current_db)
        overall_stats.add_stats(pipeline_stats.get_stats())
        tqdm.write(current_time_text(f"Finished pipeline {pipeline_num + 1}", title="System", color="ok"))
    
    # Overall stats
    write_stats(0, overall_stats, OUTPUT_DIRECTORY, current_db)
    print(current_time_text("Finished all pipelines", title="System", color="ok"))

if __name__ == "__main__":
    run()