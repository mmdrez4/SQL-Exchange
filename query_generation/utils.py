import json
from getpass import getpass
from datetime import datetime

def check_fields(response: json, fields: list) -> bool:
    '''
    Check if all fields are present in the response.
    Args:
        response (json): the json object to check
        fields (list): list of fields to check
    Returns:
        (bool): True if all fields are present, False otherwise
    '''
    for question in response:
        keys = question.keys()
        if len(keys) != len(fields):
            return False
        
        for field in fields:
            if field not in keys:
                return False
    return True

def check_special_errors(errors: dict, response_text: str, finish_reason: str = None) -> bool:
    '''
    Check fo special errors

    Args:
        errors (dict): dictionary to determine which error it is
        response_text (str): the response text from model

    Returns:
        (bool): True if pass the tests, false otherwise
    '''
    if len(response_text) == 0:
        errors["empty_output"] = True
        return False
    elif finish_reason.upper() == "MAX_TOKEN":
        errors["max_token"] = True
        return False
    elif finish_reason.upper() == "RECITATION":
        errors["recitation"] = True
        return False
    
    return True

def validate_json_response(errors: dict, response_text: str) -> tuple:
    '''
    Validate the text response to json. Fix comma delimiter issue if fixable.

    Args:
        errors (dict): the errors stats from core
        response_text (str): the text generated from LLM

    Returns:
        (tuple): tuple of the json object and a boolean indicating if the response was fixed
        
    '''
    try:
        # Load the JSON response
        loaded_json = json.loads(response_text)
        
        return loaded_json, False

    except json.JSONDecodeError as e:
        if "Expecting ',' delimiter" in str(e):
            comma_fixed, fixed_response, error_type = fix_missing_comma(response_text)
            if comma_fixed:
                loaded_json = json.loads(response_text)
                return json.loads(fixed_response), True
            else:
                errors[error_type] = True
        elif "Invalid" in str(e) and "escape" in str(e):
            errors['invalid_escape'] = True
        else:
            errors['json_decode'] = True
            
        return None, None

def check_db_id(db_id: str = None, json_obj: json = None, target_db_id: str = None) -> int:
    '''
    Checks the db_id generated matches the desire db_id
    and the target db id if provided.
    This function requires the json_obj to have the source_db_id and target_db_id fields.

    Args:
        db_id (str): the desired source db id
        json_obj (json): the generated json from LLM
        target_db_id (str): the desired target db id
    
    Returns:
        fails (int): number of different db id generated
    '''
    failed = 0

    for response in json_obj:
        if response['source_db_id'] != db_id:
            failed += 1

        if target_db_id and response['target_db_id'] != target_db_id:
            failed += 1
    
    return failed

def fix_missing_comma(content: str) -> tuple:
    """
    Detects and attempts to fix missing commas in a JSON file.

    Args:
        text (str): the text to be fixed.

    Returns:
        tuple: A tuple containing:
        - Fixed (bool): False if cannot be fixed. True if fixed or no fix needed.
        - json_obj (json): The fixed JSON object or None if not fixable.
        - error_type (str): The type of error encountered, if any.
    """

    try:
        json_obj = json.loads(content)  # Check if already valid JSON
        return True, json_obj, None  # No fix needed
    except json.JSONDecodeError as e:
        # Try to pinpoint the missing comma
        error_msg = str(e)
        if "Expecting ',' delimiter" in error_msg:
        # Find the error position 
            error_pos = int(error_msg.split(" ")[-1].strip("()"))
            # Find the last nextLine character before the error position
            last_newline = content[:error_pos].rfind('\n')
            if last_newline != -1 and content[last_newline - 1] != ',':
                error_pos = last_newline

            # Insert the missing comma
            content = content[:error_pos] + ',' + content[error_pos:]

            # Check if it's valid JSON now for returning
            return fix_missing_comma(content)
        elif "Invalid" in error_msg and "escape" in error_msg:
            return False, None, "invalid_escape"
        else:
            return False, None, "json_decode"  # Different JSON error, can't fix automatically

def get_divider(text: str=None, width: int=100, character: str="=") -> str:
    '''
    A helper function to create a divider with text in the middle.

    Args:
        text (str): The text to display in the middle of the divider.
        width (int): The width of the divider.
        character (str): The character to copy

    Returns:
        A string of the divider.
    '''
    return_text = ""
    if text:
        text = " " + text + " "
        left = (width - len(text)) // 2
        right = width - len(text) - left
        return_text += character * left + text + character * right
    else:
        return_text += character * width
    return return_text

def create_env_variable(env_file_path: str, env_variable: str) -> str:
    '''
    Create an environment variable.

    Args:
        env_file_path (str): The path to the .env file.
        env_variable (str): The name of the environment variable.

    Returns:
        str: The value of the environment variable.
    '''
    print(get_divider(f"Creating {env_variable} environment variable"))
    print(f"This variable will be stored in '{env_file_path}'\n for future use.")
    value = getpass(f"Enter the value for {env_variable} (hidden): ")
    if not value:
        raise ValueError(f"{env_variable} cannot be empty.")
    with open(env_file_path, 'a') as f:
        f.write(f"\n{env_variable}={value}")

    return value

def current_time_text(text: str, title: str = None, color: str = None) -> str:
    '''
    Make the text colorful and add the current time to it.
    Args:
        text (str): The message to print.
        title (str): The title of the message.
        color (str): The color of the message. Can be "blue", "ok", "warning", "fail".
        
    Returns:
        str: The colored message with the current time.
    '''
    BLUE = '\033[94m'
    OK = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    END = '\033[0m'
    return_str = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
    if title:
        return_str += f"[{title}] "
    match color:
        case "blue":
            return_str += f"{BLUE}{text}{END}"
        case "ok":
            return_str += f"{OK}{text}{END}"
        case "warning":
            return_str += f"{WARNING}{text}{END}"
        case "fail":
            return_str += f"{FAIL}{text}{END}"
        case _:
            return_str += f"{text}"
    return return_str