import json
import copy
from datetime import datetime

class Stats:
    '''
    Class to handle the statistics of the model.
    '''
    def __init__(self, extra_stats: dict = None):
        '''
        Initialize the Stats class with default values.
        Args:
            extra_stats (dict): Dictionary containing extra stats to be added to the default stats.
        '''
        self.__default = {
            "time_taken": 0,                # Time taken for all model response time for successful generation
            "real_time": 0,                 # Time taken for execution
            "input_token": 0,               # Not added if unexpected error happens
            "output_token": 0,
            "request": 0,                   # Number of requests sent (include failed requests and retries which might not add to input_token)
            "response": 0,                  # Total number of responses = success + error + corrected (retries will be added on to any response)
            "success_response": 0,          # No error or correction needed
            "error_response": 0,            # Error in response (cannot be used but generated)
            "corrected_response": 0,        # Error in response but corrected through algorithm
            "unexpected_error": 0,          # Error happened and prompt is not generated (e.g. model not found, exceptions, etc.)
        }
        self.unexpected_errors = []
        if extra_stats is not None:
            self.__default.update(extra_stats)
        self.full_stats = copy.deepcopy(self.__default)
        self.start_time = datetime.today().strftime('%Y-%m-%d_%H-%M-%S')
        self.skipped_db_prompts = []

    def __str__(self):
        '''
        Flatten stats on top level for easy printing.
        '''
        return_str = ""
        max_length = max(len(key) for key in self.full_stats.keys()) + 2
        for key, value in self.full_stats.items():
            key = key.replace("_", " ").title()
            if key == "Time Taken":
                key = "Time Taken (s)"
                return_str += f"{key.ljust(max_length)}: {value:.2f}\n"
            else:
                if isinstance(value, float):
                    return_str += f"{key.ljust(max_length)}: {value:.2f}\n"
                else:
                    return_str += f"{key.ljust(max_length)}: {value}\n"
        return return_str

    def __repr__(self) -> str:
        return json.dumps(self.full_stats, indent=4)

    def get_stats(self):
        return self.full_stats
    
    def update_default_stats(self, stats: dict) -> None:
        '''
        Update the stats with new values.
        Args:
            stats (dict): Dictionary containing the new stats to update. If the value is None, they will be removed from default.
        '''
        for key, value in stats.items():
            if value is None:
                del self.__default[key]
            else:
                self.__default[key] = value
    
    def to_json_str(self) -> str:
        '''
        Convert the stats to JSON format.
        Returns:
            str: JSON string of the stats.
        '''
        return json.dumps(self.full_stats, indent=4)
    
    def reset_stats(self) -> None:
        '''
        Reset the stats to default values.
        '''
        del self.full_stats
        self.full_stats = copy.deepcopy(self.__default)
        self.unexpected_errors = []
        self.start_time = datetime.today().strftime('%Y-%m-%d_%H-%M-%S')
        self.skipped_db_prompts = []
    
    def add_skipped_db_prompt(self, system_prompt: str, prompt: str, db_id: str, questions: list, errors: dict) -> None:
        '''
        Add a skipped database prompt to the stats.
        Args:
            system_prompt (str): The system prompt that was skipped.
            prompt (str): The prompt that was skipped.
            db_id (str): The database ID associated with the prompt.
            questions (list): List of questions associated with the prompt.
            errors (dict): Dictionary containing errors associated with the prompt. True for error, False for no error.
        '''
        self.skipped_db_prompts.append({
            "system_prompt": system_prompt,
            "prompt": prompt,
            "db_id": db_id,
            "questions": questions,
            "errors": errors
        })

    def add_unexpected_error(self, error: str) -> None:
        '''
        Add an error to the stats.
        Args:
            error (str): Error message to be added.
        '''
        self.unexpected_errors.append(error)
        self.full_stats["unexpected_error"] += 1
        
    def add_stats(self, stats: dict) -> None:
        '''
        Add stats to the full stats.
        Args:
            stats (dict): Dictionary containing the stats to be added.
        '''
        for key, value in stats.items():
            if key in self.full_stats and isinstance(value, (int, float)):
                self.full_stats[key] += value