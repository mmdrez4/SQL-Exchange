from time import time
import os
import json
import tiktoken

# Import model libraries
from google import genai
from google.genai import types
from openai import OpenAI

# Load environment variables from .env file
# If variables are already set in the environment, they will be overridden by the values in the .env file
from dotenv import load_dotenv
load_dotenv(override=True)

from .utils import create_env_variable

BASE_DIRECTORY = os.path.dirname(os.path.abspath(__file__)) + '/'   # Path to the current file

class CustomModel:

    def __init__(self, settings: json = None):
        self.__SETTINGS = settings
        self.model_origin = self.__SETTINGS["model_origin"].lower()
        self.model_name = self.__SETTINGS["model_name"]
        self.use_system_instruction = self.__SETTINGS["use_system_instruction"]
        self.input_token_limit = self.__SETTINGS["input_token_limit"]
        self.models_list = None

        if not self.model_name:
            raise ValueError("Model name is required.")

        # Initialize the model based on the origin and model name with their respective API keys
        match self.model_origin:
            # Initialize OpenAI models
            case 'openai':
                # Check for API key
                OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
                if not OPENAI_API_KEY:
                    OPENAI_API_KEY = create_env_variable(BASE_DIRECTORY + '.env', 'OPENAI_API_KEY')

                self.model = OpenAI(api_key=OPENAI_API_KEY)
                self.models_list = [m['id'] for m in self.model.models.list().to_dict()["data"]]

                # Check if the model name is in the list of available models
                if self.model_name not in self.models_list:
                    raise ValueError(f"Model '{self.model_name}' is not available in the OpenAI API.")

            # Initialize Google models
            case 'google':
                # Check for API key
                GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
                if not GOOGLE_API_KEY:
                    GOOGLE_API_KEY = create_env_variable(BASE_DIRECTORY + '.env', 'GOOGLE_API_KEY')
                
                self.model = genai.Client(api_key=GOOGLE_API_KEY)
                self.models_list = [m.name[len("models/"):] for m in self.model.models.list() if "generateContent" in m.supported_actions]

                # Check if the model name is in the list of available models
                if self.model_name not in self.models_list:
                    raise ValueError(f"Model '{self.model_name}' is not available in the Google API.")

            case _:
                raise ValueError(f"Model origin '{self.model_origin}' is not supported in initialization.")
            
        print(f"Model '{self.model_name}' initialized successfully from '{self.model_origin}'.")

    def available_models(self) -> list[str] | None:
        '''
        return: list of available models
        '''
        return self.models_list
    
    def count_tokens(self, text: str) -> int:
        '''
        Count the number of tokens in a given text using the model's tokenizer.
        Args:
            text (str): The input text to count tokens for.
        Returns:
            int: The number of tokens in the text.
        '''
        match self.model_origin:
            case 'openai':
                enc = tiktoken.encoding_for_model(self.model_name)
                return len(enc.encode(text))
            case 'google':
                response = self.model.models.count_tokens(model=self.model_name, contents=text)
                return response.total_tokens
            case _:
                raise ValueError(f"Model origin '{self.model_origin}' is not supported in count_tokens.")
            
    def get_token_limit(self) -> int:
        '''
        Get the token limit for the model.
        Returns:
            int: The token limit for the model.
        '''
        match self.model_origin:
            case 'openai':
                if "input_token_limit" not in self.__SETTINGS or self.__SETTINGS["input_token_limit"] is None:
                    raise ValueError("input_token_limit needs to be set in settings.\nOpenAI does not support API call check input limit.")
                return self.__SETTINGS["input_token_limit"]
            case 'google':
                return self.model.models.get(model=self.model_name).input_token_limit
            case _:
                # Implement model here
                raise ValueError(f"Model origin '{self.model_origin}' is not supported in get_token_limit.")

    def generate(self, prompt: str, system_instruction: str = "") -> dict:
        '''
        Generate a response from the model based on the prompt and system instruction.
        Args:
            prompt (str): The input prompt for the model.
            system_instruction (str): Optional system instruction to guide the model's behavior.
        Returns:
            str: The generated response from the model.
        '''
        match self.model_origin:
            case 'openai':
                return self.__generate_openai(prompt, system_instruction)
            case 'google':
                return self.__generate_google(prompt, system_instruction)
            case _:
                # Implement model here
                raise ValueError(f"Model origin '{self.model_origin}' is not supported in generate.")

    def __generate_openai(self, prompt: str, system_instruction: str) -> dict:
        '''
        Generate a response from the OpenAI model based on the prompt and system instruction.
        Args:
            prompt (str): The input prompt for the model.
            system_instruction (str): Optional system instruction to guide the model's behavior.
        Returns:
            dict: A dictionary containing the generated response and token counts.
        '''

        messages = [{"role": "user", "content": prompt}]
        if self.use_system_instruction:
            messages = [{"role": "system", "content": system_instruction}, {"role": "user", "content": prompt}]

        begin = time()
        completion = self.model.chat.completions.create(
                        model=self.model_name,
                        messages=messages,
                        temperature=self.__SETTINGS["temperature"],
                        top_p=self.__SETTINGS["top_p"] if "top_p" in self.__SETTINGS.keys() else None,
                    )
        
        return {"response": completion.choices[0].message.content,
                "prompt_token_count": completion.usage.prompt_tokens,
                "response_token_count": completion.usage.completion_tokens,
                "time_taken": time() - begin,
                "finish_reason": ""
                }

    def __generate_google(self, prompt: str, system_instruction: str) -> dict:
        '''
        Generate a response from the Google model based on the prompt and system instruction.
        Args:
            prompt (str): The input prompt for the model.
            system_instruction (str): Optional system instruction to guide the model's behavior.
        Returns:
            dict: A dictionary containing the generated response and token counts.
        '''
        begin = time()
        response = self.model.models.generate_content(
                        model=self.model_name,
                        config=types.GenerateContentConfig(
                            system_instruction = system_instruction if self.use_system_instruction else None,
                            temperature=self.__SETTINGS["temperature"],
                            top_p=self.__SETTINGS["top_p"] if "top_p" in self.__SETTINGS.keys() else None,
                            top_k=self.__SETTINGS['google']["top_k"] if 'google' in self.__SETTINGS.keys() and 'top_k' in self.__SETTINGS['google'].keys() else None,
                        ),
                        contents = prompt
                    )
        
        finish_reason = None
        match response.candidates[0].finish_reason.upper():
            case "4" | 4 | "RECITATION":
                finish_reason = "RECITATION"
            case "2" | 2 | "MAX_TOKENS":
                finish_reason = "MAX_TOKENS"
            case _:
                # Handle other finish reasons if needed
                finish_reason = ""
        
        return {"response": response.text,
                "prompt_token_count": response.usage_metadata.prompt_token_count,
                "response_token_count": response.usage_metadata.candidates_token_count,
                "time_taken": time() - begin,
                "finish_reason": finish_reason
                }