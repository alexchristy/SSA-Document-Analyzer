import os
import openai
import logging
import time

class GPT3TurboAnalysis:
    def __init__(self, api_key=None):
        """
        Initialize GPT3TurboDestinationAnalysis class.
        
        Parameters:
        - api_key (str): OpenAI API key. If None, falls back to environment variable.
        """
        self.api_key = api_key if api_key else os.getenv("OPENAI_API_KEY")
        
        # Fallback to environment variable if api_key is not provided
        if self.api_key is None:
            logging.warning("API Key not provided, attempting to use environment variable.")
        
        openai.api_key = self.api_key

    def get_destination_analysis(self, destination_data: str) -> str:
        """
        Analyzes a destination data string and returns a corrected and parsed version.
        
        Parameters:
        - destination_data (str): The raw destination data string.
        
        Returns:
        - str: The analyzed destination string as returned by GPT-3.
        """
        if destination_data == '':
            logging.warning("Empty destination data string provided.")
            return None

        # Number of attempts to make
        num_attempts = 5

        # Initial delay in seconds
        delay = 1

        for attempt in range(1, num_attempts + 1):
            try:
                # Formatting the request string for GPT-3
                request_content_str =  f"This string comes from a table. The cell it comes from lists the destinations for a flight. List the destinations you find as a python list and correct any typos in the destinations so they are real places. Return None if the string contains MORE than JUST destinations looking at the entire context of the string to see if it's a note or additional information. We are working with military bases so the destinations might include references to bases and naval stations. If a destination string mentions an AB or AFB or NS or NAS and INTL or another version of the word international those are two different destinations. Destinations can be surrounded by the characters such as \"*\" \"-\" and \"_\" and are commonly used alone or together to seperate destinations. The same destination might show up more than once. Sometimes you will see the string say Patriot Express in some variation this is not a destination.\nExample 1: \"*S/A Passengers must have a Spanish residency ID or have a Spanish Passport to make Rota, Spain their destination.\" -> None\nExample 2: \"DIEGO GARCIA (DELAYED MISSION)\" -> [\"DIEGO GARCIA\"]\nExample 3: \"BAHRAIN *** ( Patriot Express )\" -> [\"BAHRAIN\"]\nExample 4: \"PROTA,SP/SIGONELLA, IT/BAHRAIN/\" -> [\"ROTA, SP\", \"SIGONELLA, IT\", \"BAHRAIN\"]\nExample 5: \"Rota,SP is not available to all passengers\" -> None\nExample 6: \"JOINT BASE ANDREWS, MD\" -> [\"JOINT BASE ANDREWS, MD\"]\nExample 7: \"PRINCE SULTAN AB- TAIF INTL\" -> [\"PRINCE SULTAN AB\", \"TAIF INTL\"]\nExample 8: \"ALI AL SALEM AB - **RUMIL** ALI AL SALEM AB\" -> [\"ALI AL SALEM AB\", \"RUMIL\", \"ALI AL SALEM AB\"]\nExample 9: \"SIGONELLA, ITA BAHRAIN, BAH DIEGO GARCIA PATRIOT EXPRESS\" -> [\"SIGONELLA, ITA\", \"BAHRAIN\", \"DIEGO GARCIA\"]\nString: \"{destination_data}\""

                # Making the API request
                response = openai.ChatCompletion.create(
                    model="ft:gpt-3.5-turbo-0613:personal::8AQqGP3S",
                    messages=[
                        {
                            "role": "user",
                            "content": request_content_str
                        }
                    ],
                    temperature=0,
                    max_tokens=256,
                    top_p=1,
                    frequency_penalty=0,
                    presence_penalty=0
                )

                return response['choices'][0]['message']['content']

            except openai.OpenAIError as e:

                if 'The server is overloaded or not ready yet' in str(e):  # Adjust this to match the actual error message
                    logging.warning(f"Server is overloaded. Attempt {attempt}/{num_attempts}. Retrying in {delay} seconds.")
                    time.sleep(delay)
                    delay *= 2  # Double the delay for the next attempt
                    if delay > 10:  # Cap the delay to 5 seconds
                        delay = 10
                else:
                    logging.error(f"GPT3 Destination Analysis error: {e}")
                    return None
            except Exception as e:
                logging.error(f"An unexpected error occurred: {e}")
                return None

        logging.error("Max retry attempts reached. Server is still overloaded.")
        return None