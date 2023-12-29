import logging
import os
import time
from typing import Optional

import openai
from openai.error import RateLimitError

DELAY_MAX = 10


class GPT3TurboAnalysis:
    """Class for analyzing data strings using GPT-3 Turbo."""

    def __init__(self: "GPT3TurboAnalysis", api_key: Optional[str] = None) -> None:
        """Initialize GPT3TurboDestinationAnalysis class.

        Args:
        ----
        api_key (str): OpenAI API key. If None, falls back to environment variable.
        """
        self.api_key = api_key if api_key else os.getenv("OPENAI_API_KEY")

        # Fallback to environment variable if api_key is not provided
        if self.api_key is None:
            logging.warning(
                "API Key not provided, attempting to use environment variable."
            )

        openai.api_key = self.api_key

    def get_destination_analysis(
        self: "GPT3TurboAnalysis", destination_data: str
    ) -> Optional[str]:
        """Analyzes a destination data string and returns a corrected and parsed version.

        Args:
        ----
        destination_data (str): The raw destination data string.

        Returns:
        -------
        - str: The analyzed destination string as returned by GPT-3.
        """
        if destination_data == "":
            logging.warning("Empty destination data string provided.")
            return None

        # Number of attempts to make
        num_attempts = 5

        # Initial delay in seconds
        delay = 1

        for attempt in range(1, num_attempts + 1):
            try:
                system_str = """This string comes from a table. The cell it comes from lists the destinations for a flight. List the destinations you find as a python list and correct any typos in the destinations so they are real places. Return None if the string contains MORE than JUST destinations looking at the entire context of the string to see if it's a note or additional information. We are working with military bases so the destinations might include references to bases and naval stations. If a destination string mentions an AB or AFB or NS or NAS and INTL or another version of the word international those are two different destinations. Make sure destination designations like AB or AFB or NS or NAS and INTL or another version of the word international are separated by a space from the name of the destination. A destinations countries and regions should be kept together with the destination. Destinations can be surrounded by the characters such as *,-, and _ and are commonly used alone or together to separate destinations. The same destination might show up more than once. If the same destination appears sequentially combine it with a comma into one destination. Sometimes you will see the string say Patriot Express in some variation this is not a destination."""
                # Formatting the request string for GPT-3
                request_content_str = destination_data

                # Making the API request
                response = openai.ChatCompletion.create(
                    model="ft:gpt-3.5-turbo-0613:smartspacea::8auP0APZ",
                    messages=[
                        {"role": "system", "content": system_str},
                        {"role": "user", "content": request_content_str},
                    ],
                    temperature=0,
                    max_tokens=256,
                    top_p=1,
                    frequency_penalty=0,
                    presence_penalty=0,
                )

                return response["choices"][0]["message"]["content"]
            except RateLimitError:  # Specifically catch RateLimitError
                logging.warning(
                    "Rate limit exceeded. Attempt %s/%s. Retrying in %s seconds.",
                    attempt,
                    num_attempts,
                    delay,
                )
                time.sleep(delay)
                delay *= 2
                if delay > DELAY_MAX:
                    delay = DELAY_MAX

            except openai.OpenAIError as e:
                if "The server is overloaded or not ready yet" in str(
                    e
                ):  # Adjust this to match the actual error message
                    logging.warning(
                        "Server is overloaded. Attempt %s/%s. Retrying in %s seconds.",
                        attempt,
                        num_attempts,
                        delay,
                    )
                    time.sleep(delay)
                    delay *= 2  # Double the delay for the next attempt
                    if delay > DELAY_MAX:  # Cap the delay to 5 seconds
                        delay = 10
                else:
                    logging.error("GPT3 Destination Analysis error: %s", e)
                    return None
            except Exception as e:
                logging.error("An unexpected error occurred: %s", e)
                return None

        logging.error("Max retry attempts reached. Server is still overloaded.")
        return None
