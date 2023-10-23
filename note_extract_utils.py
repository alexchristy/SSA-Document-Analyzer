import logging
import re
from typing import List, Optional


def _remove_spaces_around_asterisks(input_str: str) -> Optional[str]:
    try:
        return re.sub(r"\s?(\*+)\s?", r"\1", input_str)
    except Exception as e:
        logging.error("Error in _remove_spaces_around_asterisks: %s", e)
        return None


def _extract_single_asterisk_note(text: str) -> str:
    """Extract a note from a string if it is enclosed between at least two '*' characters.

    Args:
    ----
    text (str): The input string containing the note.

    Returns:
    -------
    str: The note text if found, otherwise None.
    """
    try:
        # Using regular expression to find text between '*' characters
        # The pattern ((\*+\s*)+)+ matches one or more '*' followed by optional spaces, one or more times
        # The pattern .*? matches any character (non-greedy)
        match = re.search(r"((\*+\s*)+)+\s*(.*?)\s*((\*+\s*)+)+", text)
        if match and match.group(3).strip():
            return match.group(3)
        return ""
    except Exception as e:
        logging.error("Error in _extract_single_note: %s", e)
        return ""


def _reverse_string(s: str) -> str:
    return s[::-1]


def _merge_lists(list1: List, list2: List) -> List:
    try:
        return list(set(list1 + list2))
    except Exception as e:
        logging.error("Error in _merge_lists: %s", e)
        return []


def _extract_multiple_asterisk_notes(text: str) -> list:
    """Extract multiple notes from a string if they are enclosed between at least two '*' characters.

    Helper function for extract_notes. Extracts multiple notes from a string by finding single notes enclosed between
    '*' characters and then chopping them off. It removes the minimum number of surrounding asterisks.

    Args:
    ----
    text (str): The input string containing the notes.

    Returns:
    -------
    list: A list of notes if found, otherwise an empty list.
    """
    try:
        notes = []
        cleaned_text = _remove_spaces_around_asterisks(text)
        if cleaned_text is None:
            return []

        while True:
            parsed_note = _extract_single_asterisk_note(cleaned_text)
            if parsed_note is None:
                continue

            note = parsed_note
            if note:
                notes.append(note)
                note_with_asterisks_match = re.search(
                    r"((\*+\s*)+)+\s*.*?\s*((\*+\s*)+)+", cleaned_text
                )

                if note_with_asterisks_match is None:
                    break

                start_asterisks = note_with_asterisks_match.group(1).count("*")
                end_asterisks = note_with_asterisks_match.group(4).count("*")
                min_asterisks = min(start_asterisks, end_asterisks)
                removal_substring = "*" * min_asterisks + note + "*" * min_asterisks
                cleaned_text = cleaned_text.replace(removal_substring, "", 1).strip()
            else:
                break
        return notes
    except Exception as e:
        logging.error("Error in _extract_multiple_notes: %s", e)
        return []


def _extract_asterisk_notes(text: str) -> list:
    """Extract notes from a string if they are enclosed between at least one '*' characters.

    This function takes a string with asterick notes and returns the notes in the string. It ignores whitespace around
    and in between the astericks. The notes can be enclosed by any number of astericks and notes can share astericks.

    Example:
    -------
        "This is note a note **This is a note** and this is also a note**" --> ['This is a note', 'and this is also a note']

    Args:
    ----
        text (str): The string to extract notes from

    Returns:
    -------
        list: A list of notes found in the string
    """
    try:
        notes_list_1 = _extract_multiple_asterisk_notes(text)

        reversed_text = _reverse_string(text)

        notes_list_2 = _extract_multiple_asterisk_notes(reversed_text)
        notes_list_2 = [_reverse_string(note) for note in notes_list_2]

        notes = _merge_lists(notes_list_1, notes_list_2)

        logging.info("Extracted notes: %s", notes)
        return notes
    except Exception as e:
        logging.error("Error in extract_notes: %s", e)
        return []


def _extract_parenthesis_notes(text: str) -> list:
    """Extract a list of strings that are enclosed in parentheses.

    Example:
    -------
        "This is not a note (This is a note) and this is also not a note" --> ['This is a note']

    Args:
    ----
        text (str): The string to extract notes from

    Returns:
    -------
        list: A list of notes found in the string
    """
    # Initialize an empty list to store the extracted notes
    extracted_notes = []

    try:
        # Use regex to find all substrings enclosed within ()
        notes = re.findall(r"\((.*?)\)", text)

        for note in notes:
            # Append each found note to the list
            extracted_notes.append(note.strip())

        logging.info("Successfully extracted %d notes.", len(extracted_notes))

    except Exception as e:
        logging.error("An error occurred while extracting notes: %s", e)

    return extracted_notes


def extract_notes(text: str) -> list:
    """Extract notes from a string if they are enclosed between '*' characters or parenthesis.

    Wrapper function that extracts all notes from a string using the extract_asterisk_notes and extract_parenthesis_notes.

    Args:
    ----
        text (str): The string to extract notes from

    Returns:
    -------
        list: A list of notes found in the string
    """
    notes = []

    try:
        notes = _extract_asterisk_notes(text)
        notes = _merge_lists(notes, _extract_parenthesis_notes(text))
    except Exception as e:
        logging.error("Error in extract_notes: %s", e)

    return notes
