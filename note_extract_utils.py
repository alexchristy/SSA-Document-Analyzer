import re
import logging

def _remove_spaces_around_asterisks(input_str):
    try:
        return re.sub(r'\s?(\*+)\s?', r'\1', input_str)
    except Exception as e:
        logging.error(f"Error in _remove_spaces_around_asterisks: {e}")
        return None
    
def _extract_single_note(text: str) -> str:
    """
    Extracts a note from a string if it is enclosed between at least two '*' characters.
    
    Args:
    - text (str): The input string containing the note.
    
    Returns:
    - str: The note text if found, otherwise None.
    """
    try:
        # Using regular expression to find text between '*' characters
        # The pattern ((\*+\s*)+)+ matches one or more '*' followed by optional spaces, one or more times
        # The pattern .*? matches any character (non-greedy)
        match = re.search(r'((\*+\s*)+)+\s*(.*?)\s*((\*+\s*)+)+', text)
        if match and match.group(3).strip():
            return match.group(3)
        else:
            return None
    except Exception as e:
        logging.error(f"Error in _extract_single_note: {e}")
        return None

def _reverse_string(s: str) -> str:
    return s[::-1]

def _merge_lists(list1, list2):
    try:
        return list(set(list1 + list2))
    except Exception as e:
        logging.error(f"Error in _merge_lists: {e}")
        return []

def _extract_multiple_notes(text: str) -> list:
    """
    Helper function for extract_notes. Extracts multiple notes from a string by finding single notes enclosed between 
    '*' characters and then chopping them off. It removes the minimum number of surrounding asterisks.
    
    Args:
    - text (str): The input string containing the notes.
    
    Returns:
    - list: A list of notes if found, otherwise an empty list.
    """

    try:
        notes = []
        text = _remove_spaces_around_asterisks(text)
        if text is None:
            return []

        while True:
            note = _extract_single_note(text)
            if note:
                notes.append(note)
                note_with_asterisks_match = re.search(r'((\*+\s*)+)+\s*.*?\s*((\*+\s*)+)+', text)
                
                if note_with_asterisks_match is None:
                    break

                start_asterisks = note_with_asterisks_match.group(1).count('*')
                end_asterisks = note_with_asterisks_match.group(4).count('*')
                min_asterisks = min(start_asterisks, end_asterisks)
                removal_substring = '*' * min_asterisks + note + '*' * min_asterisks
                text = text.replace(removal_substring, '', 1).strip()
            else:
                break
        return notes
    except Exception as e:
        logging.error(f"Error in _extract_multiple_notes: {e}")
        return []

def extract_notes(text: str) -> list:

    '''
    This function takes a string with asterick notes and returns the notes in the string. It ignores whitespace around
    and in between the astericks. The notes can be enclosed by any number of astericks and notes can share astericks.

    Example:

        "This is note a note **This is a note** and this is also a note**" --> ['This is a note', 'and this is also a note']

    Args:
        text (str): The string to extract notes from

    Returns:
        list: A list of notes found in the string
    '''

    try:

        notes_list_1 = _extract_multiple_notes(text)

        reversed_text = _reverse_string(text)

        notes_list_2 = _extract_multiple_notes(reversed_text)
        notes_list_2 = [_reverse_string(note) for note in notes_list_2]

        notes = _merge_lists(notes_list_1, notes_list_2)

        logging.info(f"Extracted notes: {notes}")
        return notes
    except Exception as e:
        logging.error(f"Error in extract_notes: {e}")
        return []


# Test the adjusted function with multiple notes in a string
test_strings = [

    "This is not a note"
]

for i, test_str in enumerate(test_strings):
    print(f"Test case {i+1}: Extracted notes: {extract_notes(test_str)}")