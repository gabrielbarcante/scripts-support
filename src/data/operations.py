import re

def prepare_regex_pattern(term: str, space_between_chars: bool = False) -> str:
    """
    Prepare a regex pattern from a search term, optionally allowing spaces between characters.
    
    Args:
        term: The search term. Can contain <regex>...</regex> tags for raw regex.
        space_between_chars: If True, add optional whitespace between characters.
        
    Returns:
        str: A regex pattern string.
        
    Example:
        >>> prepare_regex_pattern("test.com")
        'test\\.com'
        >>> prepare_regex_pattern("abc", space_between_chars=True)
        'a\\s?b\\s?c'
    """
    if not isinstance(term, str):
        raise ValueError("Input term must be a string")
    
    if not term:
        return term

    place_holder = "<<<REGEX_PLACEHOLDER>>>"
    raw_regex = None
    start_pos = 0
    end_pos = 0
    regex_term = False
    
    # Extract raw regex content between <regex></regex> tags
    if "<regex>" in term and "</regex>" in term:
        start_pos = term.index("<regex>")
        end_pos = term.index("</regex>")
        raw_regex = term[start_pos + len("<regex>"):end_pos]
        # Temporarily replace with placeholder
        term = term[:start_pos] + place_holder + term[end_pos + len("</regex>"):]

        regex_term = True

    # Escape regex special characters in non-regex parts
    term = re.escape(term)
    
    # Restore raw regex content
    if raw_regex is not None:
        start_pos = term.index(place_holder)
        end_pos = start_pos + len(raw_regex)
        term = term.replace(place_holder, raw_regex)

    # Add optional whitespace between characters
    if space_between_chars:
        result = []
        skip_next = False
        
        for i, c in enumerate(term):
            if skip_next:
                skip_next = False
                continue

            if regex_term and i >= start_pos and i < end_pos:
                result.append(c)
                continue
                
            result.append(c)
            
            if c == "\\":
                result.append(term[i + 1])
                if i < len(term) - 2: 
                    result.append(r"\s?")
                skip_next = True
            elif i < len(term) - 1:
                result.append(r"\s?")
        
        term = "".join(result)
    
    return term


def match_string(search_value: str, comparison: str, regex: bool = False, prepare_search_value: bool = False, case_sensitive: bool = False, exact_match: bool = False) -> bool:
    """
    Check if a search value matches a comparison string.
    
    Args:
        search_value: The value to search for.
        comparison: The string to search in.
        regex: If True, treat search_value as a regex pattern.
        prepare_search_value: If True, automatically escape special regex characters, like '.', '+', '*', ..., and process <regex>...</regex> tags using prepare_regex_pattern(). Only applies when regex=True.
        case_sensitive: If True, perform case-sensitive matching.
        exact_match: If True, require exact match (or full string match for regex).
        
    Returns:
        bool: True if the search value matches, False otherwise.
        
    Example:
        >>> match_string("test", "This is a test")
        True
        >>> match_string("TEST", "test", case_sensitive=True)
        False
        >>> match_string("test", "test", exact_match=True)
        True
        >>> match_string("test.com", "visit test.com", regex=True, prepare_search_value=True)
        True
    """
    if not isinstance(search_value, str):
        raise ValueError("search_value must be a string")
    
    if not isinstance(comparison, str):
        raise ValueError("comparison must be a string")
    
    if prepare_search_value and not regex:
        raise ValueError("prepare_search_value can only be used when regex=True")
    
    if not search_value and not comparison:
        return search_value == comparison

    if regex:
        try:
            if prepare_search_value:
                search_value = prepare_regex_pattern(search_value)
            
            if exact_match:
                search_value = r"^" + search_value + r"$"
            
            flags = 0 if case_sensitive else re.IGNORECASE
            return re.search(search_value, comparison, flags=flags) is not None
            
        except re.error:
            # Invalid regex pattern
            return False
    else:
        if not case_sensitive:
            search_value = search_value.lower()
            comparison = comparison.lower()
        
        if exact_match:
            return search_value == comparison
        else:
            return search_value in comparison
