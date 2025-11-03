from typing import Sequence, Iterator, Dict, Tuple, List


def chunk_it(sequence_data: Sequence, chunk_size: int) -> Iterator[Sequence]:
    """
    Split a sequence into chunks of specified size.
    
    Args:
        sequence_data: The sequence to be split into chunks.
        chunk_size: The maximum size of each chunk.
        
    Yields:
        Sequence: Successive chunks from the input sequence.
        
    Example:
        >>> list(chunk_it([1, 2, 3, 4, 5], 2))
        [[1, 2], [3, 4], [5]]
    """
    num_chunks = (len(sequence_data) + chunk_size - 1) // chunk_size
    
    for i in range(num_chunks):
        yield sequence_data[i*chunk_size:(i+1)*chunk_size]


def flatten_matrix(matrix: Sequence[Sequence]) -> Sequence:
    """
    Flatten a 2D matrix (list of lists) into a 1D list.
    
    Args:
        matrix: A 2D sequence (list of lists) to flatten.
        
    Returns:
        Sequence: A flattened 1D list containing all items from the input matrix.
        
    Example:
        >>> flatten_matrix([[1, 2], [3, 4], [5]])
        [1, 2, 3, 4, 5]
    """
    return [item for sublist in matrix for item in sublist]


def filter_dict_by_value(dictionary: Dict, filter_value: str, nested_key: str | int | float | Tuple | None = None, exact_match: bool = True) -> List:
    """
    Filter dictionary keys by value, with optional nested key access and matching mode.
    
    Args:
        dictionary: The dictionary to filter.
        filter_value: The value to filter by.
        nested_key: Optional key to access nested dictionary values.
        exact_match: If True, use equality (==). If False, use containment (in).
        
    Returns:
        List: Keys of dictionary entries that match the filter criteria.
        
    Example:
        >>> d = {'a': 'test', 'b': 'testing', 'c': 'other'}
        >>> filter_dict_by_value(d, 'test', exact_match=True)
        ['a']
        >>> filter_dict_by_value(d, 'test', exact_match=False)
        ['a', 'b']
        >>> d_nested = {'a': {'val': 'test'}, 'b': {'val': 'testing'}}
        >>> filter_dict_by_value(d_nested, 'test', nested_key='val', exact_match=False)
        ['a', 'b']
    """
    def matches(value):
        if exact_match:
            return value == filter_value
        else:
            return filter_value in value
    
    def get_comparison_value(item_value):
        if nested_key is not None and isinstance(item_value, dict):
            return item_value[nested_key]
        return item_value
    
    filtered = filter(
        lambda x: matches(get_comparison_value(x[1])), 
        dictionary.items()
    )
    
    return list(dict(filtered).keys())


def filter_list_of_dicts_by_value(dict_list: Sequence[Dict], filter_field: str, filter_value: str) -> List:
    """
    Filter a list of dictionaries by matching a specific field value.
    
    Args:
        dict_list: A sequence of dictionaries to filter.
        filter_field: The dictionary key/field to check.
        filter_value: The value to match against.
        
    Returns:
        List: A list of dictionaries where the specified field matches the filter value.
        
    Example:
        >>> items = [{'name': 'John', 'age': 30}, {'name': 'Jane', 'age': 25}, {'name': 'John', 'age': 35}]
        >>> filter_list_of_dicts_by_value(items, 'name', 'John')
        [{'name': 'John', 'age': 30}, {'name': 'John', 'age': 35}]
    """
    return_list = []
    for item in dict_list:
        if item[filter_field] == filter_value:
            return_list.append(item)

    return return_list
