"""
Custom data module.
Imports and exports all custom data handling functions.
"""

from .collection import chunk_it, flatten_matrix, filter_dict_by_value, filter_list_of_dicts_by_value

__all__ = [
    "chunk_it",
    "flatten_matrix",
    "filter_dict_by_value",
    "filter_list_of_dicts_by_value"
]