import pytest
from src.data.collection import (
    chunk_it,
    flatten_matrix,
    filter_dict_keys_by_value,
    filter_list_of_dicts_by_value
)


class TestChunkIt:
    """Test cases for chunk_it function"""
    
    def test_chunk_it_basic(self):
        """Test chunk_it with basic list"""
        result = list(chunk_it([1, 2, 3, 4, 5], 2))
        assert result == [[1, 2], [3, 4], [5]]
    
    def test_chunk_it_equal_size(self):
        """Test chunk_it when length divides evenly by chunk_size"""
        result = list(chunk_it([1, 2, 3, 4, 5, 6], 3))
        assert result == [[1, 2, 3], [4, 5, 6]]
    
    def test_chunk_it_single_chunk(self):
        """Test chunk_it with chunk_size larger than sequence"""
        result = list(chunk_it([1, 2, 3], 10))
        assert result == [[1, 2, 3]]
    
    def test_chunk_it_chunk_size_1(self):
        """Test chunk_it with chunk_size of 1"""
        result = list(chunk_it([1, 2, 3], 1))
        assert result == [[1], [2], [3]]
    
    def test_chunk_it_empty_sequence(self):
        """Test chunk_it with empty sequence"""
        result = list(chunk_it([], 2))
        assert result == []
    
    def test_chunk_it_string(self):
        """Test chunk_it with string"""
        result = list(chunk_it("abcdefg", 3))
        assert result == ["abc", "def", "g"]
    
    def test_chunk_it_tuple(self):
        """Test chunk_it with tuple"""
        result = list(chunk_it((1, 2, 3, 4, 5), 2))
        assert result == [(1, 2), (3, 4), (5,)]

    def test_chunk_it_chunk_size_0(self):
        """Test chunk_it with chunk_size of 0"""
        with pytest.raises(ValueError, match="chunk_size must be a positive integer"):
            list(chunk_it([1, 2, 3, 4, 5], 0))

    def test_chunk_it_negative_chunk_size(self):
        """Test chunk_it with negative chunk_size"""
        with pytest.raises(ValueError, match="chunk_size must be a positive integer"):
            list(chunk_it([1, 2, 3, 4, 5], -2))
        

class TestFlattenMatrix:
    """Test cases for flatten_matrix function"""
    
    def test_flatten_matrix_basic(self):
        """Test flatten_matrix with basic 2D list"""
        result = flatten_matrix([[1, 2], [3, 4], [5]])
        assert result == [1, 2, 3, 4, 5]
    
    def test_flatten_matrix_empty(self):
        """Test flatten_matrix with empty list"""
        result = flatten_matrix([])
        assert result == []
    
    def test_flatten_matrix_empty_sublists(self):
        """Test flatten_matrix with empty sublists"""
        result = flatten_matrix([[], [], []])
        assert result == []
    
    def test_flatten_matrix_single_element_sublists(self):
        """Test flatten_matrix with single element sublists"""
        result = flatten_matrix([[1], [2], [3]])
        assert result == [1, 2, 3]
    
    def test_flatten_matrix_mixed_types(self):
        """Test flatten_matrix with mixed data types"""
        result = flatten_matrix([[1, "a"], [2, "b"], [3, "c"]])
        assert result == [1, "a", 2, "b", 3, "c"]
    
    def test_flatten_matrix_strings(self):
        """Test flatten_matrix with strings"""
        result = flatten_matrix([["hello", "world"], ["foo", "bar"]])
        assert result == ["hello", "world", "foo", "bar"]


class TestFilterDictByValue:
    """Test cases for filter_dict_keys_by_value function"""
    
    def test_filter_dict_exact_match_found(self):
        """Test filter_dict_keys_by_value with exact match found"""
        d = {'a': 'test', 'b': 'testing', 'c': 'other'}
        result = filter_dict_keys_by_value(d, 'test', exact_match=True)
        assert result == ['a']
    
    def test_filter_dict_exact_match_multiple(self):
        """Test filter_dict_keys_by_value with multiple exact matches"""
        d = {'a': 'test', 'b': 'test', 'c': 'other'}
        result = filter_dict_keys_by_value(d, 'test', exact_match=True)
        assert set(result) == {'a', 'b'}
    
    def test_filter_dict_exact_match_not_found(self):
        """Test filter_dict_keys_by_value with exact match not found"""
        d = {'a': 'test', 'b': 'testing', 'c': 'other'}
        result = filter_dict_keys_by_value(d, 'notfound', exact_match=True)
        assert result == []
    
    def test_filter_dict_contains_match(self):
        """Test filter_dict_keys_by_value with containment check"""
        d = {'a': 'test', 'b': 'testing', 'c': 'other'}
        result = filter_dict_keys_by_value(d, 'test', exact_match=False)
        assert set(result) == {'a', 'b'}
    
    def test_filter_dict_contains_not_found(self):
        """Test filter_dict_keys_by_value with containment check not found"""
        d = {'a': 'test', 'b': 'testing', 'c': 'other'}
        result = filter_dict_keys_by_value(d, 'xyz', exact_match=False)
        assert result == []
    
    def test_filter_dict_nested_key_exact_match(self):
        """Test filter_dict_keys_by_value with nested key and exact match"""
        d_nested = {'a': {'val': 'test'}, 'b': {'val': 'testing'}, 'c': {'val': 'test'}}
        result = filter_dict_keys_by_value(d_nested, 'test', nested_key='val', exact_match=True)
        assert set(result) == {'a', 'c'}
    
    def test_filter_dict_nested_key_contains(self):
        """Test filter_dict_keys_by_value with nested key and containment check"""
        d_nested = {'a': {'val': 'test'}, 'b': {'val': 'testing'}, 'c': {'val': 'other'}}
        result = filter_dict_keys_by_value(d_nested, 'test', nested_key='val', exact_match=False)
        assert set(result) == {'a', 'b'}
    
    def test_filter_dict_empty_dict(self):
        """Test filter_dict_keys_by_value with empty dictionary"""
        result = filter_dict_keys_by_value({}, 'test', exact_match=True)
        assert result == []
    
    def test_filter_dict_numeric_values(self):
        """Test filter_dict_keys_by_value with numeric values"""
        d = {'a': 100, 'b': 200, 'c': 100}
        result = filter_dict_keys_by_value(d, 100, exact_match=True)
        assert set(result) == {'a', 'c'}
    
    def test_filter_dict_nested_key_string(self):
        """Test filter_dict_keys_by_value with string nested key"""
        d_nested = {'a': {'status': 200}, 'b': {'status': 404}, 'c': {'status': 200}}
        result = filter_dict_keys_by_value(d_nested, 200, nested_key='status', exact_match=True)
        assert set(result) == {'a', 'c'}
    
    def test_filter_dict_nested_key_missing(self):
        """Test filter_dict_keys_by_value with missing nested key"""
        d_nested = {'a': {'val': 'test'}, 'b': {'other': 'testing'}, 'c': {'val': 'test'}}
        with pytest.raises(KeyError):
            filter_dict_keys_by_value(d_nested, 'test', nested_key='val', exact_match=True)
    
    def test_filter_dict_nested_key_non_dict_value(self):
        """Test filter_dict_keys_by_value with non-dict value when nested_key is provided"""
        d_mixed = {'a': {'val': 'test'}, 'b': 'string_value', 'c': {'val': 'test'}}
        # Non-dict values should be compared directly, nested_key is ignored
        result = filter_dict_keys_by_value(d_mixed, 'test', nested_key='val', exact_match=True)
        assert set(result) == {'a', 'c'}
    
    def test_filter_dict_contains_with_non_string(self):
        """Test filter_dict_keys_by_value containment check with non-string values"""
        d = {'a': 'test', 'b': 100, 'c': 'testing'}
        with pytest.raises(TypeError):
            filter_dict_keys_by_value(d, 'test', exact_match=False)
    
    def test_filter_dict_contains_with_list_values(self):
        """Test filter_dict_keys_by_value containment check with list values"""
        d = {'a': [1, 2, 3], 'b': [4, 5, 6], 'c': [1, 5, 9]}
        result = filter_dict_keys_by_value(d, 5, exact_match=False)
        assert set(result) == {'b', 'c'}
    
    def test_filter_dict_nested_key_with_int_key(self):
        """Test filter_dict_keys_by_value with integer nested key"""
        d_nested = {'a': {0: 'first', 1: 'second'}, 'b': {0: 'third', 1: 'second'}}
        result = filter_dict_keys_by_value(d_nested, 'second', nested_key=1, exact_match=True)
        assert set(result) == {'a', 'b'}
    
    def test_filter_dict_boolean_values(self):
        """Test filter_dict_keys_by_value with boolean values"""
        d = {'a': True, 'b': False, 'c': True}
        result = filter_dict_keys_by_value(d, True, exact_match=True)
        assert set(result) == {'a', 'c'}
    
    def test_filter_dict_none_values(self):
        """Test filter_dict_keys_by_value with None values"""
        d = {'a': None, 'b': 'value', 'c': None}
        result = filter_dict_keys_by_value(d, None, exact_match=True)
        assert set(result) == {'a', 'c'}


class TestFilterListOfDictsByValue:
    """Test cases for filter_list_of_dicts_by_value function"""
    
    def test_filter_list_basic(self):
        """Test filter_list_of_dicts_by_value with basic example"""
        items = [{'name': 'John', 'age': 30}, {'name': 'Jane', 'age': 25}, {'name': 'John', 'age': 35}]
        result = filter_list_of_dicts_by_value(items, 'name', 'John')
        assert result == [{'name': 'John', 'age': 30}, {'name': 'John', 'age': 35}]
    
    def test_filter_list_no_match(self):
        """Test filter_list_of_dicts_by_value with no matches"""
        items = [{'name': 'John', 'age': 30}, {'name': 'Jane', 'age': 25}]
        result = filter_list_of_dicts_by_value(items, 'name', 'Bob')
        assert result == []
    
    def test_filter_list_single_match(self):
        """Test filter_list_of_dicts_by_value with single match"""
        items = [{'name': 'John', 'age': 30}, {'name': 'Jane', 'age': 25}]
        result = filter_list_of_dicts_by_value(items, 'name', 'Jane')
        assert result == [{'name': 'Jane', 'age': 25}]
    
    def test_filter_list_empty_list(self):
        """Test filter_list_of_dicts_by_value with empty list"""
        result = filter_list_of_dicts_by_value([], 'name', 'John')
        assert result == []
    
    def test_filter_list_numeric_value(self):
        """Test filter_list_of_dicts_by_value with numeric values"""
        items = [{'id': 1, 'status': 'active'}, {'id': 2, 'status': 'inactive'}, {'id': 3, 'status': 'active'}]
        result = filter_list_of_dicts_by_value(items, 'status', 'active')
        assert result == [{'id': 1, 'status': 'active'}, {'id': 3, 'status': 'active'}]
    
    def test_filter_list_filter_by_number(self):
        """Test filter_list_of_dicts_by_value filtering by numeric field"""
        items = [{'id': 1, 'value': 100}, {'id': 2, 'value': 200}, {'id': 3, 'value': 100}]
        result = filter_list_of_dicts_by_value(items, 'value', 100)
        assert result == [{'id': 1, 'value': 100}, {'id': 3, 'value': 100}]

    def test_filter_list_all_match(self):
        """Test filter_list_of_dicts_by_value when all items match"""
        items = [{'type': 'A'}, {'type': 'A'}, {'type': 'A'}]
        result = filter_list_of_dicts_by_value(items, 'type', 'A')
        assert result == items
