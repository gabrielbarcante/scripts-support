import pytest
from src.data.operations import (
    prepare_regex_pattern,
    match_string
)


class TestPrepareRegexPattern:
    """Test cases for prepare_regex_pattern function"""
    
    def test_prepare_basic_escape(self):
        """Test prepare_regex_pattern escapes special characters"""
        assert prepare_regex_pattern("test.com") == r"test\.com"
        assert prepare_regex_pattern("test+123") == r"test\+123"
        assert prepare_regex_pattern("test*123") == r"test\*123"
    
    def test_prepare_multiple_special_chars(self):
        """Test prepare_regex_pattern with multiple special characters"""
        assert prepare_regex_pattern("test.com/path?query=1") == r"test\.com/path\?query=1"
        assert prepare_regex_pattern("price: $19.99") == r"price:\ \$19\.99"
    
    def test_prepare_no_special_chars(self):
        """Test prepare_regex_pattern with no special characters"""
        assert prepare_regex_pattern("test") == "test"
        assert prepare_regex_pattern("abc123") == "abc123"
    
    def test_prepare_with_spaces_between_chars(self):
        """Test prepare_regex_pattern with space_between_chars=True"""
        result = prepare_regex_pattern("abc", space_between_chars=True)
        assert result == r"a\s?b\s?c"
    
    def test_prepare_with_spaces_and_special_chars(self):
        """Test prepare_regex_pattern with spaces between chars and special characters"""
        result = prepare_regex_pattern("a.b", space_between_chars=True)
        assert result == r"a\s?\.\s?b"
    
    def test_prepare_raw_regex_tag(self):
        """Test prepare_regex_pattern with <regex> tags"""
        result = prepare_regex_pattern("test<regex>\\d+</regex>end")
        assert result == r"test\d+end"
    
    def test_prepare_raw_regex_tag_preserves_pattern(self):
        """Test prepare_regex_pattern preserves regex patterns within tags"""
        result = prepare_regex_pattern("email: <regex>[a-z]+@[a-z]+\\.com</regex>")
        assert result == r"email:\ [a-z]+@[a-z]+\.com"
    
    def test_prepare_raw_regex_tag_with_space_between(self):
        """Test prepare_regex_pattern with raw regex and space_between_chars"""
        result = prepare_regex_pattern("a<regex>\\d+</regex>b", space_between_chars=True)
        assert result == r"a\s?\d+b"
    
    def test_prepare_empty_string(self):
        """Test prepare_regex_pattern with empty string return empty string"""
        result = prepare_regex_pattern("")
        assert result == ""
    
    def test_prepare_not_string(self):
        """Test prepare_regex_pattern with non-string input raises ValueError"""
        with pytest.raises(ValueError, match="Input term must be a string"):
            prepare_regex_pattern(123) # type: ignore
        
        with pytest.raises(ValueError, match="Input term must be a string"):
            prepare_regex_pattern(None) # type: ignore
    
    def test_prepare_brackets_and_parentheses(self):
        """Test prepare_regex_pattern escapes brackets and parentheses"""
        assert prepare_regex_pattern("test[0]") == r"test\[0\]"
        assert prepare_regex_pattern("func(arg)") == r"func\(arg\)"
    
    def test_prepare_pipe_character(self):
        """Test prepare_regex_pattern escapes pipe character"""
        assert prepare_regex_pattern("a|b") == r"a\|b"


class TestMatchString:
    """Test cases for match_string function"""
    
    def test_match_string_basic_match(self):
        """Test match_string with basic substring match"""
        assert match_string("test", "This is a test") is True
        assert match_string("hello", "hello world") is True
    
    def test_match_string_no_match(self):
        """Test match_string with no match"""
        assert match_string("xyz", "This is a test") is False
        assert match_string("python", "hello world") is False
    
    def test_match_string_case_insensitive_default(self):
        """Test match_string is case-insensitive by default"""
        assert match_string("TEST", "this is a test") is True
        assert match_string("Hello", "hello world") is True
    
    def test_match_string_case_sensitive(self):
        """Test match_string with case_sensitive=True"""
        assert match_string("TEST", "test", case_sensitive=True) is False
        assert match_string("test", "test", case_sensitive=True) is True
        assert match_string("Hello", "hello", case_sensitive=True) is False
    
    def test_match_string_exact_match(self):
        """Test match_string with exact_match=True"""
        assert match_string("test", "test", exact_match=True) is True
        assert match_string("test", "testing", exact_match=True) is False
        assert match_string("test", "This is a test", exact_match=True) is False
    
    def test_match_string_exact_match_case_insensitive(self):
        """Test match_string with exact_match=True and case_insensitive"""
        assert match_string("TEST", "test", exact_match=True, case_sensitive=False) is True
        assert match_string("Test", "test", exact_match=True, case_sensitive=False) is True
    
    def test_match_string_exact_match_case_sensitive(self):
        """Test match_string with exact_match=True and case_sensitive=True"""
        assert match_string("test", "test", exact_match=True, case_sensitive=True) is True
        assert match_string("Test", "test", exact_match=True, case_sensitive=True) is False
    
    def test_match_string_regex_basic(self):
        """Test match_string with basic regex"""
        assert match_string(r"\d+", "test 123", regex=True) is True
        assert match_string(r"[a-z]+", "hello", regex=True) is True
        assert match_string(r"\d+", "no numbers", regex=True) is False
    
    def test_match_string_regex_with_prepare(self):
        """Test match_string with regex and prepare_search_value=True"""
        assert match_string("test.com", "visit test.com", regex=True, prepare_search_value=True) is True
        assert match_string("test.com", "visit testXcom", regex=True, prepare_search_value=True) is False
    
    def test_match_string_regex_without_prepare(self):
        """Test match_string with regex and prepare_search_value=False"""
        # Without prepare, . matches any character
        assert match_string("test.com", "visit testXcom", regex=True, prepare_search_value=False) is True
        assert match_string("test.com", "visit test.com", regex=True, prepare_search_value=False) is True
    
    def test_match_string_regex_exact_match(self):
        """Test match_string with regex and exact_match=True"""
        assert match_string(r"\d+", "123", regex=True, exact_match=True) is True
        assert match_string(r"\d+", "abc 123 xyz", regex=True, exact_match=True) is False
        assert match_string(r"[a-z]+", "hello", regex=True, exact_match=True) is True
    
    def test_match_string_regex_case_insensitive(self):
        """Test match_string with regex and case insensitive"""
        assert match_string(r"test", "This is a TEST", regex=True, case_sensitive=False) is True
        assert match_string(r"HELLO", "hello world", regex=True, case_sensitive=False) is True
    
    def test_match_string_regex_case_sensitive(self):
        """Test match_string with regex and case sensitive"""
        assert match_string(r"test", "This is a TEST", regex=True, case_sensitive=True) is False
        assert match_string(r"test", "This is a test", regex=True, case_sensitive=True) is True
    
    def test_match_string_regex_with_tags(self):
        """Test match_string with regex tags in search_value"""
        pattern = "test<regex>\\d+</regex>end"
        assert match_string(pattern, "test123end", regex=True, prepare_search_value=True) is True
        assert match_string(pattern, "testaend", regex=True, prepare_search_value=True) is False
    
    def test_match_string_regex_invalid_pattern(self):
        """Test match_string with invalid regex pattern returns False"""
        assert match_string(r"[invalid", "test", regex=True) is False
        assert match_string(r"(unclosed", "test", regex=True) is False
    
    def test_match_string_empty_search_value(self):
        """Test match_string with empty search_value"""
        assert match_string("", "test") is True
    
    def test_match_string_empty_comparison(self):
        """Test match_string with empty comparison"""
        assert match_string("test", "") is False

    def test_match_string_empty_search_value_and_comparison(self):
        """Test match_string with empty search_value and comparison"""
        assert match_string("", "") is True

    def test_match_string_not_string_search_value(self):
        """Test match_string with non-string search_value raises ValueError"""
        with pytest.raises(ValueError, match="search_value must be a string"):
            match_string(123, "test") # type: ignore
        
        with pytest.raises(ValueError, match="search_value must be a string"):
            match_string(None, "test") # type: ignore
    
    def test_match_string_not_string_comparison(self):
        """Test match_string with non-string comparison raises ValueError"""
        with pytest.raises(ValueError, match="comparison must be a string"):
            match_string("test", 123) # type: ignore
        
        with pytest.raises(ValueError, match="comparison must be a string"):
            match_string("test", None) # type: ignore

    def test_match_string_substring_at_start(self):
        """Test match_string finds substring at start"""
        assert match_string("hello", "hello world") is True
    
    def test_match_string_substring_at_end(self):
        """Test match_string finds substring at end"""
        assert match_string("world", "hello world") is True
    
    def test_match_string_substring_in_middle(self):
        """Test match_string finds substring in middle"""
        assert match_string("is a", "this is a test") is True
    
    def test_match_string_complex_regex_pattern(self):
        """Test match_string with complex regex patterns"""
        # Email pattern
        assert match_string(r"[a-z]+@[a-z]+\.com", "contact test@example.com", regex=True) is True
        # Phone pattern
        assert match_string(r"\(\d{2}\) 9\d{4}-\d{4}", "Call (55) 95555-1234", regex=True) is True
    
    def test_match_string_regex_with_anchors(self):
        """Test match_string with regex anchors"""
        assert match_string(r"^test", "test123", regex=True) is True
        assert match_string(r"^test", "123test", regex=True) is False
        assert match_string(r"test$", "123test", regex=True) is True
        assert match_string(r"test$", "test123", regex=True) is False

    def test_match_string_prepare_search_value_and_no_regex(self):
        """Test match_string with prepare_search_value=True and regex=False raises ValueError"""
        with pytest.raises(ValueError, match="prepare_search_value can only be used when regex=True"):
            match_string("test.com", "visit test.com", regex=False, prepare_search_value=True)
