import pytest
from src.data.text_data import (
    remove_special_characters
)


class TestSpecialCharacterRemoval:
    """Test cases for remove_special_characters function"""

    def test_remove_special_characters_basic_punctuation(self):
        """Test remove_special_characters with basic punctuation"""
        assert remove_special_characters("Hello, world!") == "Hello world"
        assert remove_special_characters("Python is great.") == "Python is great"
    
    def test_remove_special_characters_multiple_punctuation_together(self):
        """Test remove_special_characters with multiple punctuation marks together"""
        assert remove_special_characters("Hello!!! How are you???") == "Hello How are you"
        assert remove_special_characters("One, two; three: four.") == "One two three four"
    
    def test_remove_special_characters_special_chars(self):
        """Test remove_special_characters with various special characters"""
        assert remove_special_characters("Test@#$%^&*(){}[]") == "Test"
        assert remove_special_characters("Email: test@example.com") == "Email testexamplecom"
    
    def test_remove_special_characters_normalizes_whitespace(self):
        """Test remove_special_characters normalizes whitespace by default"""
        assert remove_special_characters("Hello,   world!") == "Hello world"
        assert remove_special_characters("Test\n\nNewline") == "Test Newline"
        assert remove_special_characters("  Leading and trailing  ") == "Leading and trailing"
    
    def test_remove_special_characters_no_punctuation(self):
        """Test remove_special_characters with text that has no punctuation"""
        assert remove_special_characters("Hello world") == "Hello world"
        assert remove_special_characters("ABC 123") == "ABC 123"
    
    def test_remove_special_characters_only_punctuation(self):
        """Test remove_special_characters with only punctuation"""
        assert remove_special_characters("!!!???...") == ""
        assert remove_special_characters(".,;:!?") == ""
    
    def test_remove_special_characters_with_numbers(self):
        """Test remove_special_characters preserves numbers"""
        assert remove_special_characters("Price: $19.99!") == "Price 1999"
        assert remove_special_characters("Test-123") == "Test123"
    
    def test_remove_special_characters_removes_underscores(self):
        """Test remove_special_characters removes underscores"""
        assert remove_special_characters("test_variable") == "testvariable"
        assert remove_special_characters("hello_world_123") == "helloworld123"
    
    def test_remove_special_characters_empty_string(self):
        """Test remove_special_characters with empty string returns empty string"""
        assert remove_special_characters("") == ""
        assert remove_special_characters("   ") == ""
    
    def test_remove_special_characters_not_string(self):
        """Test remove_special_characters with non-string input raises ValueError"""
        with pytest.raises(ValueError, match="Input must be a string"):
            remove_special_characters(123) # type: ignore
        
        with pytest.raises(ValueError, match="Input must be a string"):
            remove_special_characters(None) # type: ignore
        
        with pytest.raises(ValueError, match="Input must be a string"):
            remove_special_characters(["test"]) # type: ignore
    
    def test_remove_special_characters_quotes(self):
        """Test remove_special_characters removes quotes"""
        assert remove_special_characters('"Hello" said John') == 'Hello said John'
        assert remove_special_characters("It's a test") == "Its a test"
    
    def test_remove_special_characters_parentheses(self):
        """Test remove_special_characters removes parentheses and brackets"""
        assert remove_special_characters("Test (123)") == "Test 123"
        assert remove_special_characters("List[str]") == "Liststr"
    
    def test_remove_special_characters_preserves_unicode(self):
        """Test remove_special_characters preserves Unicode characters"""
        assert remove_special_characters("Héllo Wörld", keep_unicode=True) == "Héllo Wörld"
        assert remove_special_characters("Привет мир", keep_unicode=True) == "Привет мир"
        assert remove_special_characters("你好世界", keep_unicode=True) == "你好世界"
        assert remove_special_characters("مرحبا", keep_unicode=True) == "مرحبا"
    
    def test_remove_special_characters_removes_unicode(self):
        """Test remove_special_characters removes non-ASCII characters"""
        assert remove_special_characters("Héllo Wörld", keep_unicode=False) == "Hllo Wrld"
        assert remove_special_characters("Café123", keep_unicode=False) == "Caf123"
        assert remove_special_characters("Hello мир", keep_unicode=False) == "Hello"
    
    def test_remove_special_characters_keeps_ascii(self):
        """Test remove_special_characters keeps ASCII letters and digits"""
        assert remove_special_characters("Hello World 123", keep_unicode=False) == "Hello World 123"
        assert remove_special_characters("ABC xyz 789", keep_unicode=False) == "ABC xyz 789"
    
    def test_remove_special_characters_with_special_chars(self):
        """Test remove_special_characters with special characters"""
        assert remove_special_characters("Héllo! Wörld?", keep_unicode=True) == "Héllo Wörld"
        assert remove_special_characters("Café #123", keep_unicode=False) == "Caf 123"
    
    def test_remove_special_characters_normalize_whitespace_false(self):
        """Test remove_special_characters preserves multiple spaces"""
        assert remove_special_characters("Hello,   world!", normalize_whitespace=False) == "Hello   world"
        assert remove_special_characters("Test\n\nNewline", normalize_whitespace=False) == "Test\n\nNewline"
        assert remove_special_characters("Tab\t\there", normalize_whitespace=False) == "Tab\t\there"

    def test_remove_special_characters_normalize_whitespace_true(self):
        """Test remove_special_characters collapses whitespace"""
        assert remove_special_characters("Hello,   world!", normalize_whitespace=True) == "Hello world"
        assert remove_special_characters("Test\n\nNewline", normalize_whitespace=True) == "Test Newline"
        assert remove_special_characters("Tab\t\there", normalize_whitespace=True) == "Tab here"
    
    def test_remove_special_characters_remove_whitespace_true(self):
        """Test remove_special_characters removes all whitespace"""
        assert remove_special_characters("Hello, world!", normalize_whitespace=False, remove_whitespace=True) == "Helloworld"
        assert remove_special_characters("Test\n\nNewline", normalize_whitespace=False, remove_whitespace=True) == "TestNewline"
        assert remove_special_characters("Tab\t\there", normalize_whitespace=False, remove_whitespace=True) == "Tabhere"
        assert remove_special_characters("A B C D E", normalize_whitespace=False, remove_whitespace=True) == "ABCDE"
    
    def test_remove_special_characters_remove_whitespace_false_default(self):
        """Test remove_special_characters not remove whitespace is the default behavior"""
        result_default = remove_special_characters("Hello, world!")
        result_explicit = remove_special_characters("Hello, world!", remove_whitespace=False)
        assert result_default == result_explicit == "Hello world"
    
    def test_remove_special_characters_whitespace_edge_cases(self):
        """Test remove_special_characters edge cases with whitespace handling"""
        # Leading/trailing spaces with normalize
        assert remove_special_characters("  Hello  ", normalize_whitespace=True) == "Hello"
        # Leading/trailing spaces without normalize
        assert remove_special_characters("  Hello  ", normalize_whitespace=False) == "Hello"
        # Only whitespace with remove_whitespace
        assert remove_special_characters("   ", normalize_whitespace=False, remove_whitespace=True) == ""
    
    def test_remove_special_characters_both_normalize_and_remove_whitespace_raises_error(self):
        """Test remove_special_characters setting both normalize_whitespace and remove_whitespace to True raises ValueError"""
        with pytest.raises(ValueError, match="normalize_whitespace cannot be True when remove_whitespace is True"):
            remove_special_characters("Hello world", normalize_whitespace=True, remove_whitespace=True)

    def test_remove_special_characters_normalize_false_remove_true_is_valid(self):
        """Test remove_special_characters that normalize_whitespace=False and remove_whitespace=True is valid"""
        result = remove_special_characters("Hello, world!", normalize_whitespace=False, remove_whitespace=True)
        assert result == "Helloworld"
