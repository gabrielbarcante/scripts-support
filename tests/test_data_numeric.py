import pytest
from src.data.numeric_data import (
    convert_string_to_float,
    convert_number_to_currency
)


class TestConvertStringToFloat:
    """Test cases for convert_string_to_float function"""
    
    def test_convert_simple_float(self):
        """Test convert_string_to_float with simple float string"""
        assert convert_string_to_float("123.45") == 123.45
        assert convert_string_to_float("100") == 100.0
        assert convert_string_to_float("0.99") == 0.99
    
    def test_convert_brazilian_format(self):
        """Test convert_string_to_float with Brazilian format (1.234,56)"""
        assert convert_string_to_float("1.234,56") == 1234.56
        assert convert_string_to_float("1.000,00") == 1000.0
        assert convert_string_to_float("123,45") == 123.45
    
    def test_convert_american_format(self):
        """Test convert_string_to_float with American format (1,234.56)"""
        assert convert_string_to_float("1,234.56") == 1234.56
        assert convert_string_to_float("1,000.00") == 1000.0
        assert convert_string_to_float("123.45") == 123.45
    
    def test_convert_large_numbers_brazilian(self):
        """Test convert_string_to_float with large numbers in Brazilian format"""
        assert convert_string_to_float("1.000.000,00") == 1000000.0
        assert convert_string_to_float("999.999.999,99") == 999999999.99
    
    def test_convert_large_numbers_american(self):
        """Test convert_string_to_float with large numbers in American format"""
        assert convert_string_to_float("1,000,000.00") == 1000000.0
        assert convert_string_to_float("999,999,999.99") == 999999999.99
    
    def test_convert_with_whitespace(self):
        """Test convert_string_to_float with leading/trailing whitespace"""
        assert convert_string_to_float("  123,45  ") == 123.45
        assert convert_string_to_float("  123.45  ") == 123.45
        assert convert_string_to_float("\t100\n") == 100.0
    
    def test_convert_integer_string(self):
        """Test convert_string_to_float with integer strings"""
        assert convert_string_to_float("100") == 100.0
        assert convert_string_to_float("0") == 0.0
        assert convert_string_to_float("999") == 999.0
    
    def test_convert_without_decimal_part_brazilian(self):
        """Test convert_string_to_float with Brazilian format without decimals"""
        assert convert_string_to_float("1.234") == 1.234
        assert convert_string_to_float("1.000.000") == 1000000.0
    
    def test_convert_without_decimal_part_american(self):
        """Test convert_string_to_float with American format without decimals"""
        assert convert_string_to_float("1,234") == 1.234
        assert convert_string_to_float("1,000,000") == 1000000.0
    
    def test_convert_invalid_input_not_string(self):
        """Test convert_string_to_float with non-string input raises ValueError"""
        with pytest.raises(ValueError, match="Input must be a string"):
            convert_string_to_float(123)  # type: ignore
        
        with pytest.raises(ValueError, match="Input must be a string"):
            convert_string_to_float(123.45)  # type: ignore
        
        with pytest.raises(ValueError, match="Input must be a string"):
            convert_string_to_float(None)  # type: ignore
    
    def test_convert_empty_string(self):
        """Test convert_string_to_float with empty string raises ValueError"""
        with pytest.raises(ValueError, match="Input string is empty"):
            convert_string_to_float("")
        
        with pytest.raises(ValueError, match="Input string is empty"):
            convert_string_to_float("   ")
    
    def test_convert_invalid_format(self):
        """Test convert_string_to_float with invalid format raises ValueError"""
        with pytest.raises(ValueError, match="Cannot convert"):
            convert_string_to_float("abc")
        
        with pytest.raises(ValueError, match="Cannot convert"):
            convert_string_to_float("not a number")
        
        with pytest.raises(ValueError, match="Cannot convert"):
            convert_string_to_float("12a34b56")
    
    def test_convert_with_raise_exception_false(self):
        """Test convert_string_to_float with raise_exception=False"""
        result = convert_string_to_float("invalid", raise_exception=False)
        assert result == 0.0
        
        result = convert_string_to_float("", raise_exception=False)
        assert result == 0.0

        result = convert_string_to_float(123, raise_exception=False)  # type: ignore
        assert result == 0.0
    
    def test_convert_with_custom_return_on_error(self):
        """Test convert_string_to_float with custom return_on_error value"""
        result = convert_string_to_float("invalid", raise_exception=False, return_on_error=-1.0)
        assert result == -1.0
        
        result = convert_string_to_float("invalid", raise_exception=False, return_on_error=None)
        assert result is None


class TestConvertNumberToCurrency:
    """Test cases for convert_number_to_currency function"""
    
    def test_convert_basic_number_brazilian_format(self):
        """Test convert_number_to_currency with basic number in Brazilian format"""
        assert convert_number_to_currency(1234.56) == "R$ 1.234,56"
        assert convert_number_to_currency(100.00) == "R$ 100,00"
    
    def test_convert_basic_number_american_format(self):
        """Test convert_number_to_currency with basic number in American format"""
        assert convert_number_to_currency(1234.56, decimal_separator=".") == "R$ 1,234.56"
        assert convert_number_to_currency(100.00, decimal_separator=".") == "R$ 100.00"
    
    def test_convert_zero(self):
        """Test convert_number_to_currency with zero"""
        assert convert_number_to_currency(0) == "R$ 0,00"
        assert convert_number_to_currency(0.0) == "R$ 0,00"
    
    def test_convert_negative_number(self):
        """Test convert_number_to_currency with negative number"""
        assert convert_number_to_currency(-1234.56) == "-R$ 1.234,56"
        assert convert_number_to_currency(-100) == "-R$ 100,00"
    
    def test_convert_large_number(self):
        """Test convert_number_to_currency with large numbers"""
        assert convert_number_to_currency(1000000) == "R$ 1.000.000,00"
        assert convert_number_to_currency(999999999.99) == "R$ 999.999.999,99"
    
    def test_convert_small_number(self):
        """Test convert_number_to_currency with small numbers"""
        assert convert_number_to_currency(0.99) == "R$ 0,99"
        assert convert_number_to_currency(0.01) == "R$ 0,01"
    
    def test_convert_integer(self):
        """Test convert_number_to_currency with integers"""
        assert convert_number_to_currency(100) == "R$ 100,00"
        assert convert_number_to_currency(1000) == "R$ 1.000,00"
    
    def test_convert_number_custom_symbol(self):
        """Test convert_number_to_currency with custom currency symbol"""
        assert convert_number_to_currency(1234.56, symbol="$") == "$ 1.234,56"
        assert convert_number_to_currency(1234.56, symbol="€") == "€ 1.234,56"
        assert convert_number_to_currency(1234.56, symbol="USD") == "USD 1.234,56"
    
    def test_convert_number_custom_decimal_places(self):
        """Test convert_number_to_currency with custom decimal places"""
        assert convert_number_to_currency(1234.567, decimal_places=3) == "R$ 1.234,567"
        assert convert_number_to_currency(1234.5, decimal_places=1) == "R$ 1.234,5"
        assert convert_number_to_currency(1234, decimal_places=0) == "R$ 1.234"
    
    def test_convert_number_rounding(self):
        """Test convert_number_to_currency with rounding"""
        assert convert_number_to_currency(1234.567, decimal_places=2) == "R$ 1.234,57"
        assert convert_number_to_currency(1234.564, decimal_places=2) == "R$ 1.234,56"
        assert convert_number_to_currency(1234.565, decimal_places=2) == "R$ 1.234,57"
    
    def test_convert_number_american_format_complete(self):
        """Test convert_number_to_currency with American format"""
        assert convert_number_to_currency(1234.56, symbol="$", decimal_separator=".") == "$ 1,234.56"
        assert convert_number_to_currency(1000000, symbol="$", decimal_separator=".") == "$ 1,000,000.00"
    
    def test_convert_number_brazilian_format_complete(self):
        """Test convert_number_to_currency with Brazilian format"""
        assert convert_number_to_currency(1234.56, symbol="R$", decimal_separator=",") == "R$ 1.234,56"
        assert convert_number_to_currency(1000000, symbol="R$", decimal_separator=",") == "R$ 1.000.000,00"
    
    def test_convert_invalid_input_not_number(self):
        """Test convert_number_to_currency with non-numeric input raises ValueError"""
        with pytest.raises(ValueError, match="Input must be a number"):
            convert_number_to_currency("123.45") # type: ignore
        
        with pytest.raises(ValueError, match="Input must be a number"):
            convert_number_to_currency(None) # type: ignore
        
        with pytest.raises(ValueError, match="Input must be a number"):
            convert_number_to_currency([123.45]) # type: ignore
    
    def test_convert_number_no_thousands(self):
        """Test convert_number_to_currency with numbers less than 1000"""
        assert convert_number_to_currency(999.99) == "R$ 999,99"
        assert convert_number_to_currency(1.23) == "R$ 1,23"
    
    def test_convert_number_exactly_thousand(self):
        """Test convert_number_to_currency with exactly 1000"""
        assert convert_number_to_currency(1000) == "R$ 1.000,00"
        assert convert_number_to_currency(1000.00) == "R$ 1.000,00"
