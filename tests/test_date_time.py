import pytest
from datetime import datetime, timedelta
import pytz
from src.date_time.operations import (
    get_now,
    is_timezone,
    is_timezone_aware,
    add_days_to_date,
    get_month_start_end,
    format_date
)


class TestGetNow:
    """Test cases for get_now function"""
    
    def test_get_now_default_format(self):
        """Test get_now with default parameters returns string"""
        result = get_now()
        assert isinstance(result, str)
        # Verify format YYYY-MM-DD HH:MM:SS
        datetime.strptime(result, "%Y-%m-%d %H:%M:%S")
    
    def test_get_now_custom_format(self):
        """Test get_now with custom format"""
        result = get_now(format="%d/%m/%Y")
        assert isinstance(result, str)
        # Verify format DD/MM/YYYY
        datetime.strptime(result, "%d/%m/%Y")
    
    def test_get_now_as_datetime(self):
        """Test get_now returns datetime object when as_string=False"""
        result = get_now(as_string=False)
        assert isinstance(result, datetime)
        assert result.tzinfo is None # Default return_tzinfo=False
    
    def test_get_now_with_tzinfo(self):
        """Test get_now returns timezone-aware datetime"""
        result = get_now(as_string=False, return_tzinfo=True)
        assert isinstance(result, datetime)
        assert result.tzinfo is not None
    
    def test_get_now_add_days_positive(self):
        """Test get_now with positive day offset"""
        result = get_now(add_days=5, as_string=False)
        assert isinstance(result, datetime)

        now = datetime.now(pytz.timezone("America/Sao_Paulo")).replace(tzinfo=None)
        assert abs((result - now).days - 5) < 1
    
    def test_get_now_add_days_negative(self):
        """Test get_now with negative day offset"""
        result = get_now(add_days=-3, as_string=False)
        assert isinstance(result, datetime)

        now = datetime.now(pytz.timezone("America/Sao_Paulo")).replace(tzinfo=None)
        assert abs((result - now).days + 3) < 1
    
    def test_get_now_different_timezone(self):
        """Test get_now with different timezone"""
        result_utc = get_now(timezone="UTC", as_string=False, return_tzinfo=True)
        result_tokyo = get_now(timezone="Asia/Tokyo", as_string=False, return_tzinfo=True)
        assert isinstance(result_utc, datetime)
        assert isinstance(result_tokyo, datetime)
        assert result_utc.tzinfo is not None
        assert result_tokyo.tzinfo is not None
        assert str(result_utc.tzinfo) == "UTC"
        assert str(result_tokyo.tzinfo) == "Asia/Tokyo"
    
    def test_get_now_invalid_timezone(self):
        """Test get_now raises ValueError for invalid timezone"""
        with pytest.raises(ValueError, match="The timezone 'Invalid/Timezone' is not valid"):
            get_now(timezone="Invalid/Timezone")


class TestIsTimezone:
    """Test cases for is_timezone function"""
    
    def test_is_timezone_valid(self):
        """Test is_timezone with valid timezones"""
        assert is_timezone("America/Sao_Paulo") is True
        assert is_timezone("UTC") is True
        assert is_timezone("Europe/London") is True
        assert is_timezone("Asia/Tokyo") is True
    
    def test_is_timezone_invalid(self):
        """Test is_timezone with invalid timezones"""
        assert is_timezone("Invalid/Timezone") is False
        assert is_timezone("America/FakeCity") is False
        assert is_timezone("") is False
        assert is_timezone("Brazil/Sao_Paulo") is False


class TestIsTimezoneAware:
    """Test cases for is_timezone_aware function"""
    
    def test_is_timezone_aware_with_tz(self):
        """Test is_timezone_aware with timezone-aware datetime"""
        tz = pytz.timezone("America/Sao_Paulo")
        dt = datetime.now(tz)

        assert is_timezone_aware(dt) is True
    
    def test_is_timezone_aware_without_tz(self):
        """Test is_timezone_aware with naive datetime"""
        dt = datetime.now()

        assert is_timezone_aware(dt) is False
    
    def test_is_timezone_aware_utc(self):
        """Test is_timezone_aware with UTC timezone"""
        dt = datetime.now(pytz.UTC)

        assert is_timezone_aware(dt) is True


class TestAddDaysToDate:
    """Test cases for add_days_to_date function"""
    
    def test_add_days_zero(self):
        """Test add_days_to_date with zero days"""
        base_date = datetime(2023, 5, 15, 10, 30, 0)

        result = add_days_to_date(base_date, add_days=0)

        assert result == "2023-05-15 10:30:00"
    
    def test_add_days_positive(self):
        """Test add_days_to_date with positive days"""
        base_date = datetime(2023, 5, 15, 10, 30, 0)

        result = add_days_to_date(base_date, add_days=10)

        assert result == "2023-05-25 10:30:00"
    
    def test_add_days_negative(self):
        """Test add_days_to_date with negative days"""
        base_date = datetime(2023, 5, 15, 10, 30, 0)

        result = add_days_to_date(base_date, add_days=-5)

        assert result == "2023-05-10 10:30:00"
    
    def test_add_days_custom_format(self):
        """Test add_days_to_date with custom format"""
        base_date = datetime(2023, 5, 15, 10, 30, 0)

        result = add_days_to_date(base_date, add_days=0, format="%d/%m/%Y")

        assert result == "15/05/2023"
    
    def test_add_days_return_datetime(self):
        """Test add_days_to_date returns datetime object"""
        base_date = datetime(2023, 5, 15, 10, 30, 0)

        result = add_days_to_date(base_date, add_days=3, as_string=False)
        
        assert isinstance(result, datetime)
        assert result == base_date + timedelta(days=3)
        if isinstance(result, datetime):
            assert result.tzinfo is None
    
    def test_add_days_with_tzinfo(self):
        """Test add_days_to_date preserves timezone info when requested"""
        tz = pytz.timezone("America/Sao_Paulo")
        base_date = datetime(2023, 5, 15, 10, 30, 0, tzinfo=tz)

        result = add_days_to_date(base_date, add_days=0, as_string=False, return_tzinfo=True)

        assert isinstance(result, datetime)
        assert result.tzinfo is not None
    
    def test_add_days_removes_tzinfo_by_default(self):
        """Test add_days_to_date removes timezone info by default"""
        tz = pytz.timezone("America/Sao_Paulo")
        base_date = datetime(2023, 5, 15, 10, 30, 0, tzinfo=tz)
        
        result = add_days_to_date(base_date, add_days=0, as_string=False, return_tzinfo=False)
        
        assert isinstance(result, datetime)
        if isinstance(result, datetime):
            assert result.tzinfo is None


class TestGetMonthStartEnd:
    """Test cases for get_month_start_end function"""
    
    def test_get_month_start_end_january(self):
        """Test get_month_start_end for January (31 days)"""
        start, end = get_month_start_end(2023, 1)

        assert start == "2023-01-01"
        assert end == "2023-01-31"
    
    def test_get_month_start_end_february_regular(self):
        """Test get_month_start_end for February in non-leap year"""
        start, end = get_month_start_end(2023, 2)

        assert start == "2023-02-01"
        assert end == "2023-02-28"
    
    def test_get_month_start_end_february_leap(self):
        """Test get_month_start_end for February in leap year"""
        start, end = get_month_start_end(2024, 2)

        assert start == "2024-02-01"
        assert end == "2024-02-29"
    
    def test_get_month_start_end_april(self):
        """Test get_month_start_end for April (30 days)"""
        start, end = get_month_start_end(2023, 4)

        assert start == "2023-04-01"
        assert end == "2023-04-30"
    
    def test_get_month_start_end_december(self):
        """Test get_month_start_end for December (31 days)"""
        start, end = get_month_start_end(2023, 12)

        assert start == "2023-12-01"
        assert end == "2023-12-31"
    
    def test_get_month_start_end_custom_format(self):
        """Test get_month_start_end with custom format"""
        start, end = get_month_start_end(2023, 5, format="%d/%m/%Y")

        assert start == "01/05/2023"
        assert end == "31/05/2023"
    
    def test_get_month_start_end_as_datetime(self):
        """Test get_month_start_end returns datetime objects"""
        start, end = get_month_start_end(2023, 5, as_string=False)

        assert isinstance(start, datetime)
        assert isinstance(end, datetime)
        assert start == datetime(2023, 5, 1)
        assert end == datetime(2023, 5, 31)


class TestFormatDate:
    """Test cases for format_date function"""
    
    def test_format_date_default(self):
        """Test format_date with default formats"""
        result = format_date("15/05/2023 14:30:11")

        assert result == "2023-05-15 14:30:11"
    
    def test_format_date_custom_formats(self):
        """Test format_date with custom formats"""
        result = format_date("2023-05-15", original_format="%Y-%m-%d", new_format="%d/%m/%Y")

        assert result == "15/05/2023"
    
    def test_format_date_custom_formats_with_time(self):
        """Test format_date with time components"""
        result = format_date(
            "2023-05-15 14:30:45",
            original_format="%Y-%m-%d %H:%M:%S",
            new_format="%d/%m/%Y %H:%M:%S"
        )
        assert result == "15/05/2023 14:30:45"
    
    def test_format_date_to_simple_date(self):
        """Test format_date converting to simple date format"""
        result = format_date(
            "2023-05-15 14:30:45",
            original_format="%Y-%m-%d %H:%M:%S",
            new_format="%Y-%m-%d"
        )
        assert result == "2023-05-15"
    
    def test_format_date_different_separators(self):
        """Test format_date with different separators"""
        result = format_date(
            "15-05-2023",
            original_format="%d-%m-%Y",
            new_format="%d.%m.%Y"
        )
        assert result == "15.05.2023"
    
    def test_format_date_invalid_date_raises_error(self):
        """Test format_date raises error for invalid date string"""
        with pytest.raises(ValueError):
            format_date("invalid-date", original_format="%Y-%m-%d")

    def test_format_date_invalid_format_raises_error(self):
        """Test format_date raises error for invalid date string"""
        with pytest.raises(ValueError):
            format_date("15-05-2023", original_format="invalid-format")
