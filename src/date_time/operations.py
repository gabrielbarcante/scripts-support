from datetime import datetime, timedelta
import calendar
import pytz


def get_now(format: str = "%Y-%m-%d %H:%M:%S", add_days: int = 0, timezone: str = "America/Sao_Paulo", as_string: bool = True, return_tzinfo: bool = False) -> datetime | str:
    """
    Get the current datetime in a specific timezone with optional day offset.
    
    Args:
        format: String format for datetime output (default: "%Y-%m-%d %H:%M:%S")
        add_days: Number of days to add to current datetime (default: 0)
        timezone: Timezone string (default: "America/Sao_Paulo")
        as_string: If True, return as formatted string; if False, return datetime object (default: True)
        return_tzinfo: If True and as_string is False, include timezone info in datetime object (default: False)
    
    Returns:
        Formatted datetime string or datetime object based on as_string parameter
    
    Raises:
        ValueError: If the provided timezone is not valid
    """
    if not is_timezone(timezone):
        raise ValueError(f"The timezone '{timezone}' is not valid.")

    tz = pytz.timezone(timezone)
    now = datetime.now(tz)

    return add_days_to_date(now, add_days=add_days, format=format, as_string=as_string, return_tzinfo=return_tzinfo)


def is_timezone(tz_string: str) -> bool:
    """
    Check if a string is a valid timezone.
    
    Args:
        tz_string: Timezone string to validate
    
    Returns:
        True if valid timezone, False otherwise
    """
    return tz_string in pytz.all_timezones


def is_timezone_aware(dt: datetime) -> bool:
    """
    Check if a datetime object is timezone-aware.
    
    Args:
        dt: Datetime object to check
    
    Returns:
        True if timezone-aware, False otherwise
    """
    return dt.tzinfo is not None and dt.utcoffset() is not None


def add_days_to_date(date: datetime, add_days: int = 0, format: str = "%Y-%m-%d %H:%M:%S", as_string: bool = True, return_tzinfo: bool = False) -> datetime | str:
    """
    Add days to a datetime object and return it in the desired format.
    
    Args:
        date: Base datetime object
        add_days: Number of days to add (default: 0)
        format: String format for datetime output (default: "%Y-%m-%d %H:%M:%S")
        as_string: If True, return as formatted string; if False, return datetime object (default: True)
        return_tzinfo: If True and as_string is False, include timezone info in datetime object (default: False)
    
    Returns:
        Formatted datetime string or datetime object based on as_string parameter
    """
    date = date + timedelta(days=add_days)

    if as_string:
        return date.strftime(format)
    else:
        if not return_tzinfo:
            date = date.replace(tzinfo=None)
        return date


def get_month_start_end(year: int, month: int, format: str = "%Y-%m-%d", as_string: bool = True) -> tuple[str | datetime, str | datetime]:
    """
    Get the start and end dates of a specific month.
    
    Args:
        year: Year of the month
        month: Month number (1-12)
        format: String format for datetime output (default: "%Y-%m-%d")
        as_string: If True, return as formatted strings; if False, return datetime objects (default: True)
    
    Returns:
        Tuple containing (start_date, end_date) as strings or datetime objects
    """
    last_day = calendar.monthrange(year, month)[1]
    start_date = datetime(year, month, 1)
    end_date = datetime(year, month, last_day)

    if as_string:
        return start_date.strftime(format), end_date.strftime(format)
    else:
        return start_date, end_date


def format_date(date_string: str, original_format: str = "%d/%m/%Y", new_format: str = "%Y-%m-%d %H:%M:%S") -> str:
    """
    Convert a date string from one format to another.
    
    Args:
        date_string: Date string to convert
        original_format: Current format of the date string (default: "%d/%m/%Y")
        new_format: Target format for the date string (default: "%Y-%m-%d %H:%M:%S")
    
    Returns:
        Reformatted date string
    """
    return datetime.strptime(date_string, original_format).strftime(new_format)

