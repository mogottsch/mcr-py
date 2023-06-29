import sys


def str_time_to_seconds(str_time: str) -> int:
    """
    Converts a str of format HH:MM:SS to seconds since midnight.
    Can handle times that go past midnight.
    """
    hours, minutes, seconds = map(int, str_time.split(":"))

    if minutes >= 60 or seconds >= 60:
        raise ValueError("Invalid time format")

    total_seconds = hours * 3600 + minutes * 60 + seconds
    return total_seconds


def seconds_to_str_time(seconds: int) -> str:
    """
    Converts seconds since midnight to a str of format HH:MM:SS.
    Can handle times that go past midnight.
    """
    if seconds == sys.maxsize:
        return "--:--:--"
    hours = seconds // 3600
    minutes = (seconds - hours * 3600) // 60
    seconds = seconds - hours * 3600 - minutes * 60
    return f"{hours:02}:{minutes:02}:{seconds:02}"
