import pytest
import sys

from package.strtime import str_time_to_seconds, seconds_to_str_time


def test_str_time_to_seconds():
    assert str_time_to_seconds("00:00:00") == 0
    assert str_time_to_seconds("01:30:45") == 5445
    assert str_time_to_seconds("12:00:00") == 43200
    assert str_time_to_seconds("23:59:59") == 86399


def test_str_time_to_seconds_handles_past_midnight():
    assert str_time_to_seconds("24:00:00") == 86400
    assert str_time_to_seconds("25:30:45") == 91845
    assert str_time_to_seconds("36:00:00") == 129600


def test_seconds_to_str_time():
    assert seconds_to_str_time(0) == "00:00:00"
    assert seconds_to_str_time(5445) == "01:30:45"
    assert seconds_to_str_time(43200) == "12:00:00"
    assert seconds_to_str_time(86399) == "23:59:59"


def test_seconds_to_str_time_handles_maxsize():
    assert seconds_to_str_time(sys.maxsize) == "--:--:--"


def test_conversions_are_inverse():
    test_cases = [
        "00:00:00",
        "01:30:45",
        "12:00:00",
        "23:59:59",
        "24:00:00",
        "25:30:45",
        "36:00:00",
    ]
    for time_str in test_cases:
        seconds = str_time_to_seconds(time_str)
        assert seconds_to_str_time(seconds) == time_str


def test_invalid_time_format():
    with pytest.raises(ValueError):
        str_time_to_seconds("12:00")  # Missing seconds
    with pytest.raises(ValueError):
        str_time_to_seconds("12:00:60")  # Invalid seconds
    with pytest.raises(ValueError):
        str_time_to_seconds("12:00:00:00")  # Extra field
