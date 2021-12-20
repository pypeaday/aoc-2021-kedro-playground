"""test day one nodes module"""
from src.aoc_2021.pipelines.day_one_nodes import (
    get_sum_of_three_consecutive_rows,
    DataFrameHasNotThreeRows,
)
import pytest


def test_get_sum_of_three_consecutive_rows():
    test_input = (1, 2, 3)
    expected = 6
    assert get_sum_of_three_consecutive_rows(test_input) == expected


def test_DataFrameHasNotThreeRows():
    test_input = (1, 2, 3, 4)
    with pytest.raises(DataFrameHasNotThreeRows):
        get_sum_of_three_consecutive_rows(test_input)
