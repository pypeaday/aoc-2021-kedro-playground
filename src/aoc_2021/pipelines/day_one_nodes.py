from kedro.pipeline import node
import pandas as pd
from typing import Tuple
from more_itertools import windowed


nodes = []


def clean_day1_data(data: str) -> pd.DataFrame:
    new_data = [int(x) for x in data.split("\n") if x]
    return pd.DataFrame(new_data)


nodes.append(
    node(
        func=clean_day1_data,
        inputs="a_raw_day_one_test",
        # inputs="a_raw_day_one",
        outputs="b_int_day_one",
        name="create_b_int_day_one",
    )
)

nodes.append(
    node(
        func=lambda df: df,
        inputs="b_int_day_one",
        outputs="c_pri_day_one",
        name="create_c_pri_day_one",
    )
)


def count_increases(day_one: pd.DataFrame) -> str:
    """count_increases.

    Args:
        day_one (pd.DataFrame): day_one sonar measurements

    Returns:
        str: count of increases as STRING becuase STUPID
    """
    diffs = day_one.diff()
    res: pd.Series = diffs[diffs > 0].count()
    return str(res.values[0])


nodes.append(
    node(
        func=count_increases,
        inputs=["c_pri_day_one"],
        outputs="d_fea_day_one_count",
        name="create_d_fea_day_one_count",
    )
)


def get_rolling_sum(day_one: pd.DataFrame) -> pd.DataFrame:

    pass


class DataFrameHasNotThreeRows(BaseException):
    pass


def get_sum_of_three_consecutive_rows(tup: Tuple[int, int, int]) -> int:
    """get_sum_of_three_consecutive_rows.

    Args:
        tup: Tuple
    Returns:
        int:
    """
    if len(tup) != 3:
        raise DataFrameHasNotThreeRows
    return sum(tup)


def get_all_windowed_sums(day_one: pd.DataFrame) -> pd.DataFrame:
    res = []
    for s in windowed(day_one.iloc[:, 0].values, 3):
        res.append(get_sum_of_three_consecutive_rows(s))
    return pd.DataFrame({0: res})


def count_increases_slider(day_one: pd.DataFrame) -> str:
    sums = get_all_windowed_sums(day_one)
    return count_increases(sums)


nodes.append(
    node(
        func=count_increases_slider,
        inputs=["c_pri_day_one"],
        outputs="d_fea_day_one_slider_count",
        name="create_d_fea_day_one_slider_count",
    )
)
