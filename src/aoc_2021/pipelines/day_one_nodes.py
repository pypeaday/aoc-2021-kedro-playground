from kedro.pipeline import node
import pandas as pd


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
