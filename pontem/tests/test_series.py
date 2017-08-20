# -*- coding: utf-8 -*-
import pytest

SIZE = int(1e6)


@pytest.fixture
def pandas_and_pontem_1d_series():
    import pyspark
    import pontem as pt
    import pandas as pd

    sc = pyspark.SparkContext(master='local[*]', appName='pontem')
    pontem_series = pt.Series(sc=sc, data=list(range(SIZE)))

    pandas_series = pd.Series(data=list(range(SIZE)))
    return pandas_series, pontem_series


def test_series_attributes(pandas_and_pontem_1d_series) -> None:
    """
    Test pontem_series attributes against pandas series attributes of same data
    :param pontem_series:
    :return:
    """
    pandas_series, pontem_series = pandas_and_pontem_1d_series
    assert (pandas_series.shape == pontem_series.shape,
            'Resulting series size ({}) does not match passed iterable size ({})'
            .format(pandas_series.shape, pontem_series.shape)
            )

