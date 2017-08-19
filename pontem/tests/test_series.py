# -*- coding: utf-8 -*-
import pytest

SIZE = int(1e6)


@pytest.fixture
def pontem_series():
    import pyspark
    import pontem as pt

    sc = pyspark.SparkContext(master='local[*]', appName='pontem')
    series = pt.Series(sc=sc, data=list(range(SIZE)))
    return series


def test_series_construct(pontem_series) -> None:

    series = pontem_series
    assert series.shape[0] == SIZE, ('Resulting series size ({}) does not match passed iterable size ({})'
                                     .format(series.shape[0], SIZE))

