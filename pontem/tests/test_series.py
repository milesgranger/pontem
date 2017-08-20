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


def test_series_shape(pandas_and_pontem_1d_series) -> None:
    """
    Test pontem_series attributes against pandas series attributes of same data
    :param pontem_series:
    :return:
    """
    pandas_series, pontem_series = pandas_and_pontem_1d_series
    assert pandas_series.shape == pontem_series.shape, \
        ('Resulting series size ({}) does not match passed iterable size ({})'
         .format(pandas_series.shape, pontem_series.shape)
         )


def test_series_name_change(pandas_and_pontem_1d_series) -> None:
    """
    Test pontem_series attributes against pandas series attributes of same data
    :param pontem_series:
    :return:
    """
    pandas_series, pontem_series = pandas_and_pontem_1d_series
    pontem_series.name = 'new_name'

    # Ensure the name stayed.
    assert pontem_series.name == 'new_name', \
        'Assigned "new_name" to pontem.Series but did not persist; currently {}'.format(pontem_series.name)

    # Ensure the name change is propagated into the actual schema of the underlying pyspark df
    assert 'new_name' in pontem_series._pyspark_series.schema.names, 'Unable to change pontem.Series name to "new_name"'


def test_series_index_name_change(pandas_and_pontem_1d_series) -> None:
    """
    Test pontem_series attributes against pandas series attributes of same data
    :param pontem_series:
    :return:
    """
    pandas_series, pontem_series = pandas_and_pontem_1d_series
    pontem_series.index.name = 'new_name'

    # Ensure the name stayed.
    assert pontem_series.index.name == 'new_name', \
        'Assigned "new_name" to pontem.Series.index but did not persist; currently {}'.format(pontem_series.index.name)

    # Ensure the name change is propagated into the actual schema of the underlying pyspark df
    assert 'new_name' in pontem_series._pyspark_series.schema.names, \
        'Unable to change pontem.Series.index.name to "new_name"'
    