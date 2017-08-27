# -*- coding: utf-8 -*-
import pytest
from pyspark import SparkContext
from operator import add, sub, mul, truediv, floordiv


"""
Tests for pontem.Series
The majority ensure the behavior of the pontem.Series objects behave like the pandas.Series objects.
"""

# Globals
# TODO: floordiv overload fails if there are zeros present in the series.
DATA = list(range(10, int(1e3)))


def test_series_shape(spark_context) -> None:
    """
    Test pontem_series attributes against pandas series attributes of same data
    :param pontem_series:
    :return:
    """
    import pontem as pt
    import pandas as pd

    pontem_series = pt.Series(sc=spark_context, data=DATA)
    pandas_series = pd.Series(data=DATA)

    assert pandas_series.shape == pontem_series.shape, \
        'Resulting series size ({}) does not match passed iterable size ({})'.format(pandas_series.shape, pontem_series.shape)


def test_series_name_change(spark_context) -> None:
    """
    Test pontem_series attributes against pandas series attributes of same data
    :param pontem_series:
    :return:
    """
    import pontem as pt

    pontem_series = pt.Series(sc=spark_context, data=DATA, name='some_name')

    # Change name by attribute assignment
    pontem_series.name = 'new_name'

    # Ensure the name stayed.
    assert pontem_series.name == 'new_name', \
        'Assigned "new_name" to pontem.Series but did not persist; currently {}'.format(pontem_series.name)

    # Ensure the name change is propagated into the actual schema of the underlying pyspark df
    assert 'new_name' in pontem_series._pyspark_series.schema.names, 'Unable to change pontem.Series name to "new_name"'


def test_series_index_name_change(spark_context) -> None:
    """
    Test pontem_series attributes against pandas series attributes of same data
    """
    import pontem as pt

    sc = spark_context
    pontem_series = pt.Series(sc=sc, data=DATA)

    pontem_series.index.name = 'new_name'

    # Ensure the name stayed.
    assert pontem_series.index.name == 'new_name', \
        'Assigned "new_name" to pontem.Series.index but did not persist; currently {}'.format(pontem_series.index.name)

    # Ensure the name change is propagated into the actual schema of the underlying pyspark df
    assert 'new_name' in pontem_series._pyspark_series.schema.names, \
        'Unable to change pontem.Series.index.name to "new_name"'


@pytest.mark.parametrize('op_against_self', [True, False], ids=lambda val: 'pontem.Series' if val else 'scalar value')
@pytest.mark.parametrize('operation', [None, add, sub, mul, truediv, floordiv], ids=lambda _id: _id.__name__ if _id else 'None')
def test_series_arithmetic(spark_context: SparkContext, operation: callable, op_against_self: bool) -> None:
    """
    Test the arithmetic matches the pandas.Series
    Applies the overloading operator against the series and an integer or itself.
    :param spark_context - SparkContext object
    :param operation - The arithmetic operator to be applied
    :param op_against_self - Whether to apply the operator against itself, if False will apply using an integer.
    """
    import pontem as pt
    import pandas as pd

    pontem_series = pt.Series(sc=spark_context, data=DATA)
    pandas_series = pd.Series(data=DATA)

    # Apply operation if specified.
    if callable(operation):
        pontem_series = operation(pontem_series, pontem_series if op_against_self else 2)
        pandas_series = operation(pandas_series, pandas_series if op_against_self else 2)

    assert pontem_series.sum() == pandas_series.sum(), \
        ('[Operation: {}] The summation of pontem ({}) and pandas ({}) do not match.'
         .format(operation.__name__ if callable(operation) else 'None', pontem_series.sum(), pandas_series.sum())
         )

    assert pontem_series.min() == pandas_series.min(), \
        ('[Operation: {}] The minimums of pontem ({}) and pandas ({}) do not match.'
         .format(operation.__name__ if callable(operation) else 'None', pontem_series.min(), pandas_series.min())
         )

    assert pontem_series.max() == pandas_series.max(), \
        ('[Operation: {}] The maximums of pontem ({}) and pandas ({}) do not match.'
         .format(operation.__name__ if callable(operation) else 'None', pontem_series.max(), pandas_series.max())
         )

    assert pontem_series.mean() == pandas_series.mean(), \
        ('[Operation: {}] The means of pontem ({}) and pandas ({}) do not match.'
         .format(operation.__name__ if callable(operation) else 'None', pontem_series.mean(), pandas_series.mean())
         )

