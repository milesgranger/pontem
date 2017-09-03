# -*- coding: utf-8 -*-

from pyspark.sql import functions as pfuncs
from copy import copy


class RangeIndex:
    """
    Pontem RangIndex
    """
    def __init__(self, series, index_name: str=''):
        self._name = index_name
        self.series = series

    @property
    def name(self):
        return copy(self._name)

    @name.setter
    def name(self, new_name):
        self.series._pyspark_series = self.series._pyspark_series.select(
            pfuncs.col(self.name).alias(new_name),
            pfuncs.col(self.series.name).alias(self.series.name)
        )
        self._name = new_name

    def __repr__(self):
        return 'RangeIndex(start={}, stop={}, step={}, name={})'.format(self.series.min(), self.series.max(), 1, self.name)

    def __str__(self):
        return self.__repr__()