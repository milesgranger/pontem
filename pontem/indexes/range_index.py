# -*- coding: utf-8 -*-

from pyspark.sql import functions as pfuncs


class RangeIndex:
    """
    Pontem RangIndex
    """
    def __init__(self, series):
        self._name = ''
        self.start = series.min()
        self.stop = series.max()
        self.step = 1
        self.series = series

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, new_name):
        self.series._pyspark_series = self.series._pyspark_series.select(
            pfuncs.col(self.name).alias(new_name),
            pfuncs.col(self.series.name).alias(self.series.name)
        )
        self._name = new_name

    def __repr__(self):
        return 'RangeIndex(start={}, stop={}, step={}, name={})'.format(self.start, self.stop, self.step, self.name)

    def __str__(self):
        return self.__repr__()