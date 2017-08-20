# -*- coding: utf-8 -*-

from typing import Iterable, Optional, Union

import pyspark.sql.types as ptypes
import pyspark.sql.functions as pfuncs
from pyspark import SparkContext, SQLContext, RDD
from pyspark.sql import Row, DataFrame

from pontem.indexes import RangeIndex


class Series(DataFrame):
    """
    Main Series object for wrapping bridging PySpark with pandas.
    """

    def __init__(self,
                 sc: SparkContext,
                 data: Union[DataFrame, Iterable, RDD]=None,
                 name: Optional[str]=None,
                 index: Union[Iterable, RangeIndex]=None
                 ) -> None:
        """
        Interact with
        :param sc: SparkContext
        :param rdd: pyspark.rdd.RDD
        """

        # Create the spark context and name of series
        self.sc = SparkContext(master='local[*]', appName='pontem') if sc is None else sc
        name = name if name is not None else 0
        self._name = name

        # Convert to pyspark.sql.DataFrame if needed
        if type(data) != DataFrame:
            self.sc = SparkContext(self.sc) if type(self.sc) != SparkContext else self.sc
            self._pyspark_series = sc.parallelize(data)
            self.sc = SQLContext(self.sc)
            self._pyspark_series = self._pyspark_series.zipWithIndex().map(lambda row: Row(**{name: row[0], '': row[1]}))
            self._pyspark_series = self.sc.createDataFrame(self._pyspark_series)  # type: DataFrame
        else:
            self._pyspark_series = data
            self.sc = SQLContext(self.sc) if type(self.sc) != SQLContext else self.sc

        # Set index
        self.index = RangeIndex(self)

        # Call the super to make standard RDD methods available.
        super(Series, self).__init__(self._pyspark_series._jdf, self.sc)

    def __repr__(self):
        return 'pontem.core.Series[Name: {name}, Length: {length}]'.format(name=self.name, length=self._pyspark_series.count())

    def __str__(self):
        return self.__repr__()

    def __len__(self):
        return self.count()

    def __add__(self, other):
        """
        Overloading: add either scalar or another pontem.Series to this one.
        """


        # Add two pontem.Series together
        if type(other) == type(self):
            alias = self.name if self.name == other.name else ''
            result = self._pyspark_series.select((self._pyspark_series[self.name] + other.series[other.name]).alias(alias))

        # Add a scalar value to Series.
        else:
            def add(n):
                """Return pyspark UDF to add a scalar value to all values in the column"""
                return pfuncs.udf(lambda col: col + float(n), returnType=ptypes.FloatType())
            result = self._pyspark_series.select(add(other)(self.name).alias('addition_result'))
        return Series(sc=self.sc, data=result, name=result.rdd.name())

    def __getitem__(self, item):
        if type(item) == slice:
            pass
        else:
            pass

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, new_name):
        """Change the name of the series"""
        self._pyspark_series = self._pyspark_series.select(pfuncs.col(self.index.name),
                                                           pfuncs.col(self.name).alias(new_name))
        self._name = new_name

    @property
    def shape(self):
        return tuple((len(self), ))

    def describe(self):
        return super(Series, self).describe(self.name).show()

    def mean(self):
        """Get the population mean"""
        return self._pyspark_series.select(pfuncs.mean(self.name).alias('{}_mean'.format(self.name))).first()[0]

    def std(self):
        """Alias for .stddev() method; return the population standard deviation"""
        return self.stddev()

    def stddev(self):
        """Get the population standard deviation."""
        return self._pyspark_series.select(pfuncs.stddev(self.name).alias('{}_stddev'.format(self.name))).first()[0]

    def max(self):
        """Get the max value of the series"""
        return self._pyspark_series.select(self.name).rdd.max()[self.name]

    def min(self):
        """Get the min value of the series"""
        return self._pyspark_series.select(self.name).rdd.min()[self.name]

    def head(self, n: int=5):
        """Take the top n values in the series."""
        return self._pyspark_series.show(n)



