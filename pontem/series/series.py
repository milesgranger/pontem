import numpy as np
import pandas as pd

from typing import Iterable

import pyspark.sql.types as ptypes
import pyspark.sql.functions as pfuncs
from pyspark import SparkContext, SQLContext
from pyspark.sql import Row, DataFrame


class Series(DataFrame):
    """
    Main Series object for wrapping bridging PySpark with pandas.
    """

    def __init__(self, sc: SparkContext=None, data: Iterable=None, name: str=None, index: Iterable=None) -> None:
        """
        Interact with
        :param sc: SparkContext
        :param rdd: pyspark.rdd.RDD
        """

        # Create the spark context and name of series
        self.sc = SparkContext(master='local[*]', appName='pontem') if sc is None else sc
        name = name if name is not None else 'unnamed'
        self.name = name

        #Convert to pyspark.sql.DataFrame if needed
        if type(data) != DataFrame:
            # Convert none rdd data to rdd then to single column dataframe
            self.sc = SparkContext(self.sc) if type(self.sc) != SparkContext else self.sc
            self.series = sc.parallelize(data)
            self.sc = SQLContext(self.sc)
            self.series = self.series.map(lambda row: Row(**{name: row}))
            self.series = self.sc.createDataFrame(self.series)
        else:
            self.series = data
            self.sc = SQLContext(self.sc) if type(self.sc) != SQLContext else self.sc

        # Set index if available:
        # TODO: Fix this, it does not add even index column.
        self.series = self.series.withColumn('index', pfuncs.monotonically_increasing_id())

        # Call the super to make standard RDD methods available.
        super(Series, self).__init__(self.series._jdf, self.sc)

    def __repr__(self):
        return 'pontem.core.Series[{index}, {name}]'.format(index=self.index, name=self.name)

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
            result = self.series.select((self.series[self.name] + other.series[other.name]).alias('result'))

        # Add a scalar value to Series.
        else:
            def add(n):
                """Return pyspark UDF to add a scalar value to all values in the column"""
                return pfuncs.udf(lambda col: col + float(n), returnType=ptypes.FloatType())
            result = self.series.select(add(other)(self.name).alias('result'.format(self.name)))
        return Series(sc=self.sc, data=result, name=result.rdd.name())


    def __getitem__(self, item):
        if type(item) == slice:
            pass
        else:
            pass

    @property
    def shape(self):
        return tuple((len(self), ))

    def describe(self):
        return super(Series, self).describe(self.name).show()

    def mean(self):
        """Get the population mean"""
        return self.series.select(pfuncs.mean(self.name).alias('{}_mean'.format(self.name))).first()[0]

    def std(self):
        """Alias for .stddev() method; return the population standard deviation"""
        return self.stddev()

    def stddev(self):
        """Get the population standard deviation."""
        return self.series.select(pfuncs.stddev(self.name).alias('{}_stddev'.format(self.name))).first()[0]

    def max(self):
        """Get the max value of the series"""
        return self.series.select(self.name).rdd.max()[self.name]

    def min(self):
        """Get the min value of the series"""
        return self.series.select(self.name).rdd.min()[self.name]


    def head(self, n: int=5):
        """
        Take n from rdd
        :param n: int - Number of values to show
        """
        return self.rdd.take(num=n)



