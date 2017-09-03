# -*- coding: utf-8 -*-

import warnings

from operator import mul, add, sub, truediv, floordiv
from typing import Iterable, Optional, Union

import pyspark.sql.types as ptypes
import pyspark.sql.functions as pfuncs
from pyspark import SparkContext, SQLContext, HiveContext
from pyspark.rdd import RDD, PipelinedRDD
from pyspark.sql import Row, DataFrame

from pontem.indexes import RangeIndex
from pontem.series.data_prep import PontemDataPrepper


class Series(DataFrame):
    """
    Main Series object for bridging PySpark with pandas.Series
    """
    def __init__(self,
                 sc: Union[SparkContext, SQLContext, HiveContext],
                 data: Union[DataFrame, Iterable, RDD],
                 name: Optional[str]=None,
                 index: Optional[Union[RangeIndex, Iterable, PipelinedRDD, RDD]]=None
                 ) -> None:
        """
        Parameters
        ----------
        sc : SparkContext or any type, is converted to different context types as needed.
        data : SparkDataFrame, Iterable, or RDD (assumed to have atleast one values per row)
            if data is a 2d array, it is assumed the 0th position is the index, and 1th position is the series values
            if data is > 2d array, series name, and index should be numeric indicating the index of them respectively.
                you can later update the series and index names with assignment; '.name = "foo", .index.name = "bar"'
        name : Optional string value to name the series
        index : Iterable of the same length of data, existing pontem.RangeIndex, or string name to give the index.

        Returns
        -------
        pontem.Series - Intended to have similar interface as a pandas.Series
        """

        # Create the spark context or modify it back to type SparkContext, or try to create one
        if sc is None:
            warnings.warn('SparkContext is NoneType; trying to create one using master="local[*]", appName="pontem"')
            self.sc = SparkContext(master='local[*]', appName='pontem')  # type: SparkContext
        else:
            self.sc = sc._sc if type(sc) != SparkContext else sc  # type: SparkContext

        # Default name of series is 'None' (has to be string type) and cannot be '', which is the default for index
        self._name = name if name is not None and name != '' else 'None'  # Row names must be strings, if None make it str

        # Check if index name is within the passed index
        index_name = ''
        if type(index) in [RDD, PipelinedRDD] and type(index.first()) == Row:
            index_name = index_name if not next(iter(index.first().asDict()), None) else next(iter(index.first().asDict()))

        # Index and data may be different types; combine them into a suitable RDD, collection of [Row(*cols), ...]
        # where each column represents the index and the values of the series.
        self._pyspark_series = PontemDataPrepper.combine_index_and_data(sc=self.sc,
                                                                        data=data,
                                                                        name=self.name,
                                                                        index=index,
                                                                        index_name=index_name)

        # Convert RDD.collect() == [Row(name=..., index.name=...), ...] to a dataframe
        self.sc = SQLContext(self.sc)
        self._pyspark_series = self.sc.createDataFrame(self._pyspark_series)  # type: DataFrame

        # Set index
        self.index = RangeIndex(self, index_name=index_name)
        self.index.name = index_name

        # Call the super to make standard RDD methods available.
        super(Series, self).__init__(self._pyspark_series._jdf, self.sc)

    def apply(self, func: callable, args=(), **kwargs) -> 'Series':
        """
        Invoke a function against values of a Series.

        Parameters
        ----------
        func : callable to be applied against values in the Series
        args : positional arguments to pass to func
        kwargs : key-word arguments to pass to func

        Returns
        -------
        pontem.Series
        """
        index_name, series_name = self.index.name, self.name  # Fails if passed directly, tries to serialize object
        result_rdd = self._pyspark_series.rdd.map(
            lambda row: Row(**{index_name: row[index_name], series_name: func(row[series_name], *args, **kwargs)})
        )
        return Series(sc=self.sc,
                      data=result_rdd.map(lambda row: Row(**{series_name: row[series_name]})),
                      name=series_name,
                      index=result_rdd.map(lambda row: Row(**{index_name: row[index_name]}))
                      )


    def map(self, arg: Union[callable, dict],
            na_action: Optional[Union[None, str]]=None,
            inplace=False
            ) -> Optional['Series']:
        """
        Map values of a Series using input correspondence (which can be a dict, Series, or function

        Parameters
        ----------
        arg : function, dict, or Series
        na_action : {None, 'ignore'}
            If 'ignore', propagate NA values, without passing them to the mapping function
        inplace : bool, results of the mapping are applied to the series directly; replaces underlying rdd with results

        Returns
        -------
        pontem.Series or None (if inplace==True)
        """

        if callable(arg):
            return self.apply(func=arg)  # Same as .apply() without any arguments other than the function.

        # TODO: Implement mapping of dict or other series
        # Either swap out underlying rdd or return a new pontem series
        if inplace:
            self._pyspark_series = self.sc.createDataFrame()
        else:
            raise NotImplementedError('Returning pontem.Series not implemented')

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, new_name):
        """
        Change the name of the series
        """
        self._pyspark_series = self._pyspark_series.select(pfuncs.col(self.index.name),
                                                           pfuncs.col(self.name).alias(new_name))
        self._name = new_name

    @property
    def shape(self):
        return tuple((len(self), ))

    def sum(self):
        return self._pyspark_series.select(pfuncs.sum(self.name)).first()[0]

    def describe(self):
        return super(Series, self).describe(self.name).show()

    def mean(self):
        """Get the population mean"""
        return self._pyspark_series.select(pfuncs.mean(self.name)).first()[0]

    def std(self):
        """Alias for .stddev() method; return the population standard deviation"""
        return self.stddev()

    def stddev(self):
        """Get the population standard deviation."""
        return self._pyspark_series.select(pfuncs.stddev(self.name)).first()[0]

    def max(self):
        """Get the max value of the series"""
        return self._pyspark_series.select(self.name).rdd.max()[self.name]

    def min(self):
        """Get the min value of the series"""
        return self._pyspark_series.select(self.name).rdd.min()[self.name]

    def head(self, n: int=5):
        """Take the top n values in the series."""
        return self._pyspark_series.show(n)

    def astype(self, dtype: Union[type, str]) -> 'Series':
        """
        Cast series as a new type
        """
        # TODO: implement other dtypes
        if dtype in ['integer', 'int', int]:
            result = self._pyspark_series.select(self._pyspark_series[self.index.name],
                                                 self._pyspark_series[self.name].cast('integer'))

        return Series(sc=self.sc, data=result, name=self.name)

    def __handle_arithmetic__(self, operation: callable, other: object) -> 'Series':
        """
        Helper for overloading; the only difference between + / * - is the operator
        but the actual operation is the same
        :param operation: a two arg function from the standard lib 'operator' to use against this and other
        :param other: the object passed to the overloading function. ie. integer, other pontem object, etc.
        :return: result
        """

        # Add two pontem.Series together
        if type(other) == type(self):
            alias = self.name if self.name == other.name else 'None'

            # If this if floordiv, perform true div, then cast as integer; floordiv not built into pyspark df
            # TODO: If there are zeros present, an error is thrown.
            if operation.__name__ == 'floordiv':
                result = self._pyspark_series.select(
                    truediv(self._pyspark_series[self.name], other._pyspark_series[other.name]).alias(alias)
                )
                result = result.select(result[alias].cast('integer'))

            # All other operations can be applied safely.
            else:
                result = self._pyspark_series.select(
                    operation(self._pyspark_series[self.name], other._pyspark_series[other.name]).alias(alias)
                )

        # Add a scalar value to Series.
        else:
            def handle(n):
                """Return pyspark UDF to add a scalar value to all values in the column"""
                nonlocal operation
                return pfuncs.udf(lambda col: operation(col, float(n)), returnType=ptypes.FloatType())

            result = self._pyspark_series.select(self.index.name, handle(other)(self.name).alias(self.name))

        return Series(sc=self.sc, data=result, name=self.name)

    def __add__(self, other):
        """Handle addition overloading"""
        return self.__handle_arithmetic__(operation=add, other=other)

    def __sub__(self, other):
        """Handle subtraction overloading"""
        return self.__handle_arithmetic__(operation=sub, other=other)

    def __mul__(self, other):
        """Handle multiplication overloading"""
        return self.__handle_arithmetic__(operation=mul, other=other)

    def __truediv__(self, other):
        """Handle true division overloading"""
        return self.__handle_arithmetic__(operation=truediv, other=other)

    def __floordiv__(self, other):
        """Handle floor division overloading"""
        return self.__handle_arithmetic__(operation=floordiv, other=other)

    def __repr__(self):
        return 'pontem.core.Series[Name: {name}, Length: {length}]'.format(name=self.name, length=self._pyspark_series.count())

    def __str__(self):
        return self.__repr__()

    def __len__(self):
        return self.count()

    def __getitem__(self, item):
        if type(item) == slice:
            pass
        else:
            pass
        raise NotImplementedError('Item indexing not implemented.')



