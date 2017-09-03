# -*- coding: utf-8 -*-

from operator import mul, add, sub, truediv, floordiv
from typing import Iterable, Optional, Union

import pyspark.sql.types as ptypes
import pyspark.sql.functions as pfuncs
from pyspark import SparkContext, SQLContext, HiveContext
from pyspark.rdd import RDD, PipelinedRDD
from pyspark.sql import Row, DataFrame

from pontem.indexes import RangeIndex


class DataConverter:
    """
    Helper class to convert passed data types into target types
    """

    @classmethod
    def combine_index_and_data(cls,
                               sc: SparkContext,
                               data: Union[DataFrame, RDD, Iterable],
                               name: str,
                               index: Optional[Union[RangeIndex, Iterable, PipelinedRDD, RDD]],
                               index_name: str
                               ) -> RDD:
        """
        Take SparkContext and join data and index suitable for SQLContext.createDataFrame(rdd)
        :param sc: SparkContext
        :param data: Data of almost any time which is iterable or a spark RDD or dataframe
        :param name: name of the target column
        :param index: same as type data
        :param index_name:  name of the target index column in the rdd
        :return: pyspark.rdd where the collection represents Row() types
        """
        # TODO: Put each scenario or the logic below into it's own method.
        # Handle data which is not a pyspark rdd/dataframe but is iterable
        if type(data) not in [DataFrame, RDD, PipelinedRDD]:

            # If index is an iterable, zip with data assuming values in data are 0th index as well as for index iterable
            if isinstance(index, Iterable):
                _pyspark_series = sc.parallelize(zip(data, index))
                _pyspark_series = _pyspark_series.map(
                    lambda row: Row(**{name: row[0], index_name: row[-1]})
                )

            elif index is None:
                _pyspark_series = sc.parallelize(data)
                _pyspark_series = _pyspark_series.zipWithIndex().map(
                    lambda row: Row(**{name: row[0], index_name: row[-1]})
                )

            elif type(index) in [RDD, PipelinedRDD]:
                _pyspark_series = sc.parallelize(data).zip(index).map(
                    lambda row: Row(**{name: row[0], index_name: row[-1]})
                )

        # Handle data which is a 1d rdd
        elif type(data) in [DataFrame, RDD, PipelinedRDD] and len(data.take(1)) == 1:

            if isinstance(index, Iterable):
                index = sc.parallelize(index)

            if type(index) in [RDD, PipelinedRDD]:
                _pyspark_series = data.zip(index).map(
                    lambda row: Row(**{name: row[0], index_name: row[-1]})
                )
            else:
                _pyspark_series = data.zipWithIndex().map(
                    lambda row: Row(**{name: row[0], index_name: row[-1]})
                )

        # Handle a >1d rdd and each element is type pyspark.sql.Row
        elif type(data) in [RDD, PipelinedRDD] and len(data.take(1)) > 1:

            if name is None:
                raise ValueError('If supplying an RDD which has elements of lengths > 1, the name must be '
                                 'provides as either numeric, indicating the index of the values in the RDD '
                                 'or string implying the RDD is made up of Row and "name" exists within the RDD')

            _pyspark_series = data.zipWithIndex().map(
                lambda row: Row(**{str(name): row[name], index_name: row[-1]})
            )

        else:
            raise RuntimeError('Unknown combination of data types!')

        return _pyspark_series


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

        # Create the spark context and name of series
        self.sc = SparkContext(master='local[*]', appName='pontem') if sc is None else sc
        self.sc = sc._sc if type(self.sc) != SparkContext else self.sc
        name = name if name is not None and name != '' else 'None'  # Row names must be strings, if None make it str
        self._name = name
        index_name = ''

        self._pyspark_series = DataConverter.combine_index_and_data(sc=sc,
                                                                    data=data,
                                                                    name=name,
                                                                    index=index, 
                                                                    index_name=index_name)

        self.sc = SQLContext(self.sc)
        self._pyspark_series = self.sc.createDataFrame(self._pyspark_series)  # type: DataFrame

        # Set index
        self.index = RangeIndex(self)
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
        return Series(sc=self.sc, data=result_rdd, name=self.name, index=self.index.name)


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



