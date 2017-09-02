# -*- coding: utf-8 -*-

from operator import mul, add, sub, truediv, floordiv
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
                 data: Union[DataFrame, Iterable, RDD],
                 name: Optional[str]=None,
                 index: Optional[Union[Iterable, RangeIndex, str]]=None
                 ) -> None:
        """
        Parameters
        ----------
        sc : SparkContext
        data : SparkDataFrame, Iterable, or RDD (assumed to have at most two values per row, with last being the values)
        name : Optional string value to name the series
        index : Iterable of the same length of data, existing pontem.RangeIndex, or string name to give the index.

        Returns
        -------
        pontem.Series - Intended to have similar interface as a pandas.Series
        """

        # Create the spark context and name of series
        self.sc = SparkContext(master='local[*]', appName='pontem') if sc is None else sc
        name = name if name is not None else 'None'  # Row names must be strings, if None make it str
        self._name = name

        # Handle local data which is not a pyspark rdd/dataframe
        if type(data) not in [DataFrame, RDD]:
            self.sc = SparkContext(self.sc) if type(self.sc) != SparkContext else self.sc
            self._pyspark_series = sc.parallelize(data)
            self.sc = SQLContext(self.sc)
            self._pyspark_series = self._pyspark_series.zipWithIndex().map(
                lambda row: Row(**{name: row[0], '': row[1]})
            )
            self._pyspark_series = self.sc.createDataFrame(self._pyspark_series)  # type: DataFrame

        # Handle a 1d rdd
        elif type(data) == RDD and len(data.take(1)[0]) == 1:
            self._pyspark_series = data
            self.sc = SQLContext(self.sc)
            self._pyspark_series = self._pyspark_series.zipWithIndex().map(
                lambda row: Row(**{name: row[0], index if type(index) == str else '': row[1]})
            )
            self._pyspark_series = self.sc.createDataFrame(self._pyspark_series)  # type: DataFrame

        # Handle a 2d rdd and each element is type pyspark.sql.Row
        elif type(data) == RDD and len(data.take(1)[0]) >= 2 and type(data.take(1)[0]) == Row:

            # Ensure that a name and index were passed since we are pulling out elements of a row from an RDD
            if type(name) != str or type(index) != str:
                raise ValueError('When passing an RDD with elements of type pyspark.sql.Row; you must pass '
                                 'string values for the pontem.Series name ({}) and index ({})'
                                 .format(name, index)
                                 )

            self._pyspark_series = data
            self.sc = SQLContext(self.sc)

            # Re-create rows to only select index and series column before converting to dataframe
            self._pyspark_series = self._pyspark_series.map(
                lambda row: Row(**{name: row[name], index: row[index]})
            )

            self._pyspark_series = self.sc.createDataFrame(self._pyspark_series) # type: DataFrame

        # Handle a regular RDD > 1d, this this case, the name and (optionally) index should be positional args
        elif type(data) == RDD and len(data.take(1)[0]) > 1 and type(data.take(1)[0]) != Row:
            raise NotImplementedError('Constructing a pontem.Series from an RDD with element lengths > 1 '
                                      'not yet implemented.')

        else:
            self._pyspark_series = data
            self.sc = SQLContext(self.sc) if type(self.sc) != SQLContext else self.sc

        # Set index
        self.index = RangeIndex(self)
        if type(index) == str:
            self.index.name = index  # Set the name of the index

        # Call the super to make standard RDD methods available.
        super(Series, self).__init__(self._pyspark_series._jdf, self.sc)

    def map(self, arg: Union[callable, dict], na_action: Optional[Union[None, str]]=None, inplace=False):
        """
        Map values of a Series using input correspondence (which can be a dict, Series, or function
        Parameters
        ----------
        arg : function, dict, or Series
        na_action : {None, 'ignore'}
            If 'ignore', propagate NA values, without passing them to the mapping function
        inplace : bool, results of the mapping are applied to the series directly; replaces underlying rdd with results
        """
        if callable(arg):
            index_name, series_name = self.index.name, self.name  # Fails if passed directly, tries to serialize object
            result_rdd = self._pyspark_series.rdd.map(
                lambda row: Row(**{index_name: row[index_name], series_name: arg(row[series_name])})
            )

        # TODO: Implement mapping of dict or other series

        # Either swap out underlying rdd or return a new pontem series
        if inplace:
            self._pyspark_series.rdd = result_rdd
        else:
            Series(self.sc, data=)



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

    def astype(self, dtype: Union[type, str]):
        """
        Cast series as a new type
        """
        # TODO: implement other dtypes
        if dtype in ['integer', 'int', int]:
            result = self._pyspark_series.select(self._pyspark_series[self.index.name],
                                                 self._pyspark_series[self.name].cast('integer'))

        return Series(sc=self.sc, data=result, name=self.name)


    def __handle_arithmetic__(self, operation: callable, other: object):
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



