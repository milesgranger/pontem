# -*- coding: utf-8 -*-

import collections
import numpy as np
from typing import Optional, Union, Iterable
from pyspark import SparkContext
from pyspark.rdd import PipelinedRDD, RDD
from pyspark.sql import DataFrame, Row

from pontem.indexes import RangeIndex


class PontemDataPrepper:
    """
    Helper class to convert passed data types into target types
    ie.
    .combine_index_and_data() takes a number of args to construct an RDD suitable for sqlcontext.createDataFrame(rdd)
    with an index and value column for the pontem.Series type.
    >>> PontemDataPrepper.combine_index_and_data(sc=sc, data=[1,2,3,4], index=pyspark.rdd.RDD)  # index and data should be same length
    pyspark_rdd  # Suitable for pontem.Series.sc.createDataFrame()
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

        # Convert to regular interable, since numpy isn't supported directly with spark.
        index = index.tolist() if isinstance(index, np.ndarray) else index
        data = data.tolist() if isinstance(data, np.ndarray) else data

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
                    lambda row: Row(**{name: row[0] if type(row[0]) != Row else row[0][name],
                                       index_name: row[-1] if type(row[-1]) != Row else row[-1][index_name]}
                                    )
                )
            else:
                print('doing this')
                data = data if not hasattr(data, 'rdd') else data.rdd
                _pyspark_series = data.zipWithIndex().map(
                    lambda row: Row(**{name: row[0][-1], index_name: row[-1]})
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


