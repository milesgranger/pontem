import numpy as np
import pandas as pd

from typing import Iterable
from pyspark import SparkContext, SQLContext
from pyspark.rdd import RDD
from pyspark.sql import Column, Row


class Series(RDD):
    """
    Main Series object for wrapping bridging PySpark with pandas.
    """

    def __init__(self, sc: SparkContext=None, data: Iterable=None, name: str=None) -> None:
        """
        Interact with
        :param sc: SparkContext
        :param rdd: pyspark.rdd.RDD
        """
        self.sc = SparkContext(master='local[*]', appName='pontem') if sc is None else sc
        name = name if name is not None else 'unnamed'
        self.name = name

        self.rdd = sc.parallelize(data)
        self.rdd = self.rdd.map(lambda row: Row(**{name: row}))
        super(Series, self).__init__(self.rdd._jrdd, self.rdd.ctx)

    def head(self, n=5):
        """
        Take n from rdd
        :param n: int - Number of values to show
        """
        return self.rdd.take(num=n)
